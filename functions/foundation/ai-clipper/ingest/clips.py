"""Clip manipulation primitives — shared between ai-clipper (Phase 2),
packager (Phase 3), and the generic subclipper (Phase 4).

Three responsibilities:
  * `merge_matching_shots` — collapse adjacent matching shots into one clip
  * `constrain_clips` — enforce min/max duration + max count
  * `cut_clip` — ffmpeg stream-copy or transcode a [start,end] span to a file
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from typing import List, Optional, Tuple

try:
    from shared.config import register_defaults
except ImportError:
    from config import register_defaults  # type: ignore


CONFIG_SCOPE = "ai-clipper"

CLIPS_CONFIG_SCHEMA = [
    {
        "key": "merge_gap_seconds",
        "type": "duration_seconds",
        "default": 2.0,
        "min": 0.0,
        "max": 30.0,
        "group": "Clip assembly",
        "order": 10,
        "description": (
            "Adjacent matching shots with a gap <= this are merged into "
            "a single clip. Use 0 to only merge truly contiguous shots."
        ),
    },
    {
        "key": "min_clip_seconds",
        "type": "duration_seconds",
        "default": 2.0,
        "min": 0.5,
        "max": 30.0,
        "group": "Clip assembly",
        "order": 20,
        "description": "Minimum clip duration after merging. Shorter matches are dropped.",
    },
    {
        "key": "max_clip_seconds",
        "type": "duration_seconds",
        "default": 30.0,
        "min": 1.0,
        "max": 600.0,
        "group": "Clip assembly",
        "order": 30,
        "description": "Maximum clip duration. Longer matches are trimmed to this length.",
    },
    {
        "key": "max_clips_per_source",
        "type": "int",
        "default": 5,
        "min": 1,
        "max": 500,
        "group": "Clip assembly",
        "order": 40,
        "description": (
            "Cap on clips extracted per source video. When more candidates "
            "match than this, an LLM curation pass picks the best N "
            "(see curation_* knobs); falls back to highest-confidence on failure."
        ),
    },
    {
        "key": "cut_use_stream_copy",
        "type": "bool",
        "default": True,
        "group": "Clip assembly",
        "order": 50,
        "description": (
            "Use ffmpeg -c copy for cutting. Fast but cuts only at "
            "keyframes. Set false to force a re-encode for frame-exact cuts."
        ),
    },
    {
        "key": "clip_buffer_pre_seconds",
        "type": "duration_seconds",
        "default": 0.0,
        "min": 0.0,
        "max": 30.0,
        "group": "Clip assembly",
        "order": 60,
        "description": (
            "Lead-in padding added BEFORE each clip's matched span. "
            "Helps soften abrupt cuts at the front of an AI-selected clip. "
            "Applied after merge+constrain; clamped at the source's start."
        ),
    },
    {
        "key": "clip_buffer_post_seconds",
        "type": "duration_seconds",
        "default": 0.0,
        "min": 0.0,
        "max": 30.0,
        "group": "Clip assembly",
        "order": 70,
        "description": (
            "Tail-out padding added AFTER each clip's matched span. "
            "Useful when the action you care about ends just past the "
            "vision model's last matching shot. Clamped at source duration."
        ),
    },
]

register_defaults(CONFIG_SCOPE, CLIPS_CONFIG_SCHEMA)


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

@dataclass
class MatchedShot:
    """A shot (start, end) plus its best vision-model verdict."""
    start: float
    end: float
    confidence: float
    reason: str
    model: str


@dataclass
class ClipSpan:
    """A final clip range plus the merged-shot metadata behind it."""
    start: float
    end: float
    shot_count: int
    confidence: float  # max across merged shots
    reason: str        # reason of the peak-confidence shot
    model: str

    @property
    def duration(self) -> float:
        return self.end - self.start


# ---------------------------------------------------------------------------
# Merge + constrain
# ---------------------------------------------------------------------------

def merge_matching_shots(
    shots: List[MatchedShot],
    merge_gap_seconds: float = 2.0,
) -> List[ClipSpan]:
    """Merge adjacent matching shots into clips. Gaps <= merge_gap are glued."""
    if not shots:
        return []

    sorted_shots = sorted(shots, key=lambda s: s.start)
    out: List[ClipSpan] = []
    cur = ClipSpan(
        start=sorted_shots[0].start,
        end=sorted_shots[0].end,
        shot_count=1,
        confidence=sorted_shots[0].confidence,
        reason=sorted_shots[0].reason,
        model=sorted_shots[0].model,
    )

    for s in sorted_shots[1:]:
        gap = s.start - cur.end
        if gap <= merge_gap_seconds:
            # merge
            cur.end = max(cur.end, s.end)
            cur.shot_count += 1
            if s.confidence > cur.confidence:
                cur.confidence = s.confidence
                cur.reason = s.reason
                cur.model = s.model
        else:
            out.append(cur)
            cur = ClipSpan(
                start=s.start,
                end=s.end,
                shot_count=1,
                confidence=s.confidence,
                reason=s.reason,
                model=s.model,
            )
    out.append(cur)
    return out


def apply_buffer(
    clips: List[ClipSpan],
    pre_seconds: float = 0.0,
    post_seconds: float = 0.0,
    source_duration: Optional[float] = None,
) -> List[ClipSpan]:
    """Pad each clip span by `pre_seconds` at the head and `post_seconds`
    at the tail. Intended for editorial polish — softens the abrupt cut
    that lands exactly at a vision-model shot boundary.

    Clamps to [0, source_duration] when source_duration is provided.
    Adjacent clips that grow into each other after buffering are NOT
    re-merged here — call merge_matching_shots first if you need that.
    A no-op when both buffers are zero or negative.
    """
    pre  = max(0.0, float(pre_seconds  or 0.0))
    post = max(0.0, float(post_seconds or 0.0))
    if pre == 0.0 and post == 0.0:
        return clips
    out: List[ClipSpan] = []
    for c in clips:
        new_start = max(0.0, c.start - pre)
        new_end   = c.end + post
        if source_duration is not None and source_duration > 0:
            new_end = min(new_end, float(source_duration))
        if new_end <= new_start:
            # buffer math collapsed the span — skip it rather than emit
            # a zero/negative clip
            continue
        out.append(ClipSpan(
            start=new_start,
            end=new_end,
            shot_count=c.shot_count,
            confidence=c.confidence,
            reason=c.reason,
            model=c.model,
        ))
    return out


def constrain_clips(
    clips: List[ClipSpan],
    min_clip_seconds: float = 2.0,
    max_clip_seconds: float = 30.0,
    max_clips: Optional[int] = None,
) -> List[ClipSpan]:
    """Apply duration + (optional) count constraints.

    `max_clips=None` (default) leaves all duration-valid candidates intact;
    a downstream curation pass is responsible for picking the top N. Pass
    an integer to enable the legacy "top-K by confidence" cap as a fallback.
    """
    # Drop too-short
    kept = [c for c in clips if c.duration >= min_clip_seconds]

    # Trim too-long (center-anchored is nicer than head/tail)
    trimmed: List[ClipSpan] = []
    for c in kept:
        if c.duration <= max_clip_seconds:
            trimmed.append(c)
            continue
        mid = (c.start + c.end) / 2
        half = max_clip_seconds / 2
        c2 = ClipSpan(
            start=max(c.start, mid - half),
            end=min(c.end, mid + half),
            shot_count=c.shot_count,
            confidence=c.confidence,
            reason=c.reason,
            model=c.model,
        )
        trimmed.append(c2)

    # Optional fallback cap — kept for callers that don't run curation.
    if max_clips is not None and len(trimmed) > max_clips:
        top = sorted(trimmed, key=lambda c: c.confidence, reverse=True)[:max_clips]
        top_set = {(c.start, c.end) for c in top}
        trimmed = [c for c in trimmed if (c.start, c.end) in top_set]

    return trimmed


def top_k_by_confidence(
    clips: List[ClipSpan],
    target_count: int,
) -> List[ClipSpan]:
    """Fallback selector — keep the top-N highest-confidence clips,
    preserving source order in the returned list.
    """
    if len(clips) <= target_count:
        return list(clips)
    top = sorted(clips, key=lambda c: c.confidence, reverse=True)[:target_count]
    keep_keys = {(c.start, c.end) for c in top}
    return [c for c in clips if (c.start, c.end) in keep_keys]


# ---------------------------------------------------------------------------
# Cut
# ---------------------------------------------------------------------------

class ClipCutError(Exception):
    """ffmpeg failed to cut the requested span."""


def _ffmpeg_binary() -> str:
    override = os.environ.get("FFMPEG_BINARY")
    if override and os.path.isfile(override):
        return override
    return "ffmpeg"


def cut_clip(
    src: str,
    start: float,
    end: float,
    out: str,
    stream_copy: bool = True,
    video_codec: Optional[str] = None,
    audio_codec: Optional[str] = None,
    width: Optional[int] = None,
    height: Optional[int] = None,
    crf: Optional[int] = None,
    timeout: int = 300,
) -> str:
    """Cut [start, end] from `src` into `out` via ffmpeg.

    When `stream_copy=True` and no codec/scale overrides are set, uses
    `-c copy` which is fast but snaps cuts to the nearest keyframe. Any
    codec or resolution override forces a re-encode for frame-exact cuts.

    Returns the output path on success.
    """
    if end <= start:
        raise ClipCutError(f"invalid span: {start}..{end}")

    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)

    reencode = bool(video_codec or audio_codec or width or height or crf is not None)
    use_copy = stream_copy and not reencode

    cmd: List[str] = [
        _ffmpeg_binary(),
        "-nostdin",
        "-hide_banner",
        "-loglevel", "error",
        "-y",
        "-ss", f"{start:.3f}",
        "-to", f"{end:.3f}",
        "-i", src,
    ]

    if use_copy:
        cmd += ["-c", "copy", "-avoid_negative_ts", "make_zero"]
    else:
        if video_codec:
            cmd += ["-c:v", video_codec]
        else:
            cmd += ["-c:v", "libx264"]
        if crf is not None:
            cmd += ["-crf", str(int(crf))]
        if width and height:
            cmd += ["-vf", f"scale={width}:{height}"]
        if audio_codec:
            cmd += ["-c:a", audio_codec]
        else:
            cmd += ["-c:a", "aac", "-b:a", "128k"]
        cmd += ["-movflags", "+faststart"]

    cmd.append(out)

    try:
        r = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired as e:
        raise ClipCutError(f"ffmpeg cut timed out ({timeout}s) for {start:.2f}..{end:.2f}") from e

    if r.returncode != 0 or not os.path.isfile(out) or os.path.getsize(out) == 0:
        raise ClipCutError(
            f"ffmpeg cut failed ({start:.2f}..{end:.2f}): {r.stderr[:500]}"
        )
    return out
