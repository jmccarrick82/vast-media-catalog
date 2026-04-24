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
        "default": 20,
        "min": 1,
        "max": 500,
        "group": "Clip assembly",
        "order": 40,
        "description": (
            "Cap on clips extracted per source video. If more shots match "
            "than this, the highest-confidence ones win."
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


def constrain_clips(
    clips: List[ClipSpan],
    min_clip_seconds: float = 2.0,
    max_clip_seconds: float = 30.0,
    max_clips: int = 20,
) -> List[ClipSpan]:
    """Apply duration + count constraints. Keeps the highest-confidence clips."""
    # Drop too-short
    kept = [c for c in clips if c.duration >= min_clip_seconds]

    # Trim too-long (center-anchored is nicer than head/tail)
    trimmed: List[ClipSpan] = []
    for c in kept:
        if c.duration <= max_clip_seconds:
            trimmed.append(c)
            continue
        # center on the middle of the span
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

    # Cap by count — highest confidence wins, preserve source order in output
    if len(trimmed) > max_clips:
        top = sorted(trimmed, key=lambda c: c.confidence, reverse=True)[:max_clips]
        top_set = {(c.start, c.end) for c in top}
        trimmed = [c for c in trimmed if (c.start, c.end) in top_set]

    return trimmed


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
