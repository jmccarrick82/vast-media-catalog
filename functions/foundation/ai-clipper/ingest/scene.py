"""Scene / shot detection using ffmpeg's scene filter.

Returns a list of shot boundaries: [(start, end), ...] covering the
entire duration of the video. Each shot is a continuous run between
detected scene changes.

We prefer ffmpeg over PySceneDetect because:
  - no extra dependency (we already bundle ffmpeg for qc-inspector)
  - good enough for the clip-extraction workflow (false positives are
    fine; the AI classifier filters on semantic content, not shot
    boundaries)

The key runtime knob is `scene_change_threshold` (0.0-1.0, higher =
fewer, more confident scene changes). Registered in `function_configs`
under scope `ai-clipper` so it's editable at runtime.
"""

from __future__ import annotations

import os
import re
import subprocess
from typing import List, Tuple

try:
    from shared.config import register_defaults
except ImportError:  # when bundled into a function's flat layout
    from config import register_defaults  # type: ignore


CONFIG_SCOPE = "ai-clipper"

# Only declare the knobs scene.py owns. vision.py and clips.py add their
# own keys under the same scope.
SCENE_CONFIG_SCHEMA = [
    {
        "key": "scene_change_threshold",
        "type": "float",
        "default": 0.30,
        "min": 0.05,
        "max": 0.95,
        "group": "Scene detection",
        "order": 10,
        "description": (
            "ffmpeg scene filter threshold (0.0-1.0). Higher = fewer, "
            "more confident cuts. 0.3 is a sane default for most content; "
            "raise to 0.4-0.5 for sports or high-motion footage."
        ),
    },
    {
        "key": "min_shot_seconds",
        "type": "duration_seconds",
        "default": 1.0,
        "min": 0.1,
        "max": 30.0,
        "group": "Scene detection",
        "order": 20,
        "description": (
            "Shots shorter than this are merged with the next shot. "
            "Filters out rapid-cut noise (cross-dissolves, flashes)."
        ),
    },
    {
        "key": "max_shot_seconds",
        "type": "duration_seconds",
        "default": 60.0,
        "min": 5.0,
        "max": 600.0,
        "group": "Scene detection",
        "order": 30,
        "description": (
            "Upper bound on shot length. Shots longer than this are split "
            "at this interval so long static takes still get sampled."
        ),
    },
]

register_defaults(CONFIG_SCOPE, SCENE_CONFIG_SCHEMA)


def _ffmpeg_binary() -> str:
    override = os.environ.get("FFMPEG_BINARY")
    if override and os.path.isfile(override):
        return override
    return "ffmpeg"


class SceneDetectError(Exception):
    """Scene detection failed — ffmpeg barfed or output was unparseable."""


# ffmpeg emits lines like:
#   [Parsed_showinfo_1 @ 0x...] n:   3 pts: 30000 pts_time:1.25 ...
# when combined with `-vf select='gt(scene,N)',showinfo`.
_PTS_TIME_RE = re.compile(r"pts_time:([0-9]+\.[0-9]+)")


def detect_scenes(
    path: str,
    duration_seconds: float,
    threshold: float = 0.30,
    min_shot_seconds: float = 1.0,
    max_shot_seconds: float = 60.0,
    timeout: int = 300,
) -> List[Tuple[float, float]]:
    """Return a list of (start, end) shot boundaries covering [0, duration].

    Uses ffmpeg's `scene` filter to find change points, then assembles
    contiguous shots. Applies min/max shot length constraints so the
    downstream AI classifier has roughly uniform-sized input.

    Arguments:
      path:              local path to the video
      duration_seconds:  total duration from ffprobe
      threshold:         ffmpeg scene threshold (0-1)
      min_shot_seconds:  shots shorter than this are merged forward
      max_shot_seconds:  shots longer than this are split into equal pieces

    Returns:
      [(start, end), ...] with len >= 1. Always covers full duration.
    """
    if duration_seconds <= 0:
        return []

    cmd = [
        _ffmpeg_binary(),
        "-nostdin",
        "-hide_banner",
        "-loglevel", "info",
        "-i", path,
        "-an",  # skip audio
        "-sn",  # skip subs
        "-vf", f"select='gt(scene,{threshold})',showinfo",
        "-f", "null",
        "-",
    ]
    try:
        r = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired as e:
        raise SceneDetectError(f"ffmpeg scene detect timed out after {timeout}s") from e
    except FileNotFoundError as e:
        raise SceneDetectError(f"ffmpeg binary not found: {e}") from e

    # showinfo writes to stderr
    cut_points: List[float] = []
    for line in r.stderr.splitlines():
        m = _PTS_TIME_RE.search(line)
        if m:
            try:
                t = float(m.group(1))
                if 0 < t < duration_seconds:
                    cut_points.append(t)
            except ValueError:
                continue

    cut_points = sorted(set(cut_points))
    return _assemble_shots(cut_points, duration_seconds, min_shot_seconds, max_shot_seconds)


def _assemble_shots(
    cut_points: List[float],
    duration: float,
    min_len: float,
    max_len: float,
) -> List[Tuple[float, float]]:
    """Convert cut points into (start, end) shots, applying min/max length."""
    if not cut_points:
        return _split_long([(0.0, duration)], max_len)

    raw: List[Tuple[float, float]] = []
    prev = 0.0
    for cp in cut_points:
        if cp > prev:
            raw.append((prev, cp))
            prev = cp
    if prev < duration:
        raw.append((prev, duration))

    # Merge short shots forward
    merged: List[Tuple[float, float]] = []
    for s, e in raw:
        if merged and (e - s) < min_len:
            # extend previous
            ps, _ = merged[-1]
            merged[-1] = (ps, e)
        else:
            merged.append((s, e))
    # If the final merged shot is still shorter than min_len, glue it back
    if len(merged) >= 2 and (merged[-1][1] - merged[-1][0]) < min_len:
        ps, _ = merged[-2]
        _, pe = merged[-1]
        merged = merged[:-2] + [(ps, pe)]

    return _split_long(merged, max_len)


def _split_long(shots: List[Tuple[float, float]], max_len: float) -> List[Tuple[float, float]]:
    """Split any shot longer than max_len into equal-sized pieces."""
    out: List[Tuple[float, float]] = []
    for s, e in shots:
        length = e - s
        if length <= max_len:
            out.append((s, e))
            continue
        n_pieces = max(2, int(length // max_len) + 1)
        step = length / n_pieces
        for i in range(n_pieces):
            out.append((s + i * step, s + (i + 1) * step))
    return out


def extract_keyframe(path: str, timestamp: float, out_jpg: str, timeout: int = 30) -> str:
    """Extract a single JPEG keyframe at `timestamp` using ffmpeg.

    Uses fast seeking (-ss before -i) with a small decode window for
    accuracy. Returns the output path.
    """
    os.makedirs(os.path.dirname(out_jpg) or ".", exist_ok=True)
    cmd = [
        _ffmpeg_binary(),
        "-nostdin",
        "-hide_banner",
        "-loglevel", "error",
        "-ss", f"{max(0.0, timestamp - 0.05):.3f}",
        "-i", path,
        "-frames:v", "1",
        "-q:v", "2",
        "-y",
        out_jpg,
    ]
    try:
        r = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired as e:
        raise SceneDetectError(f"ffmpeg keyframe extract timed out at t={timestamp}") from e

    if r.returncode != 0 or not os.path.isfile(out_jpg):
        raise SceneDetectError(
            f"ffmpeg keyframe extract failed at t={timestamp}: {r.stderr[:300]}"
        )
    return out_jpg
