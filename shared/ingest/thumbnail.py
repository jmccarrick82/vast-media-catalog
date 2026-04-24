"""Extract a single JPEG thumbnail from a clip — typically the middle frame.

Pure: takes a local path, writes a local JPEG, returns the path. No S3 or
DB side effects here.
"""

from __future__ import annotations

import os
import subprocess
from typing import Optional

try:
    from shared.config import register_defaults
except ImportError:
    from config import register_defaults  # type: ignore


CONFIG_SCOPE = "packager"


THUMBNAIL_CONFIG_SCHEMA = [
    {
        "key":         "thumbnail_enabled",
        "type":        "bool",
        "default":     True,
        "group":       "Thumbnails",
        "order":       10,
        "description": "Emit a JPEG thumbnail per clip into the package bundle.",
    },
    {
        "key":         "thumbnail_max_width",
        "type":        "int",
        "default":     1280,
        "min":         160,
        "max":         3840,
        "group":       "Thumbnails",
        "order":       20,
        "description": "Target thumbnail width in pixels (height preserves aspect).",
    },
    {
        "key":         "thumbnail_quality",
        "type":        "int",
        "default":     4,
        "min":         1,
        "max":         31,
        "group":       "Thumbnails",
        "order":       30,
        "description": "ffmpeg -q:v value. 2=best, 31=worst. 4 is a good default.",
    },
]

register_defaults(CONFIG_SCOPE, THUMBNAIL_CONFIG_SCHEMA)


class ThumbnailError(Exception):
    """ffmpeg failed to extract the thumbnail."""


def _ffmpeg_binary() -> str:
    override = os.environ.get("FFMPEG_BINARY")
    if override and os.path.isfile(override):
        return override
    return "ffmpeg"


def extract_thumbnail(
    src: str,
    out_jpg: str,
    timestamp: Optional[float] = None,
    duration: Optional[float] = None,
    max_width: int = 1280,
    quality: int = 4,
    timeout: int = 30,
) -> str:
    """Extract a single JPEG at `timestamp` (or duration/2 if omitted).

    Returns the path to the written JPEG. Raises ThumbnailError on failure.
    """
    os.makedirs(os.path.dirname(out_jpg) or ".", exist_ok=True)

    # Pick the sample point: middle of the clip if we know duration
    if timestamp is None:
        if duration and duration > 0:
            timestamp = duration / 2
        else:
            timestamp = 0.5

    cmd = [
        _ffmpeg_binary(),
        "-nostdin",
        "-hide_banner",
        "-loglevel", "error",
        "-ss", f"{max(0.0, timestamp - 0.05):.3f}",
        "-i", src,
        "-frames:v", "1",
        "-vf", f"scale={max_width}:-2",
        "-q:v", str(int(quality)),
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
        raise ThumbnailError(f"thumbnail extract timed out at t={timestamp}") from e

    if r.returncode != 0 or not os.path.isfile(out_jpg):
        raise ThumbnailError(
            f"ffmpeg thumbnail failed at t={timestamp}: {r.stderr[:300]}"
        )
    return out_jpg
