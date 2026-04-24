"""Preset-driven transcoding for the media-packager.

One public function: `transcode(src, out, preset, timeout)` — takes a raw
clip and produces a single rendition by name. Presets are configuration,
not code — they live in `function_configs` under scope `packager` as a
JSON array of preset objects so they're editable via the Settings UI.

A preset object:
  {
    "name":          "h264-1080p",
    "container":     "mp4",
    "video_codec":   "libx264",
    "audio_codec":   "aac",
    "width":         1920,
    "height":        1080,
    "video_bitrate": 4000000,
    "audio_bitrate": 128000,
    "crf":           null,
    "preset":        "medium",
    "min_source_height": null,   // optional: skip if source is smaller
    "faststart":     true,
    "pix_fmt":       "yuv420p"
  }
"""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from typing import List, Optional

try:
    from shared.config import register_defaults
except ImportError:
    from config import register_defaults  # type: ignore


CONFIG_SCOPE = "packager"


# Default rendition presets. These are stored as a single JSON row in the
# config table so the operator can edit the whole set at once in the UI.
_DEFAULT_PRESETS = [
    {
        "name":              "h264-1080p",
        "container":         "mp4",
        "video_codec":       "libx264",
        "audio_codec":       "aac",
        "width":             1920,
        "height":            1080,
        "video_bitrate":     4000000,
        "audio_bitrate":     128000,
        "preset":            "medium",
        "pix_fmt":           "yuv420p",
        "faststart":         True,
        "min_source_height": None,
    },
    {
        "name":              "h264-720p",
        "container":         "mp4",
        "video_codec":       "libx264",
        "audio_codec":       "aac",
        "width":             1280,
        "height":            720,
        "video_bitrate":     2000000,
        "audio_bitrate":     128000,
        "preset":            "medium",
        "pix_fmt":           "yuv420p",
        "faststart":         True,
        "min_source_height": None,
    },
    {
        "name":              "proxy-360p",
        "container":         "mp4",
        "video_codec":       "libx264",
        "audio_codec":       "aac",
        "width":             640,
        "height":            360,
        "video_bitrate":     500000,
        "audio_bitrate":     96000,
        "preset":            "veryfast",
        "pix_fmt":           "yuv420p",
        "faststart":         True,
        "min_source_height": None,
    },
    {
        "name":              "hevc-4k",
        "container":         "mp4",
        "video_codec":       "libx265",
        "audio_codec":       "aac",
        "width":             3840,
        "height":            2160,
        "video_bitrate":     12000000,
        "audio_bitrate":     192000,
        "preset":            "medium",
        "pix_fmt":           "yuv420p",
        "faststart":         True,
        "min_source_height": 2160,   # only emit if source already ≥2160p
    },
]


TRANSCODE_CONFIG_SCHEMA = [
    {
        "key":         "rendition_presets",
        "type":        "json",
        "default":     _DEFAULT_PRESETS,
        "group":       "Transcoding",
        "order":       10,
        "description": (
            "Rendition presets applied to every extracted clip. Each preset "
            "becomes one output file. `min_source_height` skips the preset if "
            "the source clip is smaller than that height. Edit carefully — "
            "bad JSON here breaks packaging. Fields per preset: name, "
            "container, video_codec, audio_codec, width, height, "
            "video_bitrate, audio_bitrate, preset, pix_fmt, faststart, "
            "min_source_height."
        ),
    },
    {
        "key":         "transcode_timeout_seconds",
        "type":        "duration_seconds",
        "default":     300.0,
        "min":         30.0,
        "max":         3600.0,
        "group":       "Transcoding",
        "order":       20,
        "description": "Per-rendition ffmpeg timeout.",
    },
    {
        "key":         "transcode_threads",
        "type":        "int",
        "default":     2,
        "min":         1,
        "max":         16,
        "group":       "Transcoding",
        "order":       30,
        "description": "ffmpeg -threads value per rendition. Keep low so multiple renditions can run in parallel without oversubscribing CPU.",
    },
]

register_defaults(CONFIG_SCOPE, TRANSCODE_CONFIG_SCHEMA)


class TranscodeError(Exception):
    """ffmpeg failed to produce the requested rendition."""


def _ffmpeg_binary() -> str:
    override = os.environ.get("FFMPEG_BINARY")
    if override and os.path.isfile(override):
        return override
    return "ffmpeg"


@dataclass
class RenditionResult:
    preset_name: str
    path: str
    container: str
    video_codec: str
    audio_codec: str
    width: int
    height: int
    fps: float
    video_bitrate: int
    audio_bitrate: int
    file_size_bytes: int


def should_emit_preset(preset: dict, source_height: Optional[int]) -> bool:
    """Decide whether a rendition preset applies given the source's height.

    Currently the only gate is `min_source_height`. We upscale anyway if
    target > source and the preset doesn't set a min — letting the operator
    explicitly opt out via min_source_height if they care.
    """
    min_h = preset.get("min_source_height")
    if min_h is not None and source_height is not None and source_height < int(min_h):
        return False
    return True


def transcode(
    src: str,
    out: str,
    preset: dict,
    timeout: float = 300.0,
    threads: int = 2,
) -> RenditionResult:
    """Run ffmpeg to produce one rendition. Returns structured result."""
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)

    width  = int(preset.get("width")  or 0) or None
    height = int(preset.get("height") or 0) or None
    vcodec = preset.get("video_codec") or "libx264"
    acodec = preset.get("audio_codec") or "aac"
    vb     = int(preset.get("video_bitrate") or 0) or None
    ab     = int(preset.get("audio_bitrate") or 0) or None
    ff_pre = preset.get("preset") or "medium"
    pix    = preset.get("pix_fmt") or "yuv420p"
    fstart = bool(preset.get("faststart", True))

    cmd: List[str] = [
        _ffmpeg_binary(),
        "-nostdin",
        "-hide_banner",
        "-loglevel", "error",
        "-y",
        "-i", src,
        "-c:v", vcodec,
        "-preset", ff_pre,
        "-pix_fmt", pix,
        "-threads", str(int(threads)),
    ]
    if width and height:
        # Even-dimension scale to avoid h264 complaints on odd sizes
        cmd += ["-vf", f"scale={width}:{height}:force_original_aspect_ratio=decrease,"
                       f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2"]
    if vb:
        cmd += ["-b:v", str(vb)]
    if acodec:
        cmd += ["-c:a", acodec]
    if ab:
        cmd += ["-b:a", str(ab)]
    if fstart:
        cmd += ["-movflags", "+faststart"]
    cmd.append(out)

    try:
        r = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=int(timeout),
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired as e:
        raise TranscodeError(
            f"ffmpeg transcode timed out after {timeout}s for preset {preset.get('name')}"
        ) from e

    if r.returncode != 0 or not os.path.isfile(out) or os.path.getsize(out) == 0:
        raise TranscodeError(
            f"ffmpeg transcode failed for preset {preset.get('name')}: "
            f"{r.stderr[:500]}"
        )

    size = os.path.getsize(out)
    return RenditionResult(
        preset_name=preset.get("name") or "unknown",
        path=out,
        container=preset.get("container") or "mp4",
        video_codec=vcodec,
        audio_codec=acodec,
        width=width or 0,
        height=height or 0,
        fps=0.0,  # caller can probe if desired
        video_bitrate=vb or 0,
        audio_bitrate=ab or 0,
        file_size_bytes=size,
    )
