"""subclipper primitives — config knobs for the generic direct-invoke
subclipper function.

Only declares knobs. The actual cutting logic is in `clips.cut_clip`,
which this function calls for each requested clip.
"""

from __future__ import annotations

try:
    from shared.config import register_defaults
except ImportError:
    from config import register_defaults  # type: ignore


CONFIG_SCOPE = "subclipper"

SUBCLIPPER_CONFIG_SCHEMA = [
    {
        "key":         "default_out_bucket",
        "type":        "string",
        "default":     "james-media-subclips",
        "group":       "Subclipper defaults",
        "order":       10,
        "description": "Bucket to drop output clips into when the caller doesn't specify.",
    },
    {
        "key":         "default_stream_copy",
        "type":        "bool",
        "default":     True,
        "group":       "Subclipper defaults",
        "order":       20,
        "description": (
            "Default ffmpeg mode when the caller doesn't override: true = "
            "fast `-c copy` (snaps to nearest keyframe), false = re-encode "
            "for frame-exact cuts."
        ),
    },
    {
        "key":         "default_video_codec",
        "type":        "string",
        "default":     "libx264",
        "group":       "Subclipper defaults",
        "order":       30,
        "description": "Video codec when re-encoding is requested (stream_copy=false or width/height override).",
    },
    {
        "key":         "default_audio_codec",
        "type":        "string",
        "default":     "aac",
        "group":       "Subclipper defaults",
        "order":       40,
        "description": "Audio codec when re-encoding.",
    },
    {
        "key":         "default_crf",
        "type":        "int",
        "default":     23,
        "min":         0,
        "max":         51,
        "group":       "Subclipper defaults",
        "order":       50,
        "description": "CRF value when re-encoding (H.264/H.265 quality; lower = better).",
    },
    {
        "key":         "cut_timeout_seconds",
        "type":        "duration_seconds",
        "default":     300.0,
        "min":         10.0,
        "max":         3600.0,
        "group":       "Subclipper defaults",
        "order":       60,
        "description": "Per-clip ffmpeg timeout.",
    },
    {
        "key":         "max_clips_per_request",
        "type":        "int",
        "default":     200,
        "min":         1,
        "max":         5000,
        "group":       "Subclipper defaults",
        "order":       70,
        "description": "Safety cap on clips in a single invoke payload.",
    },
]

register_defaults(CONFIG_SCOPE, SUBCLIPPER_CONFIG_SCHEMA)
