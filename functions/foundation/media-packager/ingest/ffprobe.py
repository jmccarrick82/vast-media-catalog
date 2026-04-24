"""Thin wrapper around ffprobe. Returns a normalized dict that every
downstream consumer can trust.

All callers should use `probe_metadata(path)` rather than spawning ffprobe
themselves — keeps parsing + error handling in one place.
"""

import json
import os
import subprocess
from typing import Optional


class FFProbeError(Exception):
    """ffprobe failed — file is corrupt, missing, or unreadable."""


def _ffprobe_binary() -> str:
    """Return path to ffprobe. Prefer an env override (set by functions
    that bundle their own binary), then PATH."""
    override = os.environ.get("FFPROBE_BINARY")
    if override and os.path.isfile(override):
        return override
    return "ffprobe"


def probe_metadata(path: str, timeout: int = 30) -> dict:
    """Run ffprobe and return a dict shaped like:

        {
          "duration_seconds": 90.03,
          "format_name":      "mov,mp4,m4a,3gp,3g2,mj2",
          "bit_rate":         524288,
          "size_bytes":       5898240,
          "streams":          [raw stream dicts from ffprobe],
          "video": {
              "codec": "h264", "profile": "High",
              "width": 854, "height": 480,
              "fps": 29.97, "pix_fmt": "yuv420p",
              "color_space": "bt709", "color_range": "tv",
              "bitrate": 400000,
              "nb_frames": 2700,
          },
          "audio": {
              "codec": "aac", "sample_rate": 44100,
              "channels": 2, "layout": "stereo",
              "bitrate": 64000,
          },
        }

    Missing fields are `None` rather than omitted, so callers don't have to
    guard every lookup.
    """
    cmd = [
        _ffprobe_binary(), "-v", "quiet",
        "-print_format", "json",
        "-show_format", "-show_streams",
        path,
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        raise FFProbeError(f"ffprobe subprocess failed: {e}") from e

    if r.returncode != 0:
        raise FFProbeError(f"ffprobe returned {r.returncode}: {r.stderr[:400]}")
    try:
        data = json.loads(r.stdout)
    except json.JSONDecodeError as e:
        raise FFProbeError(f"ffprobe output not JSON: {e}") from e

    fmt = data.get("format") or {}
    streams = data.get("streams") or []

    v_stream = next((s for s in streams if s.get("codec_type") == "video"), None)
    a_stream = next((s for s in streams if s.get("codec_type") == "audio"), None)

    try:
        duration = float(fmt.get("duration")) if fmt.get("duration") else None
    except (TypeError, ValueError):
        duration = None
    try:
        total_bitrate = int(fmt.get("bit_rate")) if fmt.get("bit_rate") else None
    except (TypeError, ValueError):
        total_bitrate = None
    try:
        size_bytes = int(fmt.get("size")) if fmt.get("size") else None
    except (TypeError, ValueError):
        size_bytes = None

    out = {
        "duration_seconds": duration,
        "format_name":      fmt.get("format_name"),
        "bit_rate":         total_bitrate,
        "size_bytes":       size_bytes,
        "streams":          streams,
        "video":            None,
        "audio":            None,
    }

    if v_stream is not None:
        out["video"] = {
            "codec":       v_stream.get("codec_name"),
            "profile":     v_stream.get("profile"),
            "width":       _int_or_none(v_stream.get("width")),
            "height":      _int_or_none(v_stream.get("height")),
            "fps":         _parse_fraction(v_stream.get("avg_frame_rate") or v_stream.get("r_frame_rate")),
            "pix_fmt":     v_stream.get("pix_fmt"),
            "color_space": v_stream.get("color_space"),
            "color_range": v_stream.get("color_range"),
            "bitrate":     _int_or_none(v_stream.get("bit_rate")),
            "nb_frames":   _int_or_none(v_stream.get("nb_frames")),
            "sar":         v_stream.get("sample_aspect_ratio"),
            "dar":         v_stream.get("display_aspect_ratio"),
        }

    if a_stream is not None:
        out["audio"] = {
            "codec":       a_stream.get("codec_name"),
            "sample_rate": _int_or_none(a_stream.get("sample_rate")),
            "channels":    _int_or_none(a_stream.get("channels")),
            "layout":      a_stream.get("channel_layout"),
            "bitrate":     _int_or_none(a_stream.get("bit_rate")),
        }

    return out


# ── helpers ───────────────────────────────────────────────────────────

def _int_or_none(v) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _parse_fraction(v: Optional[str]) -> Optional[float]:
    """Turn ffprobe's "30000/1001" string into 29.97."""
    if not v or v == "0/0":
        return None
    try:
        if "/" in v:
            n, d = v.split("/", 1)
            n = float(n); d = float(d)
            return n / d if d else None
        return float(v)
    except (TypeError, ValueError):
        return None
