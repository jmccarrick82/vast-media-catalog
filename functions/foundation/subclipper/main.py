"""james-subclipper — a reusable, direct-invoke clip cutter.

Phase 4 of the pre-ingest pipeline. Unlike the other functions which fire
from S3 PUT events, this one is invoked directly from a script, the webapp,
or another DataEngine function that already knows what it wants to cut.

Event payload (JSON, body or as a cloud event body):

    {
      "src":        "s3://james-media-catalog/basketball-backyard.mp4",
      "out_bucket": "james-media-subclips",     # optional; defaults from config
      "out_prefix": "bball/",                   # optional; prepended to each clip key
      "stream_copy": true,                      # optional; default from config
      "clips": [
        { "start": 0.0,  "end": 3.5  },
        { "start": 12.0, "end": 15.2, "name": "goal-1", "width": 1280, "height": 720 },
        { "start": 30.0, "end": 32.0, "stream_copy": false, "crf": 18 }
      ]
    }

Per-clip overrides win over event-level overrides, which win over config
defaults. Returns a structured result with the output S3 paths + file sizes.

This function does not write VAST DB rows by design — the caller knows
what it wants and tracks its own results. Phase 2 (ai-clipper) and Phase
3 (packager) can call this one if we want to centralize clip-cutting,
but for now they own their own `cut_clip` calls.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
import traceback

from config_loader import load_config as load_json_config
from config import load_config
from s3_client import S3Client

from ingest import clips as clips_lib
from ingest import s3_helpers
from ingest import subclipper  # noqa: F401  (register_defaults at import)


_BIN_DIR = os.path.dirname(os.path.abspath(__file__))
os.environ.setdefault("FFMPEG_BINARY",  os.path.join(_BIN_DIR, "ffmpeg"))
os.environ.setdefault("FFPROBE_BINARY", os.path.join(_BIN_DIR, "ffprobe"))


def init(ctx):
    cfg_file = load_json_config()
    s3 = S3Client(cfg_file)

    ctx.user_data = {
        "cfg_file": cfg_file,
        "s3":       s3,
    }

    try:
        cfg = load_config("subclipper")
        ctx.logger.info(f"subclipper config snapshot: {cfg.snapshot()}")
    except Exception as e:
        ctx.logger.info(f"WARN: couldn't load subclipper config: {e}")

    ctx.logger.info("subclipper initialized")


def handler(ctx, event):
    log = ctx.logger.info
    t_start = time.monotonic()
    try:
        return _handle(ctx, event, log, t_start)
    except Exception as e:
        log(f"HANDLER ERROR: {type(e).__name__}: {e}")
        log(f"TRACEBACK: {traceback.format_exc()}")
        raise


def _handle(ctx, event, log, t_start):
    # 1. Parse the invoke payload
    payload = _parse_event(event)
    src = payload.get("src")
    if not src:
        raise ValueError("payload.src (s3://…) is required")

    cfg = load_config("subclipper")
    snap = cfg.snapshot()

    out_bucket = payload.get("out_bucket") or snap.get("default_out_bucket") or "james-media-subclips"
    out_prefix = payload.get("out_prefix") or ""
    evt_stream_copy = payload.get("stream_copy")
    evt_stream_copy = snap.get("default_stream_copy", True) if evt_stream_copy is None else bool(evt_stream_copy)

    specs = payload.get("clips") or []
    if not isinstance(specs, list) or not specs:
        raise ValueError("payload.clips must be a non-empty list")

    max_clips = int(snap.get("max_clips_per_request") or 200)
    if len(specs) > max_clips:
        raise ValueError(f"too many clips ({len(specs)} > max {max_clips})")

    log(f"[1/3] subclipper: src={src} clips={len(specs)} out=s3://{out_bucket}/{out_prefix}")

    # 2. Download the source once
    log("[2/3] downloading source...")
    _, src_key = s3_helpers.parse_s3_path(src)
    local_src = ctx.user_data["s3"].download_to_temp(src)

    work_dir = tempfile.mkdtemp(prefix="subclipper-")
    results: list = []

    try:
        # 3. Cut + upload each clip
        timeout = float(snap.get("cut_timeout_seconds") or 300.0)
        default_crf = int(snap.get("default_crf") or 23)
        default_vcodec = snap.get("default_video_codec") or "libx264"
        default_acodec = snap.get("default_audio_codec") or "aac"

        log(f"[3/3] cutting {len(specs)} clip(s)...")
        for idx, spec in enumerate(specs):
            start = _require_float(spec, "start")
            end   = _require_float(spec, "end")
            if end <= start:
                results.append({
                    "index": idx, "status": "failed",
                    "error": f"invalid span start={start} end={end}",
                })
                continue

            # Per-clip overrides (beat event + config defaults)
            clip_stream_copy = (
                bool(spec["stream_copy"]) if "stream_copy" in spec else evt_stream_copy
            )
            width  = _maybe_int(spec.get("width"))
            height = _maybe_int(spec.get("height"))
            crf    = _maybe_int(spec.get("crf"))
            video_codec = spec.get("video_codec") or (default_vcodec if (width or height or crf is not None or not clip_stream_copy) else None)
            audio_codec = spec.get("audio_codec") or (default_acodec if (width or height or crf is not None or not clip_stream_copy) else None)

            name = spec.get("name") or f"clip-{idx:03d}"
            # Clean up the user-facing name for s3 key safety
            name = "".join(c if c.isalnum() or c in "-_" else "-" for c in name)
            filename = f"{name}.mp4"
            local_out = os.path.join(work_dir, filename)

            try:
                clips_lib.cut_clip(
                    local_src, start, end, local_out,
                    stream_copy=clip_stream_copy,
                    video_codec=video_codec,
                    audio_codec=audio_codec,
                    width=width, height=height,
                    crf=crf if crf is not None else (default_crf if video_codec else None),
                    timeout=int(timeout),
                )
            except Exception as e:
                log(f"       clip {idx} ({name}): cut failed — {e}")
                results.append({
                    "index": idx, "status": "failed",
                    "error": f"cut failed: {e}",
                    "start": start, "end": end,
                })
                continue

            dst_key = f"{out_prefix}{filename}" if out_prefix else filename
            dst_s3 = f"s3://{out_bucket}/{dst_key}"
            try:
                ctx.user_data["s3"].upload_file(
                    local_out, dst_s3,
                    metadata={
                        "src":   src,
                        "name":  name,
                        "start": f"{start:.3f}",
                        "end":   f"{end:.3f}",
                    },
                )
            except Exception as e:
                log(f"       clip {idx} ({name}): upload failed — {e}")
                results.append({
                    "index": idx, "status": "failed",
                    "error": f"upload failed: {e}",
                })
                continue

            size = os.path.getsize(local_out) if os.path.isfile(local_out) else 0
            results.append({
                "index":          idx,
                "status":         "ok",
                "name":           name,
                "start":          round(start, 3),
                "end":            round(end, 3),
                "duration":       round(end - start, 3),
                "out":            dst_s3,
                "size_bytes":     size,
                "stream_copy":    clip_stream_copy,
                "video_codec":    video_codec,
                "width":          width,
                "height":         height,
                "crf":            crf,
            })
            log(f"       ✓ clip {idx:03d}  {start:6.2f}..{end:6.2f}s  → {dst_s3}")

    finally:
        try: os.unlink(local_src)
        except OSError: pass
        import shutil
        shutil.rmtree(work_dir, ignore_errors=True)

    ok_count   = sum(1 for r in results if r.get("status") == "ok")
    fail_count = sum(1 for r in results if r.get("status") == "failed")

    total = time.monotonic() - t_start
    log(f"Done. subclipper cut {ok_count}/{len(specs)} clips ({fail_count} failed) in {total:.2f}s")

    return json.dumps({
        "src":       src,
        "out_bucket": out_bucket,
        "out_prefix": out_prefix,
        "requested": len(specs),
        "ok":        ok_count,
        "failed":    fail_count,
        "clips":     results,
        "elapsed":   round(total, 3),
    })


# ── helpers ────────────────────────────────────────────────────────────

def _parse_event(event) -> dict:
    """Accept either a direct-invoke JSON body or a cloud-event with a JSON body."""
    body = None
    if hasattr(event, "body"):
        body = event.body
    elif isinstance(event, (str, bytes)):
        body = event
    elif isinstance(event, dict):
        return event
    if body is None:
        raise RuntimeError(f"unsupported event type: {type(event).__name__}")
    if isinstance(body, bytes):
        body = body.decode("utf-8")
    body = body.strip()
    try:
        return json.loads(body)
    except json.JSONDecodeError as e:
        raise ValueError(f"event body is not valid JSON: {e}")


def _require_float(spec: dict, key: str) -> float:
    if key not in spec:
        raise ValueError(f"clip spec missing required key: {key}")
    try:
        return float(spec[key])
    except (TypeError, ValueError):
        raise ValueError(f"clip spec {key} is not a number: {spec[key]!r}")


def _maybe_int(v):
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
