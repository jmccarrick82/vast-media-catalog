"""james-qc-inspector — first stage of the pre-ingest pipeline.

Trigger: S3 PUT on `s3://james-media-inbox/*`
Responsibilities:
  1. Download the uploaded file locally once.
  2. Probe it (ffprobe).
  3. Run the configured battery of detectors (silence / black / freeze
     / loudness / VFR / interlaced). Which of these run, and with what
     thresholds, is read from the `function_configs` VAST DB table at
     handler startup — every knob is runtime-editable via the
     webapp Settings page.
  4. Apply the policy (`qc_policy.evaluate_qc`) → passed / warn / failed.
  5. Upsert one row into the `source_videos` VAST DB table with every
     structured result.
  6. Move the file (server-side S3 copy + delete) to either
     `james-media-qc-passed` or `james-media-qc-failed`, with
     x-amz-meta-source-id and x-amz-meta-qc-status tags.

Everything that can be a reusable library primitive lives in `ingest/*.py`
(copied from `shared/ingest/` at build time). This handler is thin glue.
"""

import hashlib
import json
import os
import subprocess
import time
import traceback

from config_loader import load_config as load_json_config   # reads config.json on disk
from config import load_config                              # DB-backed runtime knobs
from s3_client import S3Client

from ingest import ffprobe, qc, qc_policy
from ingest import s3_helpers, tables
from schemas import SOURCE_VIDEOS_SCHEMA  # noqa: F401 (used indirectly by tables)


# Point qc's ffmpeg/ffprobe wrappers at our bundled binaries so Paketo
# doesn't need to supply them.
_BIN_DIR = os.path.dirname(os.path.abspath(__file__))
os.environ.setdefault("FFMPEG_BINARY",  os.path.join(_BIN_DIR, "ffmpeg"))
os.environ.setdefault("FFPROBE_BINARY", os.path.join(_BIN_DIR, "ffprobe"))


def init(ctx):
    """One-time init per pod. Validate config + connectivity."""
    cfg_file = load_json_config()
    s3 = S3Client(cfg_file)

    ctx.user_data = {
        "cfg_file": cfg_file,
        "s3":       s3,
        "bucket":   cfg_file["vast"]["bucket"],
        "schema":   cfg_file["vast"].get("schema", "media-catalog"),
        "qc_passed_bucket":  "james-media-qc-passed",
        "qc_failed_bucket":  "james-media-qc-failed",
    }

    # Non-fatal connectivity checks — log and keep going
    endpoint = cfg_file["vast"]["endpoint"]
    host = endpoint.replace("http://", "").replace("https://", "").split(":")[0].split("/")[0]
    try:
        r = subprocess.run(["ping", "-c", "1", "-W", "3", host],
                           capture_output=True, text=True, timeout=5)
        if r.returncode == 0:
            ctx.logger.info(f"Ping {host} OK")
        else:
            ctx.logger.info(f"WARN: ping {host} failed (rc={r.returncode})")
    except Exception as e:
        ctx.logger.info(f"WARN: ping exception: {e}")

    # Ensure source_videos table exists (schema evolution runs here too)
    try:
        import vastdb
        session = vastdb.connect(
            endpoint=cfg_file["vast"]["endpoint"],
            access=cfg_file["vast"]["access_key"],
            secret=cfg_file["vast"]["secret_key"],
        )
        tables.ensure_source_videos_table(session, ctx.user_data["bucket"], ctx.user_data["schema"])
        ctx.logger.info("source_videos table ready")
    except Exception as e:
        ctx.logger.info(f"WARN: source_videos setup deferred: {e}")

    # Preload & log the config so the operator can see exactly what
    # thresholds were active at handler time
    try:
        scope_cfg = load_config("qc-inspector")
        ctx.logger.info(f"qc-inspector config snapshot: {scope_cfg.snapshot()}")
    except Exception as e:
        ctx.logger.info(f"WARN: couldn't load config from DB (will retry per handler): {e}")

    ctx.logger.info("qc-inspector initialized")


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
    # ── 1. Parse event ──
    s3_path = _parse_s3_event(event)
    log(f"[1/5] qc-inspector: {s3_path}")

    # Deterministic source_id (md5 of inbox path).
    source_id = hashlib.md5(s3_path.encode()).hexdigest()
    filename = os.path.basename(s3_path)

    # ── 2. Load runtime config ──
    cfg = load_config("qc-inspector")
    snapshot = cfg.snapshot()
    log(f"[2/5] config snapshot keys: {list(snapshot.keys())}")

    import vastdb
    cfg_file = ctx.user_data["cfg_file"]
    session = vastdb.connect(
        endpoint=cfg_file["vast"]["endpoint"],
        access=cfg_file["vast"]["access_key"],
        secret=cfg_file["vast"]["secret_key"],
    )

    # Seed a "pending" row so partial failures still show up in the DB.
    bucket, schema = ctx.user_data["bucket"], ctx.user_data["schema"]
    def mark(status: str, **extra):
        """Update source_videos row with a progress marker. Every checkpoint
        bumps `updated_at`, so we can see from the outside exactly where the
        handler died if it ever stops progressing."""
        tables.upsert_source_video(session, bucket, schema, {
            "source_id": source_id, "qc_status": status, **extra,
        })

    tables.upsert_source_video(session, bucket, schema, {
        "source_id":       source_id,
        "s3_inbox_path":   s3_path,
        "current_s3_path": s3_path,
        "filename":        filename,
        "uploaded_at":     time.time(),
        "qc_status":       "pending:seeded",
        "status":          "active",
    })

    # ── 3. Download + probe ──
    log("[3a/5] starting download...")
    try:
        local = ctx.user_data["s3"].download_to_temp(s3_path)
    except Exception as e:
        mark("failed:download", qc_verdict_reason=f"download failed: {e!r}")
        raise
    mark("pending:downloaded")
    try:
        log(f"[3b/5] downloaded → {local}")
        try:
            probe = ffprobe.probe_metadata(local)
        except Exception as e:
            mark("failed:probe", qc_verdict_reason=f"ffprobe failed: {e!r}")
            raise
        mark("pending:probed",
             duration_seconds=probe.get("duration_seconds"),
             bitrate_total=probe.get("bit_rate"),
             file_size_bytes=probe.get("size_bytes"))
        v = probe.get("video") or {}
        a = probe.get("audio") or {}
        log(f"       probe: duration={probe.get('duration_seconds'):.1f}s  "
            f"video={v.get('codec')}/{v.get('width')}x{v.get('height')}@{v.get('fps')}  "
            f"audio={a.get('codec')}/{a.get('channels')}ch@{a.get('sample_rate')}")

        # ── 4. Run detectors (checkpoint each so we can see which one hangs) ──
        log(f"[4/5] running detectors...")

        mark("pending:black_starting")
        try:
            black_runs = _timed(log, "black", qc.detect_black_frames, local,
                                cfg.get_duration("black_frame_min_run_seconds"),
                                cfg.get_float("black_frame_pixel_threshold"))
        except Exception as e:
            mark("failed:black", qc_verdict_reason=f"black detect: {e!r}")
            raise
        mark("pending:black_done")

        mark("pending:freeze_starting")
        try:
            freeze_runs = _timed(log, "freeze", qc.detect_freeze_frames, local,
                                 cfg.get_duration("freeze_min_run_seconds"),
                                 cfg.get_float("freeze_noise_threshold"))
        except Exception as e:
            mark("failed:freeze", qc_verdict_reason=f"freeze detect: {e!r}")
            raise
        mark("pending:freeze_done")

        silence_runs = []
        if a:
            mark("pending:silence_starting")
            try:
                silence_runs = _timed(log, "silence", qc.detect_silence, local,
                                      cfg.get_duration("silence_min_run_seconds"),
                                      cfg.get_float("silence_threshold_db"))
            except Exception as e:
                mark("failed:silence", qc_verdict_reason=f"silence detect: {e!r}")
                raise
            mark("pending:silence_done")

        loudness = None
        if a and cfg.get_bool("loudness_enabled"):
            mark("pending:loudness_starting")
            try:
                loudness = _timed(log, "loudness", qc.measure_loudness, local)
            except Exception as e:
                mark("failed:loudness", qc_verdict_reason=f"loudness: {e!r}")
                raise
            mark("pending:loudness_done")

        vfr = None
        if cfg.get_bool("vfr_detection_enabled"):
            mark("pending:vfr_starting")
            try:
                vfr = _timed(log, "vfr", qc.detect_vfr, local)
            except Exception as e:
                mark("failed:vfr", qc_verdict_reason=f"vfr: {e!r}")
                raise
            mark("pending:vfr_done")

        inter = None
        if cfg.get_bool("interlaced_detection_enabled"):
            mark("pending:idet_starting")
            try:
                inter = _timed(log, "interlaced", qc.detect_interlaced, local)
            except Exception as e:
                mark("failed:interlaced", qc_verdict_reason=f"idet: {e!r}")
                raise
            mark("pending:idet_done")

        mark("pending:detectors_done")

        # ── 5. Policy ──
        verdict = qc_policy.evaluate_qc(
            probe, black_runs, freeze_runs, silence_runs,
            loudness, vfr, inter, cfg,
        )
        log(f"[5/5] verdict: {verdict['status']} — {verdict['reason']}")
        for issue in verdict["issues"]:
            log(f"      • {issue}")

    finally:
        try: os.unlink(local)
        except OSError: pass

    # ── 6. Write full row ──
    row = {
        "source_id":        source_id,
        "s3_inbox_path":    s3_path,
        "filename":         filename,
        "duration_seconds": probe.get("duration_seconds"),
        "bitrate_total":    probe.get("bit_rate"),
        "file_size_bytes":  probe.get("size_bytes"),
        "video_codec":      v.get("codec"),
        "video_profile":    v.get("profile"),
        "width":            v.get("width"),
        "height":           v.get("height"),
        "fps":              v.get("fps"),
        "pixel_format":     v.get("pix_fmt"),
        "color_space":      v.get("color_space"),
        "color_range":      v.get("color_range"),
        "audio_codec":      a.get("codec"),
        "audio_channels":   a.get("channels"),
        "audio_sample_rate":a.get("sample_rate"),
        "audio_layout":     a.get("layout"),
        "qc_status":        verdict["status"],
        "qc_verdict_reason":verdict["reason"],
        "qc_issues_json":   tables.json_runs(verdict["issues"]),
        "qc_black_runs_json":   tables.json_runs(black_runs),
        "qc_freeze_runs_json":  tables.json_runs(freeze_runs),
        "qc_silence_runs_json": tables.json_runs(silence_runs),
        "qc_black_ratio":   verdict["ratios"]["black"],
        "qc_freeze_ratio":  verdict["ratios"]["freeze"],
        "qc_silence_ratio": verdict["ratios"]["silence"],
        "qc_loudness_lufs": (loudness or {}).get("integrated_lufs"),
        "qc_true_peak_dbtp":(loudness or {}).get("true_peak_dbtp"),
        "qc_is_vfr":        bool((vfr or {}).get("is_vfr", False)),
        "qc_is_interlaced": bool((inter or {}).get("is_interlaced", False)),
        "qc_config_snapshot_json": json.dumps(snapshot),
        "qc_checked_at":    time.time(),
        "qc_elapsed_seconds": round(time.monotonic() - t_start, 3),
    }
    tables.upsert_source_video(session, bucket, schema, row)

    # ── 7. Move file to qc-passed or qc-failed ──
    dest_bucket = (
        ctx.user_data["qc_failed_bucket"] if verdict["status"] == "failed"
        else ctx.user_data["qc_passed_bucket"]
    )
    mark(f"{verdict['status']}:moving")
    try:
        new_s3 = s3_helpers.move_object(
            ctx.user_data["s3"].client,
            src_s3=s3_path,
            dst_bucket=dest_bucket,
            metadata={
                "source-id":  source_id,
                "qc-status":  verdict["status"],
            },
        )
    except Exception as e:
        mark(f"{verdict['status']}:move_failed",
             qc_verdict_reason=f"move failed: {e!r}")
        raise
    log(f"       moved → {new_s3}")

    tables.upsert_source_video(session, bucket, schema, {
        "source_id":       source_id,
        "current_s3_path": new_s3,
        "qc_status":       verdict["status"],   # restore final verdict (was :moving)
        "status":          "quarantined" if verdict["status"] == "failed" else "active",
    })

    total = time.monotonic() - t_start
    log(f"Done. qc-inspector complete for {filename} — total {total:.2f}s")
    return json.dumps({
        "source_id": source_id,
        "status":    verdict["status"],
        "reason":    verdict["reason"],
        "elapsed":   total,
    })


# ── helpers ────────────────────────────────────────────────────────────

def _parse_s3_event(event) -> str:
    """Accept the various event shapes DataEngine hands us."""
    if hasattr(event, "object_key") and hasattr(event, "bucket"):
        return f"s3://{event.bucket}/{event.object_key}"
    if hasattr(event, "body"):
        b = event.body.decode("utf-8") if isinstance(event.body, bytes) else str(event.body)
        return b.strip()
    raise RuntimeError(f"unsupported event type: {type(event).__name__}")


def _timed(log, label, fn, *args, **kwargs):
    t0 = time.monotonic()
    try:
        res = fn(*args, **kwargs)
        log(f"       [t] {label}: {time.monotonic() - t0:.2f}s")
        return res
    except Exception as e:
        log(f"       [t] {label}: {time.monotonic() - t0:.2f}s FAILED ({type(e).__name__}: {e})")
        raise
