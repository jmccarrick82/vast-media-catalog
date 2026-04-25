"""james-ai-clipper — Phase 2 of the pre-ingest pipeline.

Trigger: S3 PUT on `s3://james-media-qc-passed/*`

Pipeline:
  1. Resolve the prompt:
       a. x-amz-meta-clip-prompt on the inbound object (from uploader)
       b. sidecar JSON (same key with .clip.json)      [not yet]
       c. fallback: `default_clip_prompt` from function_configs
  2. Download the qc-passed video locally once.
  3. Probe to get duration + fps.
  4. Detect shots via ffmpeg scene filter (configurable threshold).
  5. For each shot: extract mid-point keyframe, classify with 11B model;
     escalate borderline-confidence frames to 90B.
  6. Keep matching shots, merge adjacent ones (within `merge_gap_seconds`),
     constrain by min/max duration and max clip count.
  7. Cut each clip (ffmpeg -c copy by default) and upload to
     `s3://james-media-clips/<source_id>/clip-NNN.mp4` with x-amz-meta
     tags (source-id, clip-id, confidence, prompt).
  8. Write rows to `extracted_clips` VAST DB table and update the
     `source_videos` row (clip_count, clip_extraction_status).

All thresholds live in `function_configs` under scope `ai-clipper`.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import traceback

from config_loader import load_config as load_json_config
from config import load_config
from s3_client import S3Client

from ingest import ffprobe, scene, vision, clips
from ingest import s3_helpers, tables
from schemas import EXTRACTED_CLIPS_SCHEMA  # noqa: F401


# Point bundled binaries at ours so Paketo doesn't need apt-provided ones
_BIN_DIR = os.path.dirname(os.path.abspath(__file__))
os.environ.setdefault("FFMPEG_BINARY",  os.path.join(_BIN_DIR, "ffmpeg"))
os.environ.setdefault("FFPROBE_BINARY", os.path.join(_BIN_DIR, "ffprobe"))


def init(ctx):
    cfg_file = load_json_config()
    s3 = S3Client(cfg_file)

    ctx.user_data = {
        "cfg_file":      cfg_file,
        "s3":            s3,
        "bucket":        cfg_file["vast"]["bucket"],
        "schema":        cfg_file["vast"].get("schema", "media-catalog"),
        "clips_bucket":  cfg_file.get("buckets", {}).get("clips_bucket", "james-media-clips"),
        "inference_key": cfg_file.get("inference", {}).get("api_key"),
    }

    # Ensure extracted_clips table exists (schema evolution too)
    try:
        import vastdb
        session = vastdb.connect(
            endpoint=cfg_file["vast"]["endpoint"],
            access=cfg_file["vast"]["access_key"],
            secret=cfg_file["vast"]["secret_key"],
        )
        tables.ensure_extracted_clips_table(
            session, ctx.user_data["bucket"], ctx.user_data["schema"]
        )
        ctx.logger.info("extracted_clips table ready")
    except Exception as e:
        ctx.logger.info(f"WARN: extracted_clips setup deferred: {e}")

    # Preload + log the config snapshot
    try:
        cfg = load_config("ai-clipper")
        ctx.logger.info(f"ai-clipper config snapshot keys: {list(cfg.snapshot().keys())}")
    except Exception as e:
        ctx.logger.info(f"WARN: couldn't load ai-clipper config: {e}")

    ctx.logger.info("ai-clipper initialized")


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
    # 1. Parse event → s3 path on qc-passed bucket
    s3_path = _parse_s3_event(event)
    log(f"[1/8] ai-clipper triggered on {s3_path}")

    # The source_id was set in qc-inspector based on the original INBOX
    # path. The inbox path and the qc-passed path share the same key, so
    # we can reconstruct it (same filename, same key, different bucket).
    _, key = s3_helpers.parse_s3_path(s3_path)
    inbox_path = f"s3://james-media-inbox/{key}"
    source_id = hashlib.md5(inbox_path.encode()).hexdigest()
    filename = os.path.basename(key)
    log(f"       source_id={source_id}  filename={filename}")

    # 2. Config
    cfg = load_config("ai-clipper")
    snapshot = cfg.snapshot()
    log(f"[2/8] config loaded ({len(snapshot)} keys)")

    # 3. VAST session + mark extraction started
    import vastdb
    cfg_file = ctx.user_data["cfg_file"]
    session = vastdb.connect(
        endpoint=cfg_file["vast"]["endpoint"],
        access=cfg_file["vast"]["access_key"],
        secret=cfg_file["vast"]["secret_key"],
    )
    bucket, schema = ctx.user_data["bucket"], ctx.user_data["schema"]

    def mark_source(**extra):
        tables.upsert_source_video(session, bucket, schema,
                                   {"source_id": source_id, **extra})

    # Seed identity fields up front. qc-inspector normally owns these, but
    # if ai-clipper is the first writer (direct upload to qc-passed,
    # tests, or qc-inspector skipped), we still want a usable row that
    # the UI can render. upsert_source_video skips None values so we
    # never clobber data qc-inspector already wrote.
    mark_source(
        clip_extraction_status="pending:started",
        filename=filename,
        s3_inbox_path=inbox_path,
        current_s3_path=s3_path,
        uploaded_at=time.time(),
        status="active",
    )

    # 4. Resolve prompt (S3 metadata → sidecar → default)
    try:
        meta = s3_helpers.get_object_tags(ctx.user_data["s3"].client, s3_path)
    except Exception as e:
        log(f"       WARN: could not read object metadata: {e}")
        meta = {}
    prompt = None
    prompt_source = None
    if meta.get("clip-prompt"):
        prompt = meta["clip-prompt"]
        prompt_source = "s3_metadata"
    if not prompt:
        prompt = cfg.get_string("default_clip_prompt")
        prompt_source = "default"
    log(f"[3/8] prompt ({prompt_source}): {prompt!r}")

    # 5. Download + probe
    log("[4/8] downloading...")
    try:
        local = ctx.user_data["s3"].download_to_temp(s3_path)
    except Exception as e:
        mark_source(clip_extraction_status=f"failed:download: {e!r}")
        raise

    clip_ids_written: list[str] = []
    try:
        probe = ffprobe.probe_metadata(local)
        duration = float(probe.get("duration_seconds") or 0.0)
        if duration <= 0:
            mark_source(clip_extraction_status="failed:zero_duration")
            raise RuntimeError(f"probe returned duration={duration!r}")
        log(f"       duration={duration:.1f}s")

        # 6. Scene detection
        log("[5/8] detecting shots...")
        shots = scene.detect_scenes(
            local,
            duration_seconds=duration,
            threshold=cfg.get_float("scene_change_threshold"),
            min_shot_seconds=cfg.get_duration("min_shot_seconds"),
            max_shot_seconds=cfg.get_duration("max_shot_seconds"),
        )
        log(f"       {len(shots)} shots")

        if not shots:
            mark_source(clip_extraction_status="done", clip_count=0, clip_prompt=prompt,
                        clip_prompt_source=prompt_source, clip_extracted_at=time.time())
            return json.dumps({"source_id": source_id, "clips_extracted": 0,
                               "reason": "no shots detected"})

        # 7. Classify each shot via vision model (with escalation)
        log("[6/8] classifying shots with vision model...")
        api_key = ctx.user_data.get("inference_key")
        if not api_key:
            mark_source(clip_extraction_status="failed:no_inference_key")
            raise RuntimeError("no inference api_key in config.json")

        primary = cfg.get_string("vision_model_primary")
        escalate = cfg.get_string("vision_model_escalation")
        low = cfg.get_float("escalation_confidence_low")
        high = cfg.get_float("escalation_confidence_high")
        timeout = int(cfg.get_duration("inference_timeout_seconds"))
        retries = cfg.get_int("inference_retries")
        step_delay = cfg.get_duration("inference_step_delay_seconds")

        keyframe_dir = os.path.join("/tmp", f"aiclip-{source_id}")
        os.makedirs(keyframe_dir, exist_ok=True)

        matched: list[clips.MatchedShot] = []
        for i, (s, e) in enumerate(shots):
            mid = (s + e) / 2
            jpg = os.path.join(keyframe_dir, f"shot-{i:04d}.jpg")
            try:
                scene.extract_keyframe(local, mid, jpg)
            except Exception as ke:
                log(f"       shot {i}: keyframe extract failed — {ke}")
                continue

            try:
                verdict = vision.classify_with_escalation(
                    jpg,
                    prompt,
                    api_key=api_key,
                    primary_model=primary,
                    escalation_model=escalate,
                    low=low,
                    high=high,
                    timeout=timeout,
                    retries=retries,
                )
            except Exception as ce:
                log(f"       shot {i}: classify failed — {ce}")
                continue

            tag = "ESC" if verdict.get("escalated") else "   "
            log(
                f"       shot {i:03d} {s:6.1f}..{e:6.1f}s  "
                f"{'MATCH' if verdict['match'] else 'nope '}  "
                f"conf={verdict['confidence']:.2f}  {tag}  {verdict['reason'][:80]}"
            )

            if verdict["match"] and verdict["confidence"] >= low:
                matched.append(clips.MatchedShot(
                    start=s, end=e,
                    confidence=verdict["confidence"],
                    reason=verdict["reason"],
                    model=verdict.get("model") or primary,
                ))

            if step_delay > 0:
                time.sleep(step_delay)

        log(f"       {len(matched)} matching shots")

        # 8. Merge adjacent matches + constrain
        log("[7/8] merging + constraining...")
        merged = clips.merge_matching_shots(
            matched,
            merge_gap_seconds=cfg.get_duration("merge_gap_seconds"),
        )
        constrained = clips.constrain_clips(
            merged,
            min_clip_seconds=cfg.get_duration("min_clip_seconds"),
            max_clip_seconds=cfg.get_duration("max_clip_seconds"),
            max_clips=cfg.get_int("max_clips_per_source"),
        )
        # Editorial buffer — pad the matched span at head/tail (config-driven).
        # Applied AFTER constrain so the buffer is additive on top of the
        # matched-span max length, not bounded by it.
        pre  = cfg.get_duration("clip_buffer_pre_seconds")
        post = cfg.get_duration("clip_buffer_post_seconds")
        buffered = clips.apply_buffer(
            constrained,
            pre_seconds=pre,
            post_seconds=post,
            source_duration=duration,
        )
        log(f"       {len(merged)} merged → {len(constrained)} constrained "
            f"→ {len(buffered)} buffered (+{pre:.1f}s pre / +{post:.1f}s post)")
        constrained = buffered

        # 9. Cut + upload each clip, write DB rows
        log("[8/8] cutting + uploading clips...")
        use_copy = cfg.get_bool("cut_use_stream_copy")

        for idx, clip in enumerate(constrained):
            local_out = os.path.join(keyframe_dir, f"clip-{idx:03d}.mp4")
            try:
                clips.cut_clip(
                    local,
                    clip.start,
                    clip.end,
                    local_out,
                    stream_copy=use_copy,
                )
            except Exception as cutErr:
                log(f"       clip {idx}: cut failed — {cutErr}")
                continue

            clip_key = f"{source_id}/clip-{idx:03d}.mp4"
            clip_s3  = f"s3://{ctx.user_data['clips_bucket']}/{clip_key}"
            try:
                ctx.user_data["s3"].upload_file(
                    local_out, clip_s3,
                    metadata={
                        "source-id":       source_id,
                        "prompt":          prompt[:256],
                        "confidence":      f"{clip.confidence:.3f}",
                        "shot-count":      str(clip.shot_count),
                    },
                )
            except Exception as upErr:
                log(f"       clip {idx}: upload failed — {upErr}")
                continue

            clip_id = hashlib.md5(clip_s3.encode()).hexdigest()
            try:
                size = os.path.getsize(local_out)
            except OSError:
                size = None

            tables.upsert_extracted_clip(session, bucket, schema, {
                "clip_id":           clip_id,
                "source_id":         source_id,
                "clip_index":        idx,
                "clip_s3_path":      clip_s3,
                "start_seconds":     round(clip.start, 3),
                "end_seconds":       round(clip.end, 3),
                "duration_seconds":  round(clip.duration, 3),
                "shot_count":        clip.shot_count,
                "file_size_bytes":   size,
                "prompt":            prompt,
                "prompt_source":     prompt_source,
                "match_confidence":  clip.confidence,
                "match_reason":      clip.reason,
                "vision_model":      clip.model,
                "frame_scores_json": json.dumps([]),
                "status":            "active",
            })
            clip_ids_written.append(clip_id)
            log(f"       ✓ clip {idx:03d}  {clip.start:6.1f}..{clip.end:6.1f}s  "
                f"conf={clip.confidence:.2f}  → {clip_s3}")

        # 10. Update source_videos summary
        mark_source(
            clip_extraction_status="done",
            clip_count=len(clip_ids_written),
            clip_prompt=prompt,
            clip_prompt_source=prompt_source,
            clip_extracted_at=time.time(),
        )

        # 11. Drop a _ready.json marker into the clips bucket so the
        #     media-packager trigger fires. The marker doubles as a
        #     per-source summary so the packager has everything it needs
        #     without re-querying the DB (though it can).
        if clip_ids_written:
            marker_key = f"{source_id}/_ready.json"
            marker_s3  = f"s3://{ctx.user_data['clips_bucket']}/{marker_key}"
            marker = {
                "source_id":     source_id,
                "filename":      filename,
                "prompt":        prompt,
                "prompt_source": prompt_source,
                "clip_ids":      clip_ids_written,
                "clip_count":    len(clip_ids_written),
                "emitted_at":    time.time(),
            }
            try:
                s3_helpers.put_bytes(
                    ctx.user_data["s3"].client,
                    marker_s3,
                    json.dumps(marker, indent=2).encode(),
                    content_type="application/json",
                    metadata={"source-id": source_id, "ready": "true"},
                )
                log(f"       ✓ packager trigger: {marker_s3}")
            except Exception as me:
                log(f"       WARN: failed to write ready marker: {me}")

    finally:
        try: os.unlink(local)
        except OSError: pass
        # best-effort cleanup of keyframes / cut clips
        try:
            import shutil
            shutil.rmtree(keyframe_dir, ignore_errors=True)
        except Exception:
            pass

    total = time.monotonic() - t_start
    log(f"Done. ai-clipper finished {filename} — {len(clip_ids_written)} clips in {total:.2f}s")
    return json.dumps({
        "source_id":       source_id,
        "prompt":          prompt,
        "prompt_source":   prompt_source,
        "clips_extracted": len(clip_ids_written),
        "clip_ids":        clip_ids_written,
        "elapsed":         total,
    })


# ── helpers ────────────────────────────────────────────────────────────

def _parse_s3_event(event) -> str:
    if hasattr(event, "object_key") and hasattr(event, "bucket"):
        return f"s3://{event.bucket}/{event.object_key}"
    if hasattr(event, "body"):
        b = event.body.decode("utf-8") if isinstance(event.body, bytes) else str(event.body)
        return b.strip()
    raise RuntimeError(f"unsupported event type: {type(event).__name__}")
