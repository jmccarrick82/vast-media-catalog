"""Foundation: Create subclips from long videos and upload to S3.

When a video file is ingested, checks if duration > threshold (default 30s).
If so, splits into N subclips using ffmpeg stream copy and uploads each to
a separate subclips bucket (james-media-subclips). Since subclips go to a
different bucket, they do not re-trigger the main pipeline.

Writes: is_subclip, subclip_count, subclip_parent_asset_id, relationships
"""

import hashlib
import json
import math
import os
import subprocess
import time
import traceback

from config_loader import load_config
from s3_client import S3Client
from vast_client import VastDBClient
from video_analyzer import extract_metadata, create_subclip
from schemas import ASSETS_SCHEMA, RELATIONSHIPS_SCHEMA


def init(ctx):
    """One-time initialization: load config, create clients.

    Validates connectivity but catches failures so the container starts up
    even if VAST DB is temporarily unreachable.  Table setup is deferred to
    first handler call.
    """
    config = load_config()

    s3 = S3Client(config)
    vast = VastDBClient(config)

    ctx.user_data = {
        "config": config,
        "s3": s3,
        "vast": vast,
        "_tables_ready": False,
    }

    # ── Connectivity validation (non-fatal) ──
    endpoint = config["vast"]["endpoint"]
    host = endpoint.replace("http://", "").replace("https://", "").split(":")[0].split("/")[0]

    try:
        ping_result = subprocess.run(
            ["ping", "-c", "1", "-W", "3", host],
            capture_output=True, text=True, timeout=5,
        )
        if ping_result.returncode == 0:
            ctx.logger.info(f"Ping to {host} OK")
        else:
            ctx.logger.info(f"WARNING: Ping to {host} failed (rc={ping_result.returncode})")
    except Exception as e:
        ctx.logger.info(f"WARNING: Ping to {host} raised exception: {e}")

    try:
        ctx.logger.info(f"Validating VAST DB connection to {endpoint}...")
        session = vast._connect()
        with session.transaction() as tx:
            tx.bucket(config["vast"]["bucket"])
        ctx.logger.info("VAST DB connection validated OK")
    except Exception as e:
        ctx.logger.info(f"WARNING: VAST DB connection failed: {e}")
        ctx.logger.info("Table setup will be retried on first handler call")

    ctx.logger.info("video-subclip initialized")


def handler(ctx, event):
    """Create subclips from long videos and upload to S3."""
    log = ctx.logger.info

    try:
        return _handle(ctx, event)
    except Exception as e:
        log(f"HANDLER ERROR: {type(e).__name__}: {e}")
        log(f"TRACEBACK: {traceback.format_exc()}")
        raise


def _handle(ctx, event):
    """Inner handler with full processing logic."""
    log = ctx.logger.info

    vast = ctx.user_data["vast"]

    # ── Lazy table setup on first handler call ──
    if not ctx.user_data["_tables_ready"]:
        log("Setting up VAST DB tables (first handler call)...")
        vast.setup_tables([
            ("assets", ASSETS_SCHEMA),
            ("relationships", RELATIONSHIPS_SCHEMA),
        ], logger=ctx.logger)
        ctx.user_data["_tables_ready"] = True
        log("Table setup complete")

    # ── Step 1: Parse event ──
    log(f"[1/4] Event received — type: {type(event).__name__}")

    if hasattr(event, "object_key") and hasattr(event, "bucket"):
        bucket_name = str(event.bucket)
        object_key = str(event.object_key)
        s3_path = f"s3://{bucket_name}/{object_key}"
        log(f"       bucket={bucket_name}  key={object_key}")
    elif hasattr(event, "body"):
        s3_path = event.body.decode("utf-8") if isinstance(event.body, bytes) else str(event.body)
        s3_path = s3_path.strip()
    else:
        log(f"       Event attrs: {[a for a in dir(event) if not a.startswith('_')]}")
        raise RuntimeError(f"Cannot extract s3_path from {type(event).__name__}")
    log(f"       s3_path={s3_path}")

    config = ctx.user_data["config"]
    s3 = ctx.user_data["s3"]
    asset_id = hashlib.md5(s3_path.encode()).hexdigest()
    log(f"       asset_id={asset_id}")

    # ── Recursion guard: skip hidden subfolder paths ──
    if _path_has_hidden_component(s3_path):
        log(f"       Skipping hidden-path file: {s3_path}")
        return json.dumps({"asset_id": asset_id, "status": "skipped_hidden_path"})

    # ── Step 2: Download and check duration ──
    log("[2/4] Downloading from S3 to temp file...")
    local_path = s3.download_to_temp(s3_path)
    log(f"       Downloaded to {local_path}")

    try:
        meta = extract_metadata(local_path)
        duration = meta.get("duration_seconds") or 0.0

        # ── Load subclip config with defaults ──
        subclip_config = config.get("subclip", {})
        clip_duration = subclip_config.get("duration_seconds", 30)
        max_subclips = subclip_config.get("max_subclips", 100)

        if duration <= clip_duration:
            log(f"       Duration {duration:.1f}s <= threshold {clip_duration}s, skipping")
            return json.dumps({"asset_id": asset_id, "status": "skipped_short"})

        # ── Step 3: Calculate and create subclips ──
        # ceil() creates a degenerate tail subclip for anything >N*clip_duration
        # (e.g. a 90.3s source gets 4 subclips where #4 is 0.3s). Require the
        # tail to be at least half a clip_duration — otherwise just fold it
        # into "we already got full coverage at N subclips". Also enforce a
        # hard minimum duration so 5s random stub clips never get uploaded.
        MIN_TAIL_FRAC = 0.5          # last subclip must be at least 50% of clip_duration
        MIN_SUBCLIP_SECONDS = 5.0    # absolute lower bound regardless of config

        floor_count = int(duration // clip_duration)    # full-length clips that fit
        tail = duration - floor_count * clip_duration   # leftover seconds

        if tail >= max(clip_duration * MIN_TAIL_FRAC, MIN_SUBCLIP_SECONDS):
            num_subclips = floor_count + 1
        elif floor_count >= 1:
            num_subclips = floor_count
        else:
            # Duration < clip_duration — caller already returned "skipped_short"
            # above, but guard anyway.
            num_subclips = 1 if duration >= MIN_SUBCLIP_SECONDS else 0

        num_subclips = min(num_subclips, max_subclips)
        if num_subclips == 0:
            log(f"       Duration {duration:.1f}s too short for any subclip, skipping")
            return json.dumps({"asset_id": asset_id, "status": "skipped_short"})

        log(f"[3/4] Duration {duration:.1f}s -> creating {num_subclips} subclips "
            f"(tail={tail:.1f}s, floor={floor_count})")

        # Build subclip S3 path in separate bucket
        bucket, key = S3Client.parse_s3_path(s3_path)
        filename = os.path.basename(key)
        _, ext = os.path.splitext(filename)
        ext = ext or ".mp4"
        subclip_bucket = config.get("subclip", {}).get("bucket", "james-media-subclips")
        subclip_prefix = os.path.splitext(filename)[0]

        relationship_rows = []
        now = time.time()

        for i in range(num_subclips):
            start_sec = i * clip_duration
            remaining = duration - start_sec
            this_duration = min(clip_duration, remaining)

            subclip_name = f"subclip_{i + 1:03d}{ext}"
            subclip_key = f"{subclip_prefix}/{subclip_name}"
            subclip_s3_path = f"s3://{subclip_bucket}/{subclip_key}"
            subclip_asset_id = hashlib.md5(subclip_s3_path.encode()).hexdigest()

            log(
                f"  Subclip {i + 1}/{num_subclips}: "
                f"{start_sec:.1f}s-{start_sec + this_duration:.1f}s"
            )

            # Create subclip with embedded metadata
            metadata_tags = {
                "parent_asset_id": asset_id,
                "parent_s3_path": s3_path,
                "subclip_index": str(i),
                "subclip_start_seconds": str(start_sec),
            }
            subclip_local = create_subclip(
                local_path, start_sec, this_duration,
                metadata_tags=metadata_tags,
            )

            try:
                # Upload to S3 — the PUT triggers the full pipeline for this subclip
                s3.upload_file(subclip_local, subclip_s3_path, metadata={
                    "parent_asset_id": asset_id,
                    "subclip_index": str(i),
                })

                # Pre-upsert subclip asset row with linkage columns
                vast.upsert_asset(subclip_asset_id, {
                    "s3_path": subclip_s3_path,
                    "filename": subclip_name,
                    "is_subclip": True,
                    "subclip_parent_asset_id": asset_id,
                    "subclip_parent_s3_path": s3_path,
                    "subclip_index": i,
                    "subclip_start_seconds": start_sec,
                    "subclip_duration_seconds": this_duration,
                })

                # Collect relationship row (deterministic ID avoids duplicates on retry)
                relationship_rows.append({
                    "relationship_id": hashlib.md5(
                        f"{asset_id}:subclip:{i}".encode()
                    ).hexdigest(),
                    "parent_asset_id": asset_id,
                    "child_asset_id": subclip_asset_id,
                    "relationship_type": "subclip",
                    "confidence": 1.0,
                    "created_at": now,
                })

            finally:
                if os.path.exists(subclip_local):
                    os.unlink(subclip_local)

        # ── Step 4: Write results ──
        log("[4/4] Writing results...")
        if relationship_rows:
            vast.write_rows("relationships", RELATIONSHIPS_SCHEMA, relationship_rows)
            log(f"       Wrote {len(relationship_rows)} relationship rows")

        # Update parent asset with subclip summary
        vast.upsert_asset(asset_id, {
            "s3_path": s3_path,
            "is_subclip": False,
            "subclip_count": num_subclips,
        })

        log(f"Done. Created {num_subclips} subclips for {asset_id}")
        return json.dumps({
            "asset_id": asset_id,
            "subclip_count": num_subclips,
            "status": "ok",
        })

    finally:
        os.unlink(local_path)
        log("       Cleaned up temp file")


def _path_has_hidden_component(s3_path: str) -> bool:
    """Check if any path component (directory or file) starts with a dot.

    Subclips are stored in hidden subfolders like .filename.mp4/, so when
    the subclip's PUT triggers video-subclip again, this check catches it
    before downloading the file — saving bandwidth.
    """
    try:
        _, key = S3Client.parse_s3_path(s3_path)
    except ValueError:
        return False
    parts = key.split("/")
    return any(part.startswith(".") for part in parts if part)
