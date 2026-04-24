"""Foundation: Extract I-frame keyframes from video and upload to S3.

Downloads the source video, extracts up to 10 I-frames via ffmpeg,
uploads each frame to s3://james-key-frames/{asset_id}/frame_NNNN.jpg,
then uploads a manifest.json sentinel file that downstream functions
(face-detector, clip-embedder) use to discover keyframes.

Writes: keyframe_count, keyframe_s3_prefix, keyframes_extracted_at
"""

import hashlib
import json
import os
import subprocess
import tempfile
import time
import traceback

from config_loader import load_config
from s3_client import S3Client
from vast_client import VastDBClient
from video_analyzer import extract_keyframes
from schemas import ASSETS_SCHEMA

KEYFRAME_BUCKET = "james-key-frames"
MAX_KEYFRAMES = 10  # Covers face-detector (10), clip-embedder (8), hash-generator (5)


def init(ctx):
    """One-time initialization: load config, create clients.

    Validates connectivity but catches failures so the container starts up
    even if VAST DB is temporarily unreachable. Table setup is deferred to
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

    ctx.logger.info("keyframe-extractor initialized")


def handler(ctx, event):
    """Process a new video file: extract keyframes, upload to S3, write manifest."""
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
        vast.setup_tables([("assets", ASSETS_SCHEMA)], logger=ctx.logger)
        ctx.user_data["_tables_ready"] = True
        log("Table setup complete")

    # ── Step 1: Parse event ──
    log(f"[1/5] Event received — type: {type(event).__name__}")

    # VAST DataEngine ElementTriggerVastEvent provides bucket + object_key
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

    s3 = ctx.user_data["s3"]
    asset_id = hashlib.md5(s3_path.encode()).hexdigest()
    filename = os.path.basename(s3_path)
    log(f"       asset_id={asset_id}  filename={filename}")

    # ── Step 2: Download video ──
    log("[2/5] Downloading from S3 to temp file...")
    local_path = s3.download_to_temp(s3_path)
    log(f"       Downloaded to {local_path}")

    frames = []
    try:
        # ── Step 3: Extract keyframes ──
        log(f"[3/5] Extracting I-frame keyframes (max {MAX_KEYFRAMES})...")
        frames = extract_keyframes(local_path, max_frames=MAX_KEYFRAMES)
        log(f"       Extracted {len(frames)} keyframes")

        if not frames:
            log("       No keyframes extracted — writing zero count to assets table")
            fields = {
                "s3_path": s3_path,
                "keyframe_count": 0,
                "keyframe_s3_prefix": "",
                "keyframes_extracted_at": time.time(),
            }
            vast.upsert_asset(asset_id, fields)
            return json.dumps({"asset_id": asset_id, "keyframe_count": 0, "status": "ok"})

        # ── Step 4: Upload keyframes + manifest to james-key-frames bucket ──
        keyframe_prefix = f"s3://{KEYFRAME_BUCKET}/{asset_id}"
        log(f"[4/5] Uploading {len(frames)} keyframes to {keyframe_prefix}/...")

        keyframe_paths = []
        for i, frame_path in enumerate(frames):
            frame_name = f"frame_{i + 1:04d}.jpg"
            dest = f"{keyframe_prefix}/{frame_name}"
            s3.upload_file(frame_path, dest)
            keyframe_paths.append(dest)
            log(f"       Uploaded {frame_name}")

        # Upload manifest.json LAST — this is the sentinel that triggers
        # downstream functions (face-detector, clip-embedder)
        manifest = {
            "asset_id": asset_id,
            "source_s3_path": s3_path,
            "source_filename": filename,
            "keyframe_bucket": KEYFRAME_BUCKET,
            "keyframe_prefix": f"{asset_id}/",
            "keyframe_count": len(frames),
            "keyframe_paths": keyframe_paths,
            "extracted_at": time.time(),
        }

        manifest_tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False,
        )
        json.dump(manifest, manifest_tmp, indent=2)
        manifest_tmp.close()

        manifest_dest = f"{keyframe_prefix}/manifest.json"
        s3.upload_file(manifest_tmp.name, manifest_dest)
        os.unlink(manifest_tmp.name)
        log("       Uploaded manifest.json (sentinel for downstream functions)")

        # ── Step 5: Upsert asset metadata ──
        fields = {
            "s3_path": s3_path,
            "keyframe_count": len(frames),
            "keyframe_s3_prefix": f"{keyframe_prefix}/",
            "keyframes_extracted_at": time.time(),
        }
        log(f"[5/5] Upserting {len(fields)} fields to assets table (asset_id={asset_id})...")
        vast.upsert_asset(asset_id, fields)
        log("       Upsert complete")

        log(f"Done. asset_id={asset_id}  keyframes={len(frames)}  prefix={keyframe_prefix}/")
        return json.dumps({
            "asset_id": asset_id,
            "keyframe_count": len(frames),
            "keyframe_prefix": f"{keyframe_prefix}/",
            "status": "ok",
        })

    finally:
        # Cleanup temp video file
        os.unlink(local_path)
        log("       Cleaned up temp video file")

        # Cleanup temp frame files
        for f in frames:
            try:
                os.unlink(f)
            except OSError:
                pass
        if frames:
            try:
                os.rmdir(os.path.dirname(frames[0]))
            except OSError:
                pass
