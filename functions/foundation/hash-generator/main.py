"""Foundation: Compute cryptographic (SHA-256) and perceptual hashes for video assets.

Downloads the full video for SHA-256 computation.  For perceptual hashing,
tries to read pre-extracted keyframes from the james-key-frames S3 bucket
(uploaded by keyframe-extractor).  Falls back to local ffmpeg extraction if
the keyframes aren't available yet.

Writes: sha256, perceptual_hash, hash_computed_at
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
from video_analyzer import (
    compute_sha256,
    compute_perceptual_hash,
    compute_video_perceptual_hash,
)
from schemas import ASSETS_SCHEMA

KEYFRAME_BUCKET = "james-key-frames"


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

    ctx.logger.info("hash-generator initialized")


def handler(ctx, event):
    """Process a new video file: compute SHA-256 and perceptual hash, upsert to assets table."""
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

    # ── Step 2: Download ──
    log(f"[2/5] Downloading from S3 to temp file...")
    local_path = s3.download_to_temp(s3_path)
    log(f"       Downloaded to {local_path}")

    try:
        # ── Step 3: Compute SHA-256 ──
        log("[3/5] Computing SHA-256 hash...")
        sha256 = compute_sha256(local_path)
        log(f"       sha256={sha256[:16]}...")

        # ── Step 4: Compute perceptual hash ──
        # Try pre-extracted keyframes from S3 first (uploaded by keyframe-extractor)
        log("[4/5] Computing perceptual hash...")
        phash = _phash_from_s3_keyframes(ctx, s3, asset_id)
        if phash:
            log(f"       Used S3 keyframes from {KEYFRAME_BUCKET}")
            log(f"       perceptual_hash={phash[:32]}...")
        else:
            log("       S3 keyframes not available, falling back to local extraction...")
            phash = compute_video_perceptual_hash(local_path)
            if phash:
                log(f"       perceptual_hash={phash[:32]}...")
            else:
                log("       perceptual_hash=(empty — no keyframes extracted)")

        # ── Step 5: Upsert to VAST DB ──
        fields = {
            "s3_path": s3_path,
            "sha256": sha256,
            "perceptual_hash": phash,
            "hash_computed_at": time.time(),
        }
        log(f"[5/5] Upserting {len(fields)} fields to assets table (asset_id={asset_id})...")
        vast.upsert_asset(asset_id, fields)
        log("       Upsert complete")

        log(f"Done. asset_id={asset_id}  sha256={sha256[:16]}...  filename={filename}")
        return json.dumps({"asset_id": asset_id, "sha256": sha256, "status": "ok"})

    finally:
        os.unlink(local_path)
        log("       Cleaned up temp file")


def _phash_from_s3_keyframes(ctx, s3, asset_id):
    """Try to compute perceptual hash from pre-extracted S3 keyframes.

    Returns the composite pHash string, or None if keyframes aren't available.
    """
    log = ctx.logger.info
    manifest_path = f"s3://{KEYFRAME_BUCKET}/{asset_id}/manifest.json"

    try:
        if not s3.file_exists(manifest_path):
            return None

        # Download and parse manifest
        manifest_tmp = s3.download_to_temp(manifest_path)
        try:
            with open(manifest_tmp) as f:
                manifest = json.load(f)
        finally:
            os.unlink(manifest_tmp)

        keyframe_paths = manifest.get("keyframe_paths", [])
        if not keyframe_paths:
            return None

        log(f"       Found {len(keyframe_paths)} keyframes in S3 manifest")

        # Download each keyframe and compute pHash
        hashes = []
        for kf_path in keyframe_paths[:5]:  # pHash uses max 5 frames
            tmp_frame = s3.download_to_temp(kf_path)
            try:
                h = compute_perceptual_hash(tmp_frame)
                hashes.append(h)
            except Exception:
                pass
            finally:
                os.unlink(tmp_frame)

        return "|".join(hashes) if hashes else None

    except Exception as e:
        log(f"       S3 keyframe read failed: {e}")
        return None
