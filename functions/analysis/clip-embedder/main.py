"""Analysis: CLIP embedding extraction for semantic re-use discovery (UC15).

Triggered by manifest.json PUT events on the james-key-frames bucket.
Downloads pre-extracted keyframes (from keyframe-extractor), runs
OpenCLIP ViT-B-32 forward pass, L2-normalizes embeddings, and writes
per-frame vectors to semantic_embeddings table.

Writes: has_semantic_embeddings, embedding_model_name,
        embedding_frame_count, embeddings_extracted_at
        + rows in semantic_embeddings table
"""

import hashlib
import json
import os
import subprocess
import time
import traceback
import uuid

from config_loader import load_config
from s3_client import S3Client
from vast_client import VastDBClient
from schemas import ASSETS_SCHEMA, SEMANTIC_EMBEDDINGS_SCHEMA

KEYFRAME_BUCKET = "james-key-frames"
CLIP_MODEL_NAME = "ViT-B-32"
CLIP_PRETRAINED = "openai"
MAX_FRAMES = 8  # CLIP uses up to 8 frames


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

    ctx.logger.info("clip-embedder initialized")


def handler(ctx, event):
    """Process a manifest.json PUT: compute CLIP embeddings for keyframes."""
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
            ("semantic_embeddings", SEMANTIC_EMBEDDINGS_SCHEMA),
        ], logger=ctx.logger)
        ctx.user_data["_tables_ready"] = True
        log("Table setup complete")

    # ── Step 1: Parse event — only process manifest.json PUTs ──
    log(f"[1/5] Event received — type: {type(event).__name__}")

    if hasattr(event, "object_key") and hasattr(event, "bucket"):
        bucket_name = str(event.bucket)
        object_key = str(event.object_key)
        s3_path = f"s3://{bucket_name}/{object_key}"
        log(f"       bucket={bucket_name}  key={object_key}")
    elif hasattr(event, "body"):
        s3_path = event.body.decode("utf-8") if isinstance(event.body, bytes) else str(event.body)
        s3_path = s3_path.strip()
        object_key = s3_path.split("/")[-1] if "/" in s3_path else s3_path
    else:
        log(f"       Event attrs: {[a for a in dir(event) if not a.startswith('_')]}")
        raise RuntimeError(f"Cannot extract s3_path from {type(event).__name__}")

    # Only process manifest.json files — skip individual frame PUTs
    if not object_key.endswith("manifest.json"):
        log(f"       Skipping non-manifest file: {object_key}")
        return json.dumps({"status": "skipped", "reason": "not manifest.json"})

    log(f"       s3_path={s3_path}")

    # ── Step 2: Download and parse manifest ──
    log("[2/5] Downloading manifest.json...")
    s3 = ctx.user_data["s3"]
    manifest_path = s3.download_to_temp(s3_path)

    try:
        with open(manifest_path) as f:
            manifest = json.load(f)
    finally:
        os.unlink(manifest_path)

    asset_id = manifest["asset_id"]
    source_s3_path = manifest["source_s3_path"]
    keyframe_paths = manifest.get("keyframe_paths", [])
    log(f"       asset_id={asset_id}  source={os.path.basename(source_s3_path)}")
    log(f"       keyframes available: {len(keyframe_paths)}")

    if not keyframe_paths:
        log("       No keyframes in manifest — skipping")
        return json.dumps({"asset_id": asset_id, "status": "no_keyframes"})

    # Limit to MAX_FRAMES for CLIP
    keyframe_paths = keyframe_paths[:MAX_FRAMES]

    # ── Step 3: Download keyframes from S3 ──
    log(f"[3/5] Downloading {len(keyframe_paths)} keyframes...")
    frame_files = []
    for kf_path in keyframe_paths:
        try:
            local_frame = s3.download_to_temp(kf_path)
            frame_files.append(local_frame)
        except Exception as e:
            log(f"       WARNING: Failed to download {kf_path}: {e}")

    log(f"       Downloaded {len(frame_files)} keyframes")

    if not frame_files:
        log("       No keyframes downloaded — skipping")
        return json.dumps({"asset_id": asset_id, "status": "no_frames_downloaded"})

    try:
        # ── Step 4: Load model and compute embeddings ──
        log("[4/5] Loading CLIP model and computing embeddings...")
        model, preprocess, device = _load_clip_model()
        model_name = f"open_clip/{CLIP_MODEL_NAME}/{CLIP_PRETRAINED}"
        log(f"       Model loaded on {device}")

        rows = []
        for idx, frame_path in enumerate(frame_files):
            try:
                embedding = _compute_embedding(model, preprocess, device, frame_path)
                rows.append({
                    "embedding_id": str(uuid.uuid4()),
                    "asset_id": asset_id,
                    "s3_path": source_s3_path,
                    "frame_index": idx,
                    "embedding": embedding,
                    "model_name": model_name,
                    "extracted_at": time.time(),
                })
                log(f"       Frame {idx}: embedding computed (dim={len(embedding)})")
            except Exception as e:
                log(f"       WARNING: Embedding failed for frame {idx}: {e}")

        # ── Step 5: Write results to VAST DB ──
        log(f"[5/5] Writing {len(rows)} embeddings to VAST DB...")

        if rows:
            vast.write_rows("semantic_embeddings", SEMANTIC_EMBEDDINGS_SCHEMA, rows)
            log(f"       Wrote {len(rows)} rows to semantic_embeddings")

            vast.upsert_asset(asset_id, {
                "s3_path": source_s3_path,
                "has_semantic_embeddings": True,
                "embedding_model_name": model_name,
                "embedding_frame_count": len(rows),
                "embeddings_extracted_at": time.time(),
            })
            log("       Asset upsert complete")

        log(f"Done. asset_id={asset_id}  embeddings={len(rows)}")
        return json.dumps({
            "asset_id": asset_id,
            "embeddings": len(rows),
            "status": "ok",
        })

    finally:
        for fp in frame_files:
            try:
                os.unlink(fp)
            except OSError:
                pass
        log("       Cleaned up temp files")


def _load_clip_model():
    """Load OpenCLIP model and preprocessing transform."""
    import open_clip
    import torch

    device = "cuda" if torch.cuda.is_available() else "cpu"
    model, _, preprocess = open_clip.create_model_and_transforms(
        CLIP_MODEL_NAME, pretrained=CLIP_PRETRAINED,
    )
    model = model.to(device)
    model.eval()
    return model, preprocess, device


def _compute_embedding(model, preprocess, device, image_path):
    """Compute L2-normalized CLIP embedding for a single image."""
    import torch
    from PIL import Image

    img = Image.open(image_path).convert("RGB")
    img_tensor = preprocess(img).unsqueeze(0).to(device)

    with torch.no_grad():
        features = model.encode_image(img_tensor)
        features = features / features.norm(dim=-1, keepdim=True)

    return features.squeeze().cpu().numpy().tolist()
