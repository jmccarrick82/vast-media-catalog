"""Foundation: Detect faces in video keyframes for talent tracking (UC05) and GDPR compliance (UC20).

Triggered by manifest.json PUT events on the james-key-frames bucket.
Downloads pre-extracted keyframes (from keyframe-extractor), runs
face_recognition to detect and cluster faces into unique person IDs,
records per-person frame timestamps.

Writes: faces_detected_count, talent_music_scanned_at,
        gdpr_faces_detected, gdpr_persons_identified, gdpr_scanned_at
        + rows in gdpr_personal_data and talent_music tables
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
from schemas import ASSETS_SCHEMA, GDPR_PERSONAL_DATA_SCHEMA, TALENT_MUSIC_SCHEMA

KEYFRAME_BUCKET = "james-key-frames"


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

    ctx.logger.info("face-detector initialized")


def handler(ctx, event):
    """Process a manifest.json PUT: detect faces in pre-extracted keyframes."""
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
            ("gdpr_personal_data", GDPR_PERSONAL_DATA_SCHEMA),
            ("talent_music", TALENT_MUSIC_SCHEMA),
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
    keyframe_count = manifest.get("keyframe_count", len(keyframe_paths))
    log(f"       asset_id={asset_id}  source={os.path.basename(source_s3_path)}")
    log(f"       keyframes available: {len(keyframe_paths)}")

    if not keyframe_paths:
        log("       No keyframes in manifest — writing zero count")
        vast.upsert_asset(asset_id, {
            "s3_path": source_s3_path,
            "faces_detected_count": 0,
            "talent_music_scanned_at": time.time(),
            "gdpr_faces_detected": 0,
            "gdpr_persons_identified": 0,
            "gdpr_blast_radius": 0,
            "gdpr_scanned_at": time.time(),
        })
        return json.dumps({"asset_id": asset_id, "faces": 0, "status": "ok"})

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

    try:
        # ── Step 4: Detect faces ──
        log("[4/5] Running face detection...")
        detections, person_frames = _detect_faces(ctx, frame_files, keyframe_count)
        log(f"       faces={len(detections)}  unique_persons={len(person_frames)}")

        # ── Step 5: Write results to VAST DB ──
        log("[5/5] Writing results to VAST DB...")

        # Per-person GDPR records
        gdpr_rows = []
        for person_id, frame_indices in person_frames.items():
            timestamps = [round(i / max(keyframe_count, 1), 3) for i in frame_indices]
            gdpr_rows.append({
                "detection_id": str(uuid.uuid4()),
                "asset_id": asset_id,
                "s3_path": source_s3_path,
                "person_id": person_id,
                "data_type": "face",
                "face_detected": True,
                "frame_timestamps": json.dumps(timestamps),
                "detected_at": time.time(),
            })

        if gdpr_rows:
            vast.write_rows("gdpr_personal_data", GDPR_PERSONAL_DATA_SCHEMA, gdpr_rows)
            log(f"       Wrote {len(gdpr_rows)} rows to gdpr_personal_data")

        # Per-detection talent_music records (detection_type = "face")
        talent_rows = []
        for det in detections:
            talent_rows.append({
                "detection_id": str(uuid.uuid4()),
                "asset_id": asset_id,
                "s3_path": source_s3_path,
                "detection_type": "face",
                "label": det["label"],
                "confidence": det["confidence"],
                "start_time_sec": det["start_time_sec"],
                "end_time_sec": det["end_time_sec"],
                "detected_at": time.time(),
            })

        if talent_rows:
            vast.write_rows("talent_music", TALENT_MUSIC_SCHEMA, talent_rows)
            log(f"       Wrote {len(talent_rows)} rows to talent_music")

        # Upsert unified assets columns
        vast.upsert_asset(asset_id, {
            "s3_path": source_s3_path,
            "faces_detected_count": len(detections),
            "talent_music_scanned_at": time.time(),
            "gdpr_faces_detected": len(detections),
            "gdpr_persons_identified": len(person_frames),
            "gdpr_blast_radius": len(person_frames),
            "gdpr_scanned_at": time.time(),
        })
        log("       Asset upsert complete")

        log(f"Done. asset_id={asset_id}  faces={len(detections)}  persons={len(person_frames)}")
        return json.dumps({
            "asset_id": asset_id,
            "faces": len(detections),
            "persons": len(person_frames),
            "status": "ok",
        })

    finally:
        for fp in frame_files:
            try:
                os.unlink(fp)
            except OSError:
                pass
        log("       Cleaned up temp files")


def _detect_faces(ctx, frame_paths, keyframe_count):
    """Detect faces across keyframes using OpenCV Haar cascades.

    Uses Haar cascades (CPU-only, no dlib/cmake required).
    Generates pseudo person IDs from face position and size across frames —
    faces at similar positions in consecutive frames get the same person ID.

    Returns:
        detections: list of dicts with label, confidence, timestamps
        person_frames: dict mapping person_id -> list of frame indices
    """
    import cv2
    import numpy as np

    # Load Haar cascade for face detection
    cascade_path = cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
    face_cascade = cv2.CascadeClassifier(cascade_path)

    if face_cascade.empty():
        ctx.logger.info("WARNING: Could not load Haar cascade classifier")
        return [], {}

    detections = []
    person_frames = {}
    known_faces = []  # list of (center_x, center_y, area, label)

    for frame_idx, frame_path in enumerate(frame_paths):
        try:
            image = cv2.imread(frame_path)
            if image is None:
                ctx.logger.info(f"       Could not read frame {frame_idx}")
                continue
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            faces = face_cascade.detectMultiScale(
                gray, scaleFactor=1.1, minNeighbors=5, minSize=(30, 30),
            )
        except Exception as e:
            ctx.logger.info(f"       Face detection failed on frame {frame_idx}: {e}")
            continue

        img_h, img_w = image.shape[:2]
        img_area = img_h * img_w

        for (x, y, w, h) in faces:
            face_area = w * h
            center_x = x + w // 2
            center_y = y + h // 2

            # Match to known faces by spatial proximity
            label = None
            best_dist = float("inf")
            for kx, ky, ka, klabel in known_faces:
                dist = np.sqrt((center_x - kx) ** 2 + (center_y - ky) ** 2)
                size_ratio = face_area / max(ka, 1)
                if dist < max(w, h) * 2 and 0.3 < size_ratio < 3.0:
                    if dist < best_dist:
                        best_dist = dist
                        label = klabel

            if label is None:
                face_hash = hashlib.md5(
                    f"{center_x}_{center_y}_{face_area}_{frame_idx}".encode()
                ).hexdigest()[:8]
                label = f"person_{face_hash}"
                known_faces.append((center_x, center_y, face_area, label))

            # Confidence from face area relative to image area
            confidence = min(1.0, (face_area / img_area) * 10)

            # Approximate timestamps from frame index position
            start_t = round((frame_idx / max(keyframe_count, 1)) * 100, 2)
            end_t = round(((frame_idx + 1) / max(keyframe_count, 1)) * 100, 2)

            detections.append({
                "label": label,
                "confidence": round(confidence, 3),
                "start_time_sec": start_t,
                "end_time_sec": end_t,
            })

            person_frames.setdefault(label, []).append(frame_idx)

    return detections, person_frames
