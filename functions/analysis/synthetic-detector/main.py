"""Analysis: AI-generated content detection via deep metadata scan.

Downloads video, runs comprehensive ffprobe metadata extraction, scans for
AI tool signatures, C2PA markers, and encoding anomalies.

Writes: ai_probability, ai_tool_detected, ai_model_version, ai_detection_method,
        ai_detected_at
"""

import hashlib
import json
import os
import subprocess
import time
import traceback

from config_loader import load_config
from s3_client import S3Client
from vast_client import VastDBClient
from schemas import ASSETS_SCHEMA

# Known AI generation tool signatures in video metadata
AI_TOOL_SIGNATURES = {
    "runway": {"tool": "Runway Gen", "weight": 0.9},
    "pika": {"tool": "Pika Labs", "weight": 0.9},
    "sora": {"tool": "OpenAI Sora", "weight": 0.95},
    "stable video": {"tool": "Stable Video Diffusion", "weight": 0.9},
    "synthesia": {"tool": "Synthesia", "weight": 0.85},
    "d-id": {"tool": "D-ID", "weight": 0.85},
    "heygen": {"tool": "HeyGen", "weight": 0.85},
    "midjourney": {"tool": "Midjourney", "weight": 0.8},
    "dall-e": {"tool": "DALL-E", "weight": 0.8},
    "stable diffusion": {"tool": "Stable Diffusion", "weight": 0.8},
    "deforum": {"tool": "Deforum", "weight": 0.85},
    "gen-2": {"tool": "Runway Gen-2", "weight": 0.9},
    "kling": {"tool": "Kling AI", "weight": 0.9},
    "luma": {"tool": "Luma Dream Machine", "weight": 0.9},
}

C2PA_MARKERS = ["c2pa", "content credentials", "content authenticity", "cai"]

AI_ENCODING_PATTERNS = {
    "unusual_fps": [8.0, 12.0, 15.0],
    "suspicious_codecs": ["vp9", "av1"],
}


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

    ctx.logger.info("synthetic-detector initialized")


def handler(ctx, event):
    """Detect AI-generated content via metadata scanning."""
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

    s3 = ctx.user_data["s3"]
    asset_id = hashlib.md5(s3_path.encode()).hexdigest()
    log(f"       asset_id={asset_id}")

    # ── Step 2: Download video ──
    log("[2/4] Downloading from S3 to temp file...")
    local_path = s3.download_to_temp(s3_path)
    log(f"       Downloaded to {local_path}")

    try:
        # ── Step 3: Extract metadata and scan for AI markers ──
        log("[3/4] Extracting metadata with ffprobe...")
        metadata = _extract_full_metadata(ctx, local_path)
        probability, tool, model_ver, methods = _scan_for_ai_markers(metadata)
        log(f"       AI probability={probability:.2f} tool={tool} methods={methods}")

        # ── Step 4: Upsert to VAST DB ──
        fields = {
            "s3_path": s3_path,
            "ai_probability": probability,
            "ai_tool_detected": tool,
            "ai_model_version": model_ver,
            "ai_detection_method": json.dumps(methods),
            "ai_detected_at": time.time(),
        }
        log(f"[4/4] Upserting {len(fields)} fields to assets table (asset_id={asset_id})...")
        vast.upsert_asset(asset_id, fields)
        log("       Upsert complete")

        log(f"Done. asset_id={asset_id}  ai_prob={probability:.2f}")
        return json.dumps({"asset_id": asset_id, "ai_probability": probability, "status": "ok"})

    finally:
        os.unlink(local_path)
        log("       Cleaned up temp file")


def _extract_full_metadata(ctx, video_path):
    """Extract comprehensive metadata including tags, comments, and format info."""
    _bin_dir = os.path.dirname(os.path.abspath(__file__))
    ffprobe_bin = os.path.join(_bin_dir, "ffprobe")
    cmd = [
        ffprobe_bin, "-v", "quiet", "-print_format", "json",
        "-show_format", "-show_streams", "-show_entries", "format_tags",
        video_path,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            return {}
        return json.loads(result.stdout)
    except Exception as e:
        ctx.logger.warning(f"ffprobe metadata extraction failed: {e}")
        return {}


def _scan_for_ai_markers(metadata):
    """Analyze metadata for AI generation markers.

    Returns (probability, tool_detected, model_version, methods_used).
    """
    probability = 0.0
    tool_detected = "none"
    model_version = "unknown"
    methods = []

    flat_text = json.dumps(metadata).lower()

    # Check AI tool signatures
    for keyword, info in AI_TOOL_SIGNATURES.items():
        if keyword in flat_text:
            probability = max(probability, info["weight"])
            tool_detected = info["tool"]
            methods.append(f"metadata_signature:{keyword}")

    # Check C2PA markers
    for marker in C2PA_MARKERS:
        if marker in flat_text:
            methods.append(f"c2pa_marker:{marker}")
            if probability == 0.0:
                probability = 0.3
            break

    # Check AI-related tags
    fmt = metadata.get("format", {})
    tags = fmt.get("tags", {})
    comment = str(tags.get("comment", "")).lower()
    description = str(tags.get("description", "")).lower()
    encoder = str(tags.get("encoder", "")).lower()
    software = str(tags.get("software", "")).lower()

    ai_keywords = ["ai generated", "artificial intelligence", "neural", "diffusion",
                    "generated by", "synthesized", "deepfake", "gan"]
    for kw in ai_keywords:
        for field in [comment, description, encoder, software]:
            if kw in field:
                probability = max(probability, 0.7)
                methods.append(f"tag_keyword:{kw}")
                break

    # Extract model version
    for tag_val in [comment, description, encoder, software]:
        for prefix in ["v", "version ", "model "]:
            idx = tag_val.find(prefix)
            if idx >= 0:
                ver_str = tag_val[idx:idx + 20].split()[0]
                if any(c.isdigit() for c in ver_str):
                    model_version = ver_str
                    break

    # Encoding anomalies
    streams = metadata.get("streams", [])
    video_stream = next((s for s in streams if s.get("codec_type") == "video"), {})

    r_frame_rate = video_stream.get("r_frame_rate", "0/1")
    try:
        if "/" in r_frame_rate:
            num, den = r_frame_rate.split("/")
            fps = int(num) / int(den) if int(den) > 0 else 0
        else:
            fps = float(r_frame_rate)
        if fps in AI_ENCODING_PATTERNS["unusual_fps"]:
            probability = max(probability, probability + 0.1)
            methods.append(f"unusual_fps:{fps}")
    except (ValueError, ZeroDivisionError):
        pass

    # Missing creation_time
    if not tags.get("creation_time"):
        probability = max(probability, probability + 0.05)
        methods.append("missing_creation_time")

    probability = min(probability, 1.0)

    if not methods:
        methods.append("no_markers_found")

    return probability, tool_detected, model_version, methods
