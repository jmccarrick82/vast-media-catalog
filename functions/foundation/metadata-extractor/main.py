"""Foundation: Extract video metadata via ffprobe and upsert to unified assets table.

Also extracts embedded custom metadata tags (e.g. parent_asset_id) from subclip
MP4 files, reinforcing the parent→subclip linkage established by video-subclip.
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
from video_analyzer import extract_metadata, FFPROBE
from schemas import ASSETS_SCHEMA


def init(ctx):
    """One-time initialization: load config, create clients.

    Validates connectivity (ping + VAST DB connect) but catches failures
    so the container starts up even if VAST DB is temporarily unreachable.
    DB table setup is deferred to first handler call.
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
    # Extract hostname/IP from endpoint URL
    host = endpoint.replace("http://", "").replace("https://", "").split(":")[0].split("/")[0]

    # Try ping
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

    # Try VAST DB connect
    try:
        ctx.logger.info(f"Validating VAST DB connection to {endpoint}...")
        session = vast._connect()
        with session.transaction() as tx:
            tx.bucket(config["vast"]["bucket"])
        ctx.logger.info(f"VAST DB connection validated OK")
    except Exception as e:
        ctx.logger.info(f"WARNING: VAST DB connection failed: {e}")
        ctx.logger.info(f"Table setup will be retried on first handler call")

    ctx.logger.info("metadata-extractor initialized")


def handler(ctx, event):
    """Process a new video file: extract metadata via ffprobe and upsert to unified assets table."""
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
    log(f"[1/7] Event received — type: {type(event).__name__}")

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

    # ── Step 2: S3 head object ──
    log(f"[2/7] Fetching S3 object metadata...")
    s3_meta = s3.get_object_metadata(s3_path)
    filename = os.path.basename(s3_path)
    asset_id = hashlib.md5(s3_path.encode()).hexdigest()
    log(f"       filename={filename}  asset_id={asset_id}  "
        f"s3_size={s3_meta.get('content_length', 'unknown')} bytes")

    # ── Step 3: Download ──
    log(f"[3/7] Downloading from S3 to temp file...")
    local_path = s3.download_to_temp(s3_path)
    log(f"       Downloaded to {local_path}")

    try:
        # ── Step 4: ffprobe metadata extraction ──
        log(f"[4/7] Running ffprobe metadata extraction...")
        meta = extract_metadata(local_path)
        log(f"       duration={meta.get('duration_seconds')}s  "
            f"resolution={meta.get('width')}x{meta.get('height')}  "
            f"fps={meta.get('fps')}  bitrate={meta.get('bitrate')}")
        log(f"       video_codec={meta.get('video_codec')}  "
            f"audio_codec={meta.get('audio_codec')}  "
            f"pixel_format={meta.get('pixel_format')}")
        log(f"       audio_channels={meta.get('audio_channels')}  "
            f"audio_sample_rate={meta.get('audio_sample_rate')}  "
            f"format={meta.get('format_name')}")
        if meta.get("creation_time"):
            log(f"       creation_time={meta['creation_time']}")
        if meta.get("title"):
            log(f"       title={meta['title']}")
        if meta.get("encoder"):
            log(f"       encoder={meta['encoder']}")

        fields = {
            "s3_path": s3_path,
            "filename": filename,
            "file_size_bytes": meta["file_size_bytes"] or s3_meta["content_length"],
            "duration_seconds": meta["duration_seconds"],
            "video_codec": meta["video_codec"],
            "audio_codec": meta["audio_codec"],
            "width": meta["width"],
            "height": meta["height"],
            "fps": meta["fps"],
            "bitrate": meta["bitrate"],
            "pixel_format": meta["pixel_format"],
            "audio_channels": meta["audio_channels"],
            "audio_sample_rate": meta["audio_sample_rate"],
            "format_name": meta["format_name"],
            "creation_time": meta["creation_time"],
            "title": meta["title"],
            "encoder": meta["encoder"],
            "ingested_at": time.time(),
        }

        # ── Step 5: Extract embedded subclip metadata tags ──
        # video-subclip embeds parent_asset_id, parent_s3_path, subclip_index,
        # and subclip_start_seconds as ffmpeg -metadata tags. If present, this
        # reinforces the parent→subclip linkage from a second source.
        log(f"[5/7] Checking for embedded subclip metadata tags...")
        custom_tags = _extract_custom_tags(local_path)
        if custom_tags.get("parent_asset_id"):
            log(f"       Found subclip tags: parent_asset_id={custom_tags['parent_asset_id']}")
            fields["is_subclip"] = True
            fields["subclip_parent_asset_id"] = custom_tags["parent_asset_id"]
            if custom_tags.get("parent_s3_path"):
                fields["subclip_parent_s3_path"] = custom_tags["parent_s3_path"]
                log(f"       parent_s3_path={custom_tags['parent_s3_path']}")
            if custom_tags.get("subclip_index") is not None:
                try:
                    fields["subclip_index"] = int(custom_tags["subclip_index"])
                    log(f"       subclip_index={fields['subclip_index']}")
                except (ValueError, TypeError):
                    pass
            if custom_tags.get("subclip_start_seconds") is not None:
                try:
                    fields["subclip_start_seconds"] = float(
                        custom_tags["subclip_start_seconds"]
                    )
                    log(f"       subclip_start_seconds={fields['subclip_start_seconds']}")
                except (ValueError, TypeError):
                    pass
        else:
            log(f"       No subclip tags found (standalone asset)")

        # ── Step 6: Upsert to VAST DB ──
        log(f"[6/7] Upserting {len(fields)} fields to assets table (asset_id={asset_id})...")
        vast.upsert_asset(asset_id, fields)
        log(f"       Upsert complete")

        # ── Step 7: Done ──
        log(f"[7/7] Done. asset_id={asset_id}  filename={filename}  "
            f"duration={meta.get('duration_seconds')}s  "
            f"size={fields['file_size_bytes']} bytes")
        return json.dumps({"asset_id": asset_id, "status": "ok"})

    finally:
        os.unlink(local_path)
        log(f"       Cleaned up temp file")


def _extract_custom_tags(video_path: str) -> dict:
    """Extract custom metadata tags embedded by video-subclip via ffmpeg -metadata.

    Returns a dict of tag key→value pairs. Only returns tags that are custom
    (parent_asset_id, parent_s3_path, subclip_index, subclip_start_seconds).
    Returns empty dict on any failure.
    """
    try:
        cmd = [
            FFPROBE,
            "-v", "quiet",
            "-print_format", "json",
            "-show_entries", "format_tags",
            video_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return {}

        data = json.loads(result.stdout)
        tags = data.get("format", {}).get("tags", {})

        # Only return the custom tags we care about
        custom_keys = {
            "parent_asset_id", "parent_s3_path",
            "subclip_index", "subclip_start_seconds",
        }
        return {k: v for k, v in tags.items() if k in custom_keys}

    except Exception:
        return {}
