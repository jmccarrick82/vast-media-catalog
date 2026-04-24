"""Foundation: Extract video metadata via ffprobe and upsert to unified assets table.

Also extracts embedded custom metadata tags (e.g. parent_asset_id) from subclip
MP4 files, reinforcing the parent→subclip linkage established by video-subclip.
"""

import hashlib
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from shared.config_loader import load_config
from shared.s3_client import S3Client
from shared.vast_client import VastDBClient
from shared.video_analyzer import extract_metadata


def init(ctx):
    """One-time initialization: load config, create DB and S3 clients."""
    config = load_config()
    ctx.user_data = {
        "config": config,
        "s3": S3Client(config),
        "vast": VastDBClient(config),
    }
    ctx.logger.info("metadata-extractor initialized")


def handler(ctx, event):
    """Process a new video file: extract metadata via ffprobe and upsert to unified assets table."""
    s3_path = event.body.decode("utf-8") if isinstance(event.body, bytes) else str(event.body)
    s3_path = s3_path.strip()
    ctx.logger.info(f"Processing: {s3_path}")

    s3 = ctx.user_data["s3"]
    vast = ctx.user_data["vast"]

    s3_meta = s3.get_object_metadata(s3_path)
    filename = os.path.basename(s3_path)
    asset_id = hashlib.md5(s3_path.encode()).hexdigest()

    local_path = s3.download_to_temp(s3_path)
    try:
        meta = extract_metadata(local_path)

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
            "ingested_at": time.time(),
        }

        # ── Extract embedded subclip metadata tags ──
        # video-subclip embeds parent_asset_id, parent_s3_path, subclip_index,
        # and subclip_start_seconds as ffmpeg -metadata tags. If present, this
        # reinforces the parent→subclip linkage from a second source.
        custom_tags = _extract_custom_tags(local_path)
        if custom_tags.get("parent_asset_id"):
            ctx.logger.info(
                f"Found subclip tags: parent_asset_id={custom_tags['parent_asset_id']}"
            )
            fields["is_subclip"] = True
            fields["subclip_parent_asset_id"] = custom_tags["parent_asset_id"]
            if custom_tags.get("parent_s3_path"):
                fields["subclip_parent_s3_path"] = custom_tags["parent_s3_path"]
            if custom_tags.get("subclip_index") is not None:
                try:
                    fields["subclip_index"] = int(custom_tags["subclip_index"])
                except (ValueError, TypeError):
                    pass
            if custom_tags.get("subclip_start_seconds") is not None:
                try:
                    fields["subclip_start_seconds"] = float(
                        custom_tags["subclip_start_seconds"]
                    )
                except (ValueError, TypeError):
                    pass

        vast.upsert_asset(asset_id, fields)

        ctx.logger.info(f"Done. asset_id={asset_id}")
        return json.dumps({"asset_id": asset_id, "status": "ok"})

    finally:
        os.unlink(local_path)


def _extract_custom_tags(video_path: str) -> dict:
    """Extract custom metadata tags embedded by video-subclip via ffmpeg -metadata.

    Returns a dict of tag key→value pairs. Only returns tags that are custom
    (parent_asset_id, parent_s3_path, subclip_index, subclip_start_seconds).
    Returns empty dict on any failure.
    """
    try:
        cmd = [
            "ffprobe",
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
