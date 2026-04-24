"""Foundation: Audio fingerprinting + language detection (UC05 audio + UC18).

Extracts a 30-second audio segment ONCE, then runs:
  1. Chromaprint audio fingerprinting + AcoustID lookup (UC05 music)
  2. SpeechBrain ECAPA-TDNN language detection (UC18)
  3. Subtitle track extraction via ffprobe (UC18)

Single audio extraction replaces duplicate work in UC05 and UC18.

Writes: music_detected, audio_fingerprint, talent_music_scanned_at,
        detected_language, language_confidence, subtitle_tracks,
        localization_detected_at
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
from video_analyzer import extract_audio_segment, extract_metadata
from schemas import ASSETS_SCHEMA, TALENT_MUSIC_SCHEMA


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

    ctx.logger.info("audio-analyzer initialized")


def handler(ctx, event):
    """Analyze audio: fingerprint + language detection + subtitle extraction."""
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
            ("talent_music", TALENT_MUSIC_SCHEMA),
        ], logger=ctx.logger)
        ctx.user_data["_tables_ready"] = True
        log("Table setup complete")

    # ── Step 1: Parse event ──
    log(f"[1/5] Event received — type: {type(event).__name__}")

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
    log("[2/5] Downloading from S3 to temp file...")
    local_path = s3.download_to_temp(s3_path)
    log(f"       Downloaded to {local_path}")

    audio_path = None
    try:
        # ── Step 3: Extract metadata and check for audio ──
        log("[3/5] Extracting metadata...")
        meta = extract_metadata(local_path)
        has_audio = bool(meta.get("audio_codec"))
        log(f"       has_audio={has_audio}")

        # ── Audio fingerprint + language detection ──
        detections = []
        detected_language = "unknown"
        language_confidence = 0.0

        if has_audio:
            log("[4/5] Extracting 30s audio segment...")
            audio_path = extract_audio_segment(local_path, duration_seconds=30)

            if audio_path and os.path.exists(audio_path) and os.path.getsize(audio_path) > 1000:
                # 1. Chromaprint fingerprint + AcoustID
                detections = _fingerprint_audio(ctx, audio_path)
                log(f"       Audio detections: {len(detections)}")

                # 2. SpeechBrain language detection
                detected_language, language_confidence = _detect_language(ctx, audio_path)
                log(f"       Language: {detected_language} ({language_confidence:.3f})")
            else:
                log("       Audio segment too small, skipping")
        else:
            log("[4/5] No audio track found, skipping audio analysis")

        # ── Subtitle tracks (from video container, not audio) ──
        subtitle_tracks = _extract_subtitle_tracks(ctx, local_path)
        log(f"       Subtitle tracks: {len(subtitle_tracks)}")

        # ── Step 5: Write results ──
        log("[5/5] Writing results...")
        now = time.time()

        # Write talent_music rows
        rows = []
        for det in detections:
            rows.append({
                "detection_id": uuid.uuid4().hex,
                "asset_id": asset_id,
                "s3_path": s3_path,
                "detection_type": det["detection_type"],
                "label": det["label"],
                "confidence": det["confidence"],
                "start_time_sec": det["start_time_sec"],
                "end_time_sec": det["end_time_sec"],
                "detected_at": now,
            })

        if rows:
            vast.write_rows("talent_music", TALENT_MUSIC_SCHEMA, rows)
            log(f"       Wrote {len(rows)} talent_music rows")

        # Upsert assets: UC05 audio columns + UC18 localization columns
        music_detected = any(
            d["detection_type"] in ("audio-fingerprint", "music-match")
            for d in detections
        )
        audio_fp = ""
        for d in detections:
            if d["detection_type"] == "audio-fingerprint":
                audio_fp = d["label"]
                break

        vast.upsert_asset(asset_id, {
            "s3_path": s3_path,
            # UC05 audio columns
            "music_detected": music_detected,
            "audio_fingerprint": audio_fp,
            "talent_music_scanned_at": now,
            # UC18 localization columns
            "detected_language": detected_language,
            "language_confidence": language_confidence,
            "subtitle_tracks": json.dumps(subtitle_tracks),
            "localization_detected_at": now,
        })
        log("       Upsert complete")

        log(f"Done. asset_id={asset_id} audio_dets={len(detections)} lang={detected_language}")
        return json.dumps({"asset_id": asset_id, "status": "ok"})

    finally:
        os.unlink(local_path)
        if audio_path and os.path.exists(audio_path):
            os.unlink(audio_path)
        log("       Cleaned up temp files")


# ── Chromaprint / AcoustID ──────────────────────────────────────────────────

def _fingerprint_audio(ctx, audio_path):
    """Compute audio fingerprint and attempt AcoustID music lookup."""
    detections = []
    try:
        import acoustid
    except ImportError:
        ctx.logger.warning("pyacoustid not installed, skipping audio fingerprint")
        return detections

    try:
        duration_result, fingerprint = acoustid.fingerprint_file(audio_path)

        if fingerprint:
            fp_short = fingerprint[:64] if len(fingerprint) > 64 else fingerprint
            detections.append({
                "detection_type": "audio-fingerprint",
                "label": f"chromaprint-{fp_short}",
                "confidence": 1.0,
                "start_time_sec": 0.0,
                "end_time_sec": float(duration_result) if duration_result else 30.0,
            })

            # AcoustID lookup for music identification
            try:
                results = acoustid.match(
                    os.environ.get("ACOUSTID_API_KEY", ""),
                    audio_path,
                )
                for score, recording_id, title, artist in results:
                    if title or artist:
                        label = f"{artist or 'Unknown'} - {title or 'Unknown'}"
                        detections.append({
                            "detection_type": "music-match",
                            "label": label,
                            "confidence": round(float(score), 4),
                            "start_time_sec": 0.0,
                            "end_time_sec": float(duration_result) if duration_result else 30.0,
                        })
            except Exception as e:
                ctx.logger.info(f"AcoustID lookup skipped: {e}")

    except Exception as e:
        ctx.logger.warning(f"Audio fingerprint error: {e}")

    return detections


# ── SpeechBrain language detection ──────────────────────────────────────────

def _detect_language(ctx, audio_path):
    """Detect spoken language using SpeechBrain ECAPA-TDNN."""
    try:
        from speechbrain.inference.classifiers import EncoderClassifier

        lang_model = EncoderClassifier.from_hparams(
            source="speechbrain/lang-id-voxlingua107-ecapa",
            savedir="/tmp/speechbrain_lang_id",
        )
        prediction = lang_model.classify_file(audio_path)
        score = float(prediction[1].squeeze())
        language = str(prediction[3][0]) if prediction[3] else "unknown"
        return language, score

    except Exception as e:
        ctx.logger.warning(f"SpeechBrain language detection failed: {e}")
        return "unknown", 0.0


# ── Subtitle track extraction ──────────────────────────────────────────────

def _extract_subtitle_tracks(ctx, video_path):
    """Extract subtitle track info from video container via ffprobe."""
    _bin_dir = os.path.dirname(os.path.abspath(__file__))
    ffprobe_bin = os.path.join(_bin_dir, "ffprobe")
    cmd = [
        ffprobe_bin, "-v", "quiet", "-print_format", "json",
        "-show_streams", "-select_streams", "s", video_path,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            return []
        data = json.loads(result.stdout)
        tracks = []
        for stream in data.get("streams", []):
            tags = stream.get("tags", {})
            tracks.append({
                "index": stream.get("index", -1),
                "language": tags.get("language", "unknown"),
                "title": tags.get("title", ""),
                "codec": stream.get("codec_name", "unknown"),
            })
        return tracks
    except Exception as e:
        ctx.logger.warning(f"Subtitle track extraction failed: {e}")
        return []
