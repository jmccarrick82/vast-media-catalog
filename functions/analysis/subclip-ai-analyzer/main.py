"""Analysis: AI-powered content analysis for subclips via inference endpoint.

Triggers on S3 PUT events in the james-media-subclips bucket. For each subclip:
1. Download the subclip video
2. Extract one keyframe (JPEG) and 30s audio (WAV)
3. Call inference models sequentially:
   - Whisper transcription (audio → text)
   - Vision OCR (keyframe → visible text)
   - Scene description + tags (keyframe → description)
   - AI content detection (keyframe → real vs AI-generated)
   - Content safety (keyframe → safe/unsafe)
   - Content summarization (all results → summary + metadata)
4. Upsert all human-readable results to the assets table

Writes: transcript, ocr_text, scene_description, content_tags,
        ai_content_assessment, ai_probability_vision, content_safety_rating,
        content_summary, content_category, content_mood, content_rating,
        searchable_keywords, ai_analyzed_at
"""

import base64
import hashlib
import http.client
import json
import os
import random
import ssl
import subprocess
import tempfile
import time
import traceback
from urllib.parse import urlparse

from config_loader import load_config
from s3_client import S3Client
from vast_client import VastDBClient
from schemas import ASSETS_SCHEMA

# ── Inference endpoint config ──
# Read from config.json (`inference.host` / `inference.api_key`) with env
# var fallbacks. Keeps credentials out of source — see README for setup.
def _load_inference_config():
    try:
        cfg = load_config() or {}
    except Exception:
        cfg = {}
    inf = (cfg.get("inference") or {}) if isinstance(cfg, dict) else {}
    host = os.environ.get("INFERENCE_HOST") or inf.get("host") or "inference.example.com"
    key  = os.environ.get("INFERENCE_KEY")  or inf.get("api_key") or ""
    return host, key

INFERENCE_HOST, INFERENCE_KEY = _load_inference_config()

# ── Models ──
MODEL_WHISPER = "local-mlx/whisper-turbo"
MODEL_VISION_90B = "nvidia/llama-3.2-90b-vision-instruct"
MODEL_VISION_11B = "nvidia/llama-3.2-11b-vision-instruct"
MODEL_SAFETY = "nvidia/llama-guard-4-12b"
# Swapped from llama-3.1-405b-instruct — a 70B model is plenty for
# fusing multi-frame descriptions into a summary + picking category/mood,
# and runs several times faster.
MODEL_SUMMARY = "nvidia/llama-3.3-70b-instruct"

# Number of keyframes to extract per subclip. The 90B vision model is
# capped at 1 image per call on this endpoint, so each frame gets its
# own describe call; the 70B fuses them in step 8.
FRAMES_PER_CLIP = 4
# Upgraded from llama-3.2-nv-embedqa-1b-v2 (1B / 2048-dim). The old model
# had no "modality/speaker" axis — meta-queries like "person talking"
# returned noise-level scores. nv-embed-v1 is NVIDIA's flagship
# general-purpose embedder (top of MTEB), much richer concept space.
MODEL_EMBED = "nvidia/nv-embed-v1"
EMBED_DIM = 4096
MAX_EMBED_CHARS = 6000

# ── Retry policy for every inference call ──
# Exponential-ish backoff with jitter. Covers both rate-limit spikes
# (429s when the shared endpoint is slammed by parallel subclips) and
# transient 5xx / connection resets. Total retry budget ≈ 230s, which
# fits comfortably under the 600s function timeout.
RETRY_BACKOFFS = [5, 15, 30, 60, 120]   # seconds between attempts; len+1 = max_attempts
RETRY_JITTER_SECONDS = 5

# ── Inference pacing ──
# Delay between sequential inference calls to avoid hammering the endpoint
# when multiple subclips are being processed in parallel.
INFERENCE_STEP_DELAY_SECONDS = 2

_BIN_DIR = os.path.dirname(os.path.abspath(__file__))
FFPROBE = os.path.join(_BIN_DIR, "ffprobe")
FFMPEG = os.path.join(_BIN_DIR, "ffmpeg")


def init(ctx):
    config = load_config()
    s3 = S3Client(config)
    vast = VastDBClient(config)

    qdrant_cfg = config.get("qdrant") or {}
    qdrant_url = qdrant_cfg.get("url") or ""
    qdrant_collection = qdrant_cfg.get("collection", "subclips")

    ctx.user_data = {
        "config": config,
        "s3": s3,
        "vast": vast,
        "_tables_ready": False,
        "qdrant_url": qdrant_url,
        "qdrant_collection": qdrant_collection,
        "_qdrant_ready": False,
    }

    # Try to ensure the Qdrant collection exists so the first handler call
    # doesn't pay the setup cost. Non-fatal — we retry lazily on first use.
    if qdrant_url:
        try:
            _qdrant_ensure_collection(qdrant_url, qdrant_collection, ctx.logger)
            ctx.user_data["_qdrant_ready"] = True
        except Exception as e:
            ctx.logger.info(f"WARNING: Qdrant setup failed ({qdrant_url}): {e}")
            ctx.logger.info("Will retry on first handler call")
    else:
        ctx.logger.info("No qdrant.url in config — text embedding/indexing disabled")

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

    ctx.logger.info("subclip-ai-analyzer initialized")


def handler(ctx, event):
    log = ctx.logger.info
    try:
        return _handle(ctx, event)
    except Exception as e:
        log(f"HANDLER ERROR: {type(e).__name__}: {e}")
        log(f"TRACEBACK: {traceback.format_exc()}")
        raise


def _handle(ctx, event):
    log = ctx.logger.info
    vast = ctx.user_data["vast"]
    t_handler_start = time.monotonic()

    # ── Lazy table setup ──
    if not ctx.user_data["_tables_ready"]:
        log("Setting up VAST DB tables (first handler call)...")
        vast.setup_tables([("assets", ASSETS_SCHEMA)], logger=ctx.logger)
        ctx.user_data["_tables_ready"] = True
        log("Table setup complete")

    # ── Step 1: Parse event ──
    log(f"[1/10] Event received — type: {type(event).__name__}")

    if hasattr(event, "object_key") and hasattr(event, "bucket"):
        bucket_name = str(event.bucket)
        object_key = str(event.object_key)
        s3_path = f"s3://{bucket_name}/{object_key}"
        log(f"       bucket={bucket_name}  key={object_key}")
    elif hasattr(event, "body"):
        s3_path = event.body.decode("utf-8") if isinstance(event.body, bytes) else str(event.body)
        s3_path = s3_path.strip()
    else:
        raise RuntimeError(f"Cannot extract s3_path from {type(event).__name__}")
    log(f"       s3_path={s3_path}")

    s3 = ctx.user_data["s3"]
    asset_id = hashlib.md5(s3_path.encode()).hexdigest()
    filename = os.path.basename(s3_path)
    log(f"       asset_id={asset_id}  filename={filename}")

    # ── Skip hidden paths ──
    try:
        _, key = S3Client.parse_s3_path(s3_path)
        parts = key.split("/")
        if any(part.startswith(".") for part in parts if part):
            log(f"       Skipping hidden-path file: {s3_path}")
            return json.dumps({"asset_id": asset_id, "status": "skipped_hidden_path"})
    except ValueError:
        pass

    # ── Step 2: Download subclip ──
    log("[2/9] Downloading subclip from S3...")
    local_path = s3.download_to_temp(s3_path)
    log(f"       Downloaded to {local_path}")

    # Pre-declare so the `finally` can clean up even on early exception
    keyframes: list[str] = []
    audio_path: str | None = None

    try:
        # ── Step 3: Extract 4 keyframes + audio ──
        # Pull 4 frames evenly across the clip (10%, 37%, 63%, 90% of duration)
        # so the vision model gets temporal context instead of one frozen
        # moment. The 90B vision model is capped at 1 image per call on this
        # endpoint, so we describe each frame in its own call (step 6), then
        # fuse into a video-level description in step 8.
        log("[3/9] Extracting 4 keyframes and audio...")
        keyframes, frame_times, duration_s = _extract_keyframes(local_path, n=FRAMES_PER_CLIP)
        audio_path = _extract_audio(local_path)
        log(f"       frames={len(keyframes)} at {[round(t,1) for t in frame_times]}s  duration={duration_s:.1f}s")

        results = {"_duration_s": duration_s, "_frame_times": frame_times}

        # Each step is wrapped in _call_with_retry_and_timing so that
        #   (a) we get per-call elapsed in the logs (easy to analyze which
        #       model is the latency bottleneck — usually steps 6 or 8),
        #   (b) transient 429/5xx errors against the shared inference
        #       endpoint are retried with exponential backoff instead of
        #       silently dropping a subclip's analysis.
        #
        # The outer try/except still catches "all retries exhausted" so
        # one broken step doesn't kill the whole subclip — we persist
        # whatever we got so far.

        # ── Step 4: Whisper transcription (full 30s audio) ──
        log("[4/9] Running Whisper transcription...")
        try:
            transcript = _call_with_retry_and_timing(
                "whisper", log, _call_whisper, audio_path,
            )
            results["transcript"] = transcript
            log(f"       Transcript: {transcript[:100]}..." if len(transcript) > 100 else f"       Transcript: {transcript}")
        except Exception as e:
            log(f"       Whisper failed: {e}")
            results["transcript"] = None

        time.sleep(INFERENCE_STEP_DELAY_SECONDS)

        # ── Step 5: Vision OCR (middle frame only — OCR doesn't vary much over 30s) ──
        # Use the middle frame as a representative sample for OCR — cheap.
        middle_frame = keyframes[len(keyframes) // 2]
        log("[5/9] Running vision OCR on middle frame...")
        try:
            ocr_text = _call_with_retry_and_timing(
                "vision-90b-ocr", log, _call_vision_ocr, middle_frame,
            )
            results["ocr_text"] = ocr_text
            log(f"       OCR: {ocr_text[:100]}..." if len(ocr_text) > 100 else f"       OCR: {ocr_text}")
        except Exception as e:
            log(f"       Vision OCR failed: {e}")
            results["ocr_text"] = None

        time.sleep(INFERENCE_STEP_DELAY_SECONDS)

        # ── Step 6: Per-frame scene description (4 calls, one per frame) ──
        log(f"[6/9] Running scene description on {len(keyframes)} frames...")
        frame_descs = []
        for i, (fp, t) in enumerate(zip(keyframes, frame_times)):
            try:
                desc = _call_with_retry_and_timing(
                    f"vision-90b-frame{i+1}", log,
                    _call_describe_frame, fp, i, len(keyframes), duration_s, t,
                )
                frame_descs.append(desc)
                log(f"       Frame {i+1} @ {t:.1f}s: {desc[:80]}...")
            except Exception as e:
                log(f"       Frame {i+1} description failed: {e}")
                frame_descs.append("")
            time.sleep(INFERENCE_STEP_DELAY_SECONDS)
        results["_frame_descriptions"] = frame_descs

        # ── Step 7: Content safety (middle frame, single call) ──
        log("[7/9] Running content safety check...")
        try:
            safety = _call_with_retry_and_timing(
                "llama-guard", log, _call_content_safety, middle_frame,
            )
            results["content_safety_rating"] = safety
            log(f"       Safety: {safety}")
        except Exception as e:
            log(f"       Content safety failed: {e}")
            results["content_safety_rating"] = None

        time.sleep(INFERENCE_STEP_DELAY_SECONDS)

        # ── Step 8: Fuse frame descriptions + transcript + OCR into a VIDEO summary ──
        # The 70B sees all 4 frame descriptions, the full transcript, and the
        # OCR text, and is asked to reason ACROSS the frames (motion, temporal
        # progression, narrative) rather than describe a still. Returns a JSON
        # object with the video-level summary plus a fused scene_description.
        #
        # Fallback behavior: if the 70B retries exhaust OR returns empty for
        # any field, we degrade gracefully to the raw per-frame vision
        # descriptions (and then to the transcript) so the row is never
        # left with None — that way `content_summary`/`scene_description`
        # always have SOMETHING for embeddings and UI to work with.
        log(f"[8/9] Fusing frame descriptions into video summary ({MODEL_SUMMARY})...")
        summary: dict = {}
        try:
            summary = _call_with_retry_and_timing(
                "summary-70b", log, _call_summarization, results, filename,
            )
        except Exception as e:
            log(f"       Summarization call FAILED, will use frame-desc fallback: {e}")

        # Build the frame-level fallback once; reused for summary + scene
        _frame_times = results.get("_frame_times") or []
        _frame_descs = results.get("_frame_descriptions") or []
        frame_fallback = "\n".join(
            f"Frame {i+1} at {t:.1f}s: {d}"
            for i, (t, d) in enumerate(zip(_frame_times, _frame_descs))
            if (d or "").strip()
        )
        transcript_snip = ((results.get("transcript") or "").strip())[:500]

        raw_summary = (summary.get("summary") or "").strip() if isinstance(summary, dict) else ""
        raw_scene = (summary.get("scene_description") or "").strip() if isinstance(summary, dict) else ""

        results["content_summary"] = (
            raw_summary
            or frame_fallback   # if fuser gave nothing, use the 4 frame descriptions
            or transcript_snip  # final resort: what Whisper heard
            or None             # truly nothing extracted — keep None
        )
        results["scene_description"] = (
            raw_scene
            or frame_fallback
            or None
        )
        results["content_category"] = (summary.get("category") or "").strip() or "Uncategorized"
        results["content_mood"] = (summary.get("mood") or "").strip() or "Unknown"
        results["content_rating"] = (summary.get("content_rating") or "").strip() or "G"
        kws = summary.get("searchable_keywords") or []
        if isinstance(kws, list):
            results["searchable_keywords"] = json.dumps(kws)
        else:
            results["searchable_keywords"] = str(kws)

        if results["content_summary"]:
            log(f"       Summary: {results['content_summary'][:100]}...")
        else:
            log("       Summary: (empty — no vision/transcript data available)")

        # ── Write to VAST DB ──
        # Flag this row as a subclip so /api/assets hides it by default.
        # upsert_asset() creates a second row for this asset_id (vastdb
        # doesn't update in-place), so _dedup_asset_rows() on the read path
        # merges the video-subclip row and this ai-analyzer row. For the
        # merge to preserve the subclip flag we must also set it here,
        # otherwise the Trino WHERE filter (which runs before Python dedup)
        # sees this row as a parent asset and lets it through.
        fields = {
            "s3_path": s3_path,
            "filename": filename,
            "ai_analyzed_at": time.time(),
            "is_subclip": True,
        }

        # Derive parent linkage from the subclip's s3_path:
        #   s3://james-media-subclips/<stem>/subclip_NNN.mp4
        # → parent s3: s3://james-media-catalog/<stem>.mp4 (asset_id = md5(s3))
        #
        # video-subclip normally writes these fields when it creates the
        # subclip, but if its row was wiped or the subclip was re-uploaded
        # directly to the subclips bucket, this derivation covers us so
        # the UI can still group the subclip under its parent asset.
        try:
            parts = s3_path.split("/")
            if len(parts) >= 5 and parts[2] == "james-media-subclips":
                parent_stem = parts[3]
                parent_s3 = f"s3://james-media-catalog/{parent_stem}.mp4"
                fields.setdefault(
                    "subclip_parent_asset_id",
                    hashlib.md5(parent_s3.encode()).hexdigest(),
                )
                fields.setdefault("subclip_parent_s3_path", parent_s3)
        except Exception as e:
            log(f"       WARN: parent linkage derivation failed: {e}")

        # Derive subclip_index from filename pattern "subclip_NNN.mp4"
        try:
            stem = os.path.splitext(filename)[0]  # "subclip_001"
            if stem.startswith("subclip_"):
                fields["subclip_index"] = int(stem.split("_", 1)[1]) - 1
        except (ValueError, IndexError):
            pass

        # Copy results into fields, skipping internal keys (leading underscore)
        # and any columns no longer in the schema (e.g. ai_content_assessment).
        for k, v in results.items():
            if k.startswith("_"):
                continue
            if v is not None:
                fields[k] = v

        # ── Step 9: Embed text + index in Qdrant ──
        # We already have every piece of text for this subclip in `results`
        # (transcript, ocr_text, scene_description, content_summary, etc.)
        # so embed it right here instead of standing up a separate polling
        # worker. The webapp's /api/semantic-search queries this collection.
        qdrant_url = ctx.user_data.get("qdrant_url") or ""
        if qdrant_url:
            try:
                log("[9/9] Building passage and embedding for Qdrant...")
                passage = _build_passage(results)
                if not passage:
                    log("       No text produced by upstream steps — skipping embed")
                else:
                    # Lazy-setup the collection if init() couldn't
                    if not ctx.user_data.get("_qdrant_ready"):
                        _qdrant_ensure_collection(
                            qdrant_url,
                            ctx.user_data["qdrant_collection"],
                            ctx.logger,
                        )
                        ctx.user_data["_qdrant_ready"] = True

                    vec = _call_with_retry_and_timing(
                        "embed-nv-embed-v1", log, _call_embeddings, passage, "passage",
                    )
                    log(f"       Embedded passage ({len(passage)} chars → {len(vec)}-dim)")

                    # Payload carries everything the search UI needs to render
                    # a hit and link back to playable clip metadata.
                    payload = {
                        "asset_id": asset_id,
                        "s3_path": s3_path,
                        "filename": filename,
                        "content_summary": results.get("content_summary"),
                        "content_category": results.get("content_category"),
                        "content_mood": results.get("content_mood"),
                        "content_rating": results.get("content_rating"),
                        "searchable_keywords": results.get("searchable_keywords"),
                        "ai_analyzed_at": fields.get("ai_analyzed_at"),
                        "subclip_index": fields.get("subclip_index"),
                        "embedded_text": passage[:4000],
                        "model_name": MODEL_EMBED,
                    }

                    _call_with_retry_and_timing(
                        "qdrant-upsert", log, _qdrant_upsert_point,
                        qdrant_url,
                        ctx.user_data["qdrant_collection"],
                        _point_id(asset_id),
                        vec,
                        payload,
                    )
                    fields["text_embedding_created_at"] = time.time()
                    fields["text_embedding_model"] = MODEL_EMBED
                    log("       Qdrant upsert OK")
            except Exception as e:
                log(f"       Embedding/indexing failed (non-fatal): {e}")

        log(f"Writing {len(fields)} fields to assets table for {asset_id}...")
        t_vast = time.monotonic()
        vast.upsert_asset(asset_id, fields)
        log(f"       [timing] vast-upsert OK in {time.monotonic() - t_vast:.2f}s")

        total_handler = time.monotonic() - t_handler_start
        log(f"Done. AI analysis complete for {filename} — total handler time: {total_handler:.2f}s")

        return json.dumps({"asset_id": asset_id, "status": "ok", "fields_written": len(fields)})

    finally:
        if os.path.exists(local_path):
            os.unlink(local_path)
        if audio_path and os.path.exists(audio_path):
            os.unlink(audio_path)
        for p in keyframes or []:
            if p and os.path.exists(p):
                os.unlink(p)


# ---------------------------------------------------------------------------
# Media extraction helpers
# ---------------------------------------------------------------------------

def _probe_duration(video_path: str) -> float:
    probe = subprocess.run(
        [FFPROBE, "-v", "quiet", "-print_format", "json", "-show_format", video_path],
        capture_output=True, text=True, timeout=30,
    )
    if probe.returncode != 0:
        return 0.0
    try:
        return float(json.loads(probe.stdout).get("format", {}).get("duration", 0))
    except (ValueError, json.JSONDecodeError):
        return 0.0


def _extract_keyframes(video_path: str, n: int = FRAMES_PER_CLIP) -> tuple[list[str], list[float], float]:
    """Extract n evenly-spaced JPEG keyframes across a video clip.

    Samples from 10% to 90% of the clip so we skip likely-black first/last
    frames. Returns (frame_paths, frame_times_seconds, duration_seconds).

    Falls back to the slower ffmpeg arg order (`-i` before `-ss`) if the
    fast order produces a zero-byte JPEG — short clips can fail fast-seek.
    """
    duration = _probe_duration(video_path)
    if duration <= 0:
        # Unknown duration — best effort: one frame at t=1s
        duration = 1.0
        times = [1.0]
    elif n == 1:
        times = [duration / 2]
    else:
        times = [duration * (0.10 + (0.80 * i / (n - 1))) for i in range(n)]

    frames: list[str] = []
    for i, t in enumerate(times):
        out = tempfile.NamedTemporaryFile(suffix=f"_f{i}.jpg", delete=False).name
        # Fast seek: -ss before -i (reuses keyframe index)
        subprocess.run(
            [FFMPEG, "-y", "-loglevel", "error", "-ss", str(t), "-i", video_path,
             "-frames:v", "1", "-q:v", "3", out],
            capture_output=True, timeout=30,
        )
        if (not os.path.exists(out)) or os.path.getsize(out) == 0:
            # Precise seek: -i before -ss
            subprocess.run(
                [FFMPEG, "-y", "-loglevel", "error", "-i", video_path, "-ss", str(t),
                 "-frames:v", "1", "-q:v", "3", out],
                capture_output=True, timeout=30,
            )
        frames.append(out)
    return frames, times, duration


def _extract_audio(video_path: str) -> str:
    """Extract audio as 16kHz mono WAV for Whisper."""
    out_path = tempfile.NamedTemporaryFile(suffix=".wav", delete=False).name
    subprocess.run(
        [FFMPEG, "-y", "-i", video_path, "-vn",
         "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1", out_path],
        capture_output=True, timeout=30,
    )
    return out_path


# ---------------------------------------------------------------------------
# Inference endpoint helpers
# ---------------------------------------------------------------------------

def _call_with_retry_and_timing(label: str, log, fn, *args, **kwargs):
    """Run fn(*args, **kwargs) with retry + per-attempt timing logs.

    Retries on any exception — most failures against the shared inference
    endpoint are transient (429 rate limit, 5xx, connection resets).
    Backoff schedule: RETRY_BACKOFFS seconds between attempts, with a
    small random jitter so parallel subclip containers don't retry in
    lockstep and hammer the endpoint again at the same moment.

    Each attempt logs:
      [timing] <label> attempt N/M OK in Xs (total Ys)
    or on failure:
      [timing] <label> attempt N/M FAIL in Xs (ErrType: …); backoff Zs

    The caller's handler continues to catch exceptions around this so a
    single step that exhausts retries doesn't kill the whole function —
    we'd rather persist partial results than lose everything.
    """
    max_attempts = len(RETRY_BACKOFFS) + 1
    t_start = time.monotonic()
    last_err = None

    for attempt in range(1, max_attempts + 1):
        t_attempt = time.monotonic()
        try:
            result = fn(*args, **kwargs)
            dt = time.monotonic() - t_attempt
            total = time.monotonic() - t_start
            log(f"       [timing] {label} OK in {dt:.2f}s "
                f"(total {total:.2f}s, attempt {attempt}/{max_attempts})")
            return result
        except Exception as e:
            last_err = e
            dt = time.monotonic() - t_attempt
            err_type = type(e).__name__
            err_msg = str(e)[:200]
            if attempt < max_attempts:
                backoff = RETRY_BACKOFFS[attempt - 1] + random.uniform(0, RETRY_JITTER_SECONDS)
                log(f"       [timing] {label} attempt {attempt}/{max_attempts} "
                    f"FAIL in {dt:.2f}s ({err_type}: {err_msg}); backoff {backoff:.1f}s")
                time.sleep(backoff)
            else:
                total = time.monotonic() - t_start
                log(f"       [timing] {label} attempt {attempt}/{max_attempts} "
                    f"FAIL in {dt:.2f}s (final, total {total:.2f}s): {err_type}: {err_msg}")
                raise RuntimeError(
                    f"{label} failed after {max_attempts} attempts: {last_err}"
                ) from last_err


def _inference_chat(model: str, messages: list, max_tokens: int = 500,
                    temperature: float = 0.2, timeout: int = 90) -> str:
    """Single-shot POST to /v1/chat/completions. Retries are handled by
    _call_with_retry_and_timing at the call site."""
    body = json.dumps({
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }).encode()

    conn = http.client.HTTPSConnection(INFERENCE_HOST, timeout=timeout)
    try:
        conn.request("POST", "/v1/chat/completions", body=body, headers={
            "Authorization": f"Bearer {INFERENCE_KEY}",
            "Content-Type": "application/json",
        })
        resp = conn.getresponse()
        raw = resp.read().decode()
        if resp.status != 200:
            raise RuntimeError(f"Inference {resp.status}: {raw[:500]}")
        return json.loads(raw)["choices"][0]["message"]["content"]
    finally:
        conn.close()


def _image_b64(path: str) -> str:
    """Read an image file and return base64-encoded data URL."""
    with open(path, "rb") as f:
        return f"data:image/jpeg;base64,{base64.b64encode(f.read()).decode()}"


def _call_whisper(audio_path: str) -> str:
    """Transcribe audio via Whisper endpoint."""
    boundary = "----WhisperBoundary"
    audio_data = open(audio_path, "rb").read()

    body = b""
    body += f"--{boundary}\r\n".encode()
    body += b"Content-Disposition: form-data; name=\"model\"\r\n\r\n"
    body += MODEL_WHISPER.encode() + b"\r\n"
    body += f"--{boundary}\r\n".encode()
    body += b"Content-Disposition: form-data; name=\"file\"; filename=\"audio.wav\"\r\n"
    body += b"Content-Type: audio/wav\r\n\r\n"
    body += audio_data
    body += b"\r\n"
    body += f"--{boundary}--\r\n".encode()

    conn = http.client.HTTPSConnection(INFERENCE_HOST, timeout=120)
    conn.request("POST", "/v1/audio/transcriptions", body=body, headers={
        "Authorization": f"Bearer {INFERENCE_KEY}",
        "Content-Type": f"multipart/form-data; boundary={boundary}",
    })
    resp = conn.getresponse()
    data = json.loads(resp.read().decode())
    conn.close()

    if resp.status != 200:
        raise RuntimeError(f"Whisper error {resp.status}: {data}")

    return (data.get("text") or "").strip()


def _call_vision_ocr(keyframe_path: str) -> str:
    """Extract visible text from keyframe using vision model."""
    img = _image_b64(keyframe_path)
    text = _inference_chat(MODEL_VISION_90B, [{
        "role": "user",
        "content": [
            {"type": "image_url", "image_url": {"url": img}},
            {"type": "text", "text": (
                "Extract ALL visible text from this image. Include any: "
                "headlines, lower thirds, chyrons, logos, watermarks, channel names, "
                "captions, subtitles. Return ONLY the extracted text, one item per line. "
                "If no text is visible, return exactly: NO_TEXT_DETECTED"
            )},
        ],
    }], max_tokens=300, temperature=0.1)
    return text.strip() if text.strip() != "NO_TEXT_DETECTED" else ""


def _call_describe_frame(frame_path: str, idx: int, total: int,
                         duration_s: float, time_s: float) -> str:
    """Describe a single frame given its position in the clip.

    Called once per frame (4× per subclip). The temporal context
    ("frame 3 of 4 at t=18.7s of a 30s clip") lets the 70B fuser in
    step 8 reason about motion and progression across the 4 outputs.
    """
    img = _image_b64(frame_path)
    prompt = (
        f"This is frame {idx+1} of {total} sampled from a {duration_s:.0f}-second "
        f"video clip, taken at the {time_s:.1f}-second mark. "
        "In 1-2 sentences, describe what's happening in THIS specific frame: "
        "subjects, action, setting, notable objects. Be concrete and specific."
    )
    text = _inference_chat(MODEL_VISION_90B, [{
        "role": "user",
        "content": [
            {"type": "image_url", "image_url": {"url": img}},
            {"type": "text", "text": prompt},
        ],
    }], max_tokens=180, temperature=0.2)
    return text.strip()


def _call_content_safety(keyframe_path: str) -> str:
    """Check content safety using LlamaGuard."""
    img = _image_b64(keyframe_path)
    text = _inference_chat(MODEL_SAFETY, [{
        "role": "user",
        "content": [
            {"type": "image_url", "image_url": {"url": img}},
            {"type": "text", "text": "Classify this image."},
        ],
    }], max_tokens=100, temperature=0.1)
    return text.strip()


def _call_summarization(results: dict, filename: str) -> dict:
    """Fuse the 4 per-frame descriptions + audio transcript + OCR into a
    video-level summary. This is what the 70B is good at and what single-
    frame scene descriptions can't produce — it reasons across time.
    """
    duration_s = results.get("_duration_s", 0) or 0
    frame_times = results.get("_frame_times") or []
    frame_descs = results.get("_frame_descriptions") or []

    parts = [f"Filename: {filename}", f"Duration: {duration_s:.0f} seconds"]
    if results.get("transcript"):
        parts.append(f"\nAudio transcript:\n{results['transcript'][:600]}")
    if results.get("ocr_text"):
        parts.append(f"\nOn-screen text (OCR):\n{results['ocr_text'][:300]}")
    if results.get("content_safety_rating"):
        parts.append(f"\nContent safety rating: {results['content_safety_rating']}")

    if frame_descs:
        parts.append(f"\nFrame-by-frame descriptions "
                     f"({len(frame_descs)} keyframes sampled across the clip):")
        for i, (t, d) in enumerate(zip(frame_times, frame_descs)):
            if d:
                parts.append(f"  Frame {i+1} at t={t:.1f}s: {d}")

    context = "\n".join(parts)

    prompt = (
        "The frame-by-frame descriptions above come from ONE short video clip. "
        "Reason ACROSS the frames to produce a VIDEO-level analysis — not a "
        "frame-level one. Capture motion, temporal progression, what happens "
        "first vs last, how subjects move or change, overall narrative.\n\n"
        "Respond with ONLY a valid JSON object (no prose before or after, no markdown). "
        "Every string value MUST be enclosed in double quotes.\n\n"
        "Example of the EXACT format required:\n"
        "{\n"
        '  "summary": "A chef prepares pasta in a kitchen, cracking eggs and kneading dough while narrating each step. The clip progresses from ingredient prep to finished dough.",\n'
        '  "scene_description": "Opens on a countertop with flour and eggs. Mid-clip the chef is kneading dough with their hands. Near the end, the dough is shaped into a ball on a wooden board. The kitchen is warmly lit with copper pots hanging in the background.",\n'
        '  "category": "Cooking/Food",\n'
        '  "mood": "Informative",\n'
        '  "content_rating": "G",\n'
        '  "searchable_keywords": ["pasta", "chef", "cooking", "kitchen", "dough", "eggs", "recipe", "tutorial", "hands", "ingredients"]\n'
        "}\n\n"
        "Required fields (all must be present and string values MUST be quoted):\n"
        '  summary              — 2-3 sentences describing the video as a whole (not a still image)\n'
        '  scene_description    — 3-5 sentences covering setting, subjects, action across the full clip\n'
        '  category             — e.g. "Cooking/Food", "Sports", "Travel/Cityscape", "Music Performance"\n'
        '  mood                 — e.g. "Energetic", "Serene", "Tense", "Informative"\n'
        '  content_rating       — one of "G", "PG", "PG-13", "R"\n'
        '  searchable_keywords  — array of 8-12 lowercase strings\n\n'
        f"Input:\n{context}\n\n"
        "Respond with ONLY the JSON object."
    )

    text = _inference_chat(MODEL_SUMMARY, [{
        "role": "user", "content": prompt,
    }], max_tokens=700, temperature=0.2)

    return _parse_fused_json(text)


def _parse_fused_json(text: str) -> dict:
    """Robustly parse the 70B's JSON output.

    The model occasionally emits malformed JSON (unquoted string values,
    trailing commas, leading prose). We try json.loads first, and if it
    fails we extract each field with a targeted regex so one bad field
    doesn't poison the others.
    """
    clean = text.strip()

    # Strip markdown fences
    if clean.startswith("```"):
        # e.g. ```json\n...\n```
        inner = clean.split("\n", 1)[1] if "\n" in clean else clean[3:]
        inner = inner.rsplit("```", 1)[0]
        clean = inner.strip()

    # If there's a { somewhere, prefer starting from there
    brace = clean.find("{")
    if brace > 0:
        clean = clean[brace:]

    # Fast path: valid JSON
    try:
        obj = json.loads(clean)
        if isinstance(obj, dict):
            return obj
    except json.JSONDecodeError:
        pass

    # Slow path: regex field extraction. Values can be quoted strings or
    # bare text up to the next field name or closing brace.
    import re
    out = {
        "summary": "",
        "scene_description": "",
        "category": "",
        "mood": "",
        "content_rating": "",
        "searchable_keywords": [],
    }
    str_fields = ["summary", "scene_description", "category", "mood", "content_rating"]
    for field in str_fields:
        # Try quoted value first
        m = re.search(rf'"{field}"\s*:\s*"((?:[^"\\]|\\.)*)"', clean, re.DOTALL)
        if m:
            out[field] = m.group(1).encode().decode("unicode_escape")
            continue
        # Fall back to unquoted value (up to the next key or close-brace)
        m = re.search(
            rf'"{field}"\s*:\s*(.+?)(?=,\s*"\w+"\s*:|$|\}})',
            clean, re.DOTALL,
        )
        if m:
            val = m.group(1).strip().rstrip(",").strip()
            # Strip surrounding quotes if any
            if val.startswith('"') and val.endswith('"'):
                val = val[1:-1]
            out[field] = val

    # Keywords array
    m = re.search(r'"searchable_keywords"\s*:\s*(\[.*?\])', clean, re.DOTALL)
    if m:
        try:
            out["searchable_keywords"] = json.loads(m.group(1))
        except json.JSONDecodeError:
            # Fallback: split on commas + strip quotes
            kws = re.findall(r'"([^"]+)"', m.group(1))
            out["searchable_keywords"] = kws

    return out


# ---------------------------------------------------------------------------
# Text embedding + Qdrant indexing
# ---------------------------------------------------------------------------

def _build_passage(results: dict) -> str | None:
    """Combine every text output from this subclip into a single passage.

    The retrieval model is asymmetric — we'll send this with
    input_type="passage" and send the user's query with input_type="query".

    We also append explicit *modality* markers (spoken narration, on-screen
    text present, etc.) when the upstream signals support them. This gives
    the embedder something to latch onto for meta-queries like
      "person talking", "narrator explaining", "someone giving instructions"
    that would otherwise score at noise level because the content passages
    are all topic-focused ("pasta", "kitchen") and never explicitly mention
    that someone is speaking. Redundant phrasing is intentional — we want
    any synonym of the modality to hit.
    """
    parts = []
    summary = (results.get("content_summary") or "").strip()
    category = (results.get("content_category") or "").strip()
    scene = (results.get("scene_description") or "").strip()
    ocr = (results.get("ocr_text") or "").strip()
    transcript = (results.get("transcript") or "").strip()
    keywords = (results.get("searchable_keywords") or "").strip()

    if summary:
        parts.append(f"Summary: {summary}")
    if category:
        parts.append(f"Category: {category}")
    if scene:
        parts.append(f"Scene: {scene}")
    if ocr:
        parts.append(f"On-screen text: {ocr}")
    if transcript:
        parts.append(f"Transcript: {transcript}")
    if keywords:
        parts.append(f"Keywords: {keywords}")

    # ── Modality markers ──
    # Non-trivial transcript → someone is speaking on camera
    transcript_words = len(transcript.split()) if transcript else 0
    if transcript_words >= 5:
        parts.append(
            "Modality: spoken narration on camera. A person is talking, "
            "speaking, narrating, and explaining. Video includes human voice, "
            "dialogue, and verbal commentary. Spoken instruction and explanation are present."
        )
    elif transcript and transcript_words >= 1:
        parts.append(
            "Modality: brief spoken audio — contains a short utterance or voice snippet."
        )
    else:
        parts.append(
            "Modality: no spoken narration detected. Video is primarily visual, "
            "silent, or contains only ambient or instrumental audio without speech."
        )

    # Non-trivial on-screen text → graphics / captions / lower-thirds
    if ocr and len(ocr) >= 5:
        parts.append(
            "Modality: on-screen text present — includes captions, graphics, titles, "
            "lower thirds, subtitles, or written information overlaid on the video."
        )

    if not parts:
        return None
    passage = "\n".join(parts)
    if len(passage) > MAX_EMBED_CHARS:
        passage = passage[:MAX_EMBED_CHARS]
    return passage


def _call_embeddings(text: str, input_type: str = "passage") -> list[float]:
    """POST to /v1/embeddings on the shared inference endpoint."""
    body = json.dumps({
        "model": MODEL_EMBED,
        "input": [text],
        "encoding_format": "float",
        "input_type": input_type,
    })
    conn = http.client.HTTPSConnection(INFERENCE_HOST, timeout=60)
    try:
        conn.request("POST", "/v1/embeddings", body=body, headers={
            "Authorization": f"Bearer {INFERENCE_KEY}",
            "Content-Type": "application/json",
        })
        resp = conn.getresponse()
        raw = resp.read().decode("utf-8")
        if resp.status != 200:
            raise RuntimeError(f"/v1/embeddings {resp.status}: {raw[:400]}")
        payload = json.loads(raw)
    finally:
        conn.close()
    return payload["data"][0]["embedding"]


def _point_id(asset_id: str) -> str:
    """Qdrant point id — we use asset_id directly so the function is idempotent.

    Qdrant accepts unsigned ints or UUIDs as point IDs. Our asset_ids are
    32-char hex MD5s which happen to be valid UUID hex without dashes, so
    we insert the dashes to make Qdrant happy.
    """
    s = asset_id.replace("-", "")
    if len(s) == 32:
        return f"{s[0:8]}-{s[8:12]}-{s[12:16]}-{s[16:20]}-{s[20:32]}"
    return asset_id


def _qdrant_request(base_url: str, method: str, path: str, body: dict | None = None):
    """Low-level Qdrant REST caller using http.client (no qdrant-client dep)."""
    parsed = urlparse(base_url)
    host = parsed.hostname
    port = parsed.port or (443 if parsed.scheme == "https" else 6333)
    if parsed.scheme == "https":
        conn = http.client.HTTPSConnection(host, port, timeout=30,
                                           context=ssl.create_default_context())
    else:
        conn = http.client.HTTPConnection(host, port, timeout=30)
    try:
        payload = json.dumps(body) if body is not None else None
        conn.request(method, path, body=payload, headers={"Content-Type": "application/json"})
        resp = conn.getresponse()
        data = resp.read().decode("utf-8")
        return resp.status, data
    finally:
        conn.close()


def _qdrant_ensure_collection(base_url: str, collection: str, logger):
    """Create the collection if missing (idempotent)."""
    status, body = _qdrant_request(base_url, "GET", f"/collections/{collection}")
    if status == 200:
        logger.info(f"Qdrant collection '{collection}' already exists")
        return
    if status != 404:
        raise RuntimeError(f"Qdrant GET collection unexpected {status}: {body[:300]}")

    create_body = {
        "vectors": {"size": EMBED_DIM, "distance": "Cosine"},
    }
    status, body = _qdrant_request(
        base_url, "PUT", f"/collections/{collection}", create_body,
    )
    if status not in (200, 201):
        raise RuntimeError(f"Qdrant create collection {status}: {body[:300]}")
    logger.info(f"Created Qdrant collection '{collection}' (dim={EMBED_DIM})")


def _qdrant_upsert_point(base_url: str, collection: str, point_id: str,
                         vector: list[float], payload: dict):
    """Upsert a single point (same id → overwrites on retry, which is what we want)."""
    body = {
        "points": [{
            "id": point_id,
            "vector": vector,
            "payload": {k: v for k, v in payload.items() if v is not None},
        }],
    }
    status, resp_body = _qdrant_request(
        base_url, "PUT", f"/collections/{collection}/points?wait=true", body,
    )
    if status not in (200, 202):
        raise RuntimeError(f"Qdrant upsert {status}: {resp_body[:300]}")
