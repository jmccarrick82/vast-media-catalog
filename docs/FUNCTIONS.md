# Media Catalog Function Reference

Documentation for all **15 function containers** + the catalog reconciler service:

- **Pre-ingest pipeline (Phases 1–3, 3 functions + 1 direct-invoke):** `qc-inspector` → `ai-clipper` → `media-packager` → (`subclipper` on demand). Takes raw uploads, QCs them, extracts AI-selected clips, transcodes to delivery renditions, and signs every output with an embedded C2PA manifest.
- **Catalog pipeline (existing, 11 functions):** fires when content lands in `james-media-catalog/` — the 10 provenance functions + `subclip-ai-analyzer` covering all 26 use cases.
- **Reconciler service:** long-running, detects post-ingest moves and deletes.

All pre-ingest functions read their thresholds from the `function_configs` VAST DB table — 65+ knobs editable at runtime via `/settings` without a redeploy.

---

## Table of Contents

1. [Architecture](#architecture)
2. [Function Pattern](#function-pattern)
3. [Pre-Ingest Pipeline — Phases 1–4](#pre-ingest-pipeline--phases-1-4)
4. [Layer 0 — Foundation](#layer-0--foundation)
5. [Layer 0.5 — Keyframe Extraction](#layer-05--keyframe-extraction)
6. [Layer 1 — Analysis](#layer-1--analysis)
7. [Layer 2 — Graph](#layer-2--graph)
8. [Catalog Reconciler Service](#catalog-reconciler-service)
9. [Shared Utilities](#shared-utilities)
10. [Multi-Row Tables Reference](#multi-row-tables-reference)

---

## Architecture

```
┌─────────────────── PRE-INGEST PIPELINE (Phases 1–3) ──────────────────┐
│                                                                        │
│  s3://james-media-inbox/ PUT                                           │
│  │                                                                     │
│  └─► qc-inspector              ffprobe + 9 non-AI detectors           │
│        │  → source_videos row                                          │
│        │  → move to qc-passed or qc-failed                             │
│        ▼                                                               │
│  s3://james-media-qc-passed/ PUT                                       │
│  │                                                                     │
│  └─► ai-clipper                ffmpeg scene detect + vision 11B/90B   │
│        │  → extracted_clips rows                                       │
│        │  → cut to s3://james-media-clips/<source_id>/ + _ready.json   │
│        ▼                                                               │
│  s3://james-media-clips/ _ready.json PUT                               │
│  │                                                                     │
│  └─► media-packager            transcode → C2PA sign → sidecar JSON   │
│        │  → delivery_packages + package_renditions rows                │
│        │  → upload bundle to s3://james-media-deliveries/<pkg_id>/     │
│                                                                        │
│  ─ Direct-invoke only (Phase 4) ─                                      │
│    subclipper                  cut specific [start,end] spans from    │
│                                any source file on demand; no triggers  │
└────────────────────────────────────────────────────────────────────────┘
                                    │  (future hand-off to james-media-catalog)
                                    ▼
┌──────────────── EXISTING CATALOG PIPELINE (11 functions) ─────────────┐
│                                                                        │
│  s3://james-media-catalog/ PUT                                         │
│  │                                                                     │
│  ├─► Layer 0 — Foundation (parallel, all triggered by video PUT)       │
│  │     ├── metadata-extractor     ffprobe → assets                     │
│  │     ├── hash-generator         SHA-256 + pHash → assets             │
│  │     ├── audio-analyzer         chromaprint + SpeechBrain → assets   │
│  │     ├── video-subclip          splits >30s videos → subclips        │
│  │     │                          (each subclip PUT re-triggers)       │
│  │     └── keyframe-extractor     I-frames → james-key-frames          │
│  │                                                                     │
│  ├─► Layer 0.5 — Keyframe Consumers (triggered by james-key-frames)    │
│  │     ├── face-detector          face_recognition → assets + gdpr     │
│  │     └── clip-embedder          CLIP ViT-B-32 → semantic_embeddings  │
│  │                                                                     │
│  ├─► Layer 1 — Analysis (parallel, after Layer 0)                      │
│  │     ├── synthetic-detector     metadata scan → assets               │
│  │     └── hash-comparator        all hash comparisons → relationships │
│  │                                                                     │
│  ├─► Layer 2 — Graph (after hash-comparator + synthetic-detector)      │
│  │     └── graph-analyzer         16 graph analyses → multiple tables  │
│  │                                                                     │
│  └─► Subclip AI (separate trigger: james-media-subclips bucket)        │
│        └── subclip-ai-analyzer    Whisper + Vision + LLM + embed →    │
│                                   transcript, OCR, scene, summary      │
└────────────────────────────────────────────────────────────────────────┘
```

---

## Pre-Ingest Pipeline — Phases 1–4

These are the newer functions that run **before** content reaches the main catalog fan-out. See [INGEST_PIPELINE.md](INGEST_PIPELINE.md) for the full plan, table schemas, and current deployment state.

### qc-inspector (Phase 1)

**File:** `functions/foundation/qc-inspector/main.py`
**Revision:** 6 (live on `james-media-unified` pipeline)
**Trigger:** PUT on `s3://james-media-inbox/*`
**Config scope:** `qc-inspector` (27 knobs)

Audits every raw upload before it enters the pipeline. Runs 9 non-AI detectors via ffmpeg/ffprobe, applies a configurable policy, writes one row to `source_videos`, and routes the file to `james-media-qc-passed/` or `james-media-qc-failed/`. Uploader S3 metadata (`x-amz-meta-*`) is preserved through the move.

**Detectors:** ffprobe (codec/res/fps/channels/sample-rate), black-frames (ffmpeg `blackdetect`), freeze-frames (`freezedetect`), silence (`silencedetect`), loudness (`ebur128` — integrated LUFS + true-peak dBTP), VFR (ffprobe frame interval sampling), interlaced (`idet`), resolution policy gate, codec allow-list policy gate.

**Writes to `source_videos`:**
`source_id`, `s3_inbox_path`, `filename`, `file_size_bytes`, `duration_seconds`, `video_codec`, `video_profile`, `width`, `height`, `fps`, `pixel_format`, `audio_codec`, `audio_channels`, `audio_sample_rate`, `qc_status`, `qc_verdict_reason`, `qc_issues_json`, `qc_black_runs_json`, `qc_freeze_runs_json`, `qc_silence_runs_json`, `qc_black_ratio`, `qc_freeze_ratio`, `qc_silence_ratio`, `qc_loudness_lufs`, `qc_true_peak_dbtp`, `qc_is_vfr`, `qc_is_interlaced`, `qc_config_snapshot_json`, `qc_checked_at`, `qc_elapsed_seconds`, `current_s3_path`.

**Observability:** the handler writes progressive checkpoint markers to the row (`pending:seeded → pending:downloaded → pending:probed → pending:black_starting → pending:black_done → pending:freeze_* → …`) so a stuck handler shows exactly which detector is hung, no log-tailing required. Every ffmpeg subprocess uses `-nostdin` + `stdin=DEVNULL` + `timeout=120` — a bad file can't wedge the pod.

### ai-clipper (Phase 2)

**File:** `functions/foundation/ai-clipper/main.py`
**Revision:** 3 (live)
**Trigger:** PUT on `s3://james-media-qc-passed/*`
**Config scope:** `ai-clipper` (16 knobs)

Given a natural-language prompt (from `x-amz-meta-clip-prompt` > sidecar JSON > `ai-clipper:default_clip_prompt` config), extract the spans of the source video that match it.

**Steps:** ffmpeg scene filter → per-shot midpoint keyframe extract → classify each frame with `nvidia/llama-3.2-11b-vision-instruct` → escalate borderline-confidence frames (`low ≤ conf < high`, configurable) to `nvidia/llama-3.2-90b-vision-instruct` → keep matches ≥ `escalation_confidence_low` → merge adjacent matches within `merge_gap_seconds` → apply `min_clip_seconds` / `max_clip_seconds` / `max_clips_per_source` constraints → expand by editorial buffer (`clip_buffer_pre_seconds` / `clip_buffer_post_seconds`, default 0; clamped to source duration) → cut via `clips.cut_clip` (stream-copy by default for speed; override for frame-exact cuts) → upload each clip to `s3://james-media-clips/<source_id>/clip-NNN.mp4` with S3 metadata → emit `_ready.json` sentinel that fires Phase 3.

**Buffer (v4):** `clip_buffer_pre_seconds` and `clip_buffer_post_seconds` (both default 0.0s, configurable in `/settings`) pad each clip's matched span at the head and tail. Useful when the action you care about starts a beat before or ends a beat after the vision model's tightest matching shot. Applied AFTER constrain so the buffer is additive on top of `max_clip_seconds` (a 30s match with 1s+1s buffer becomes 32s). Clamped to `[0, source_duration]`.

**Writes to `extracted_clips` (one row per clip):**
`clip_id`, `source_id`, `clip_index`, `clip_s3_path`, `start_seconds`, `end_seconds`, `duration_seconds`, `shot_count`, `file_size_bytes`, `prompt`, `prompt_source`, `match_confidence`, `match_reason`, `vision_model`, `frame_scores_json`, `status`.

**Also updates `source_videos`:** `clip_extraction_status`, `clip_count`, `clip_prompt`, `clip_prompt_source`, `clip_extracted_at`.

### media-packager (Phase 3)

**File:** `functions/foundation/media-packager/main.py`
**Revision:** 4 (live)
**Trigger:** PUT on `s3://james-media-clips/*` (handler filters for `_ready.json` markers; all other PUTs are skipped with a one-line log)
**Config scopes:** `packager` (12 knobs) + `provenance` (10 knobs)

The **C2PA headline**. Transcodes each clip into every configured rendition preset, cryptographically signs every rendition with an embedded C2PA manifest via `c2patool`, builds a JSON sidecar manifest, and uploads the bundle to `s3://james-media-deliveries/<package_id>/`.

**Steps per clip:** download raw clip → extract middle-frame JPEG thumbnail → for each rendition preset (e.g. `h264-1080p`, `h264-720p`, `proxy-360p`, `hevc-4k` with `min_source_height=2160` gate): transcode with ffmpeg → sign with `c2patool` (5 assertions: `c2pa.actions.v2`, `c2pa.creative_work`, `c2pa.training-mining`, `com.vast.ai_clip_selection`, `c2pa.hash.bmff.v2`) → upload with `x-amz-meta-source-id/clip-id/package-id/rendition` tags → sha256 and write `package_renditions` row. After all clips processed: build + upload `manifest.json`, finalize `delivery_packages` row.

**Bundled assets:** `ffmpeg`, `ffprobe`, `c2patool` v0.9.12 (same linux-amd64 binary bundle pattern), and a self-signed ES256 X.509 cert with C2PA-required extensions (`emailProtection` EKU, `digitalSignature,nonRepudiation` KU). Cert perms chmod'd to 0644 in `init()` because Knative flips them to 0600 during the pod build and the non-root `cnb` runtime user can't read 0600.

**Writes to `delivery_packages` (one row per bundle):**
`package_id`, `source_id`, `package_root_s3_path`, `manifest_s3_path`, `clip_count`, `rendition_count`, `total_size_bytes`, `rights_cleared_for_json`, `restrictions_json`, `source_attribution`, `clearance_expires_at`, `licensing_notes`, `c2pa_enabled`, `c2pa_signer`, `c2pa_claim_generator`, `c2pa_signed_count`, `c2pa_errors_json`, `status`, `status_reason`.

**Writes to `package_renditions` (one row per (clip × preset)):**
`rendition_id`, `package_id`, `clip_id`, `source_id`, `rendition_name`, `container`, `video_codec`, `audio_codec`, `width`, `height`, `fps`, `video_bitrate`, `audio_bitrate`, `rendition_s3_path`, `file_size_bytes`, `sha256`, `c2pa_signed`, `c2pa_manifest_label`, `c2pa_signer`, `c2pa_signed_at`, `c2pa_error`, `status`.

**Also updates `source_videos`:** `packaging_status`, `package_id`, `packaged_at`.

### subclipper (Phase 4 — direct-invoke, built but not yet deployed)

**File:** `functions/foundation/subclipper/main.py`
**Trigger:** none — invoked directly via the DataEngine `invoke` API
**Config scope:** `subclipper` (7 knobs)

A reusable "chop this video at these timestamps" primitive. Doesn't write to VAST DB — the caller tracks results from the invoke response.

**Event payload:**
```json
{
  "src":        "s3://james-media-catalog/foo.mp4",
  "out_bucket": "james-media-subclips",
  "out_prefix": "my-run/",
  "stream_copy": true,
  "clips": [
    { "start": 0.0,  "end": 3.5  },
    { "start": 12.0, "end": 15.2, "name": "goal-1", "width": 1280, "height": 720 },
    { "start": 30.0, "end": 32.0, "stream_copy": false, "crf": 18 }
  ]
}
```

Per-clip overrides > event-level overrides > config defaults. Returns a structured JSON with `{src, requested, ok, failed, clips: [{out, size_bytes, start, end, …}]}`. Useful for: scripts that already know what spans they want, future orchestration that wants to re-cut an asset with different encoding params, the webapp invoking cuts on demand.

---

### Keyframe Extraction Flow

Previously, `hash-generator`, `clip-embedder`, and `face-detector` all independently downloaded the video and ran ffmpeg to extract I-frames. This caused triple redundancy: 3 video downloads + 3 ffmpeg runs for overlapping frame sets.

Now a dedicated `keyframe-extractor` function runs once, extracts up to 10 I-frames, and uploads them to the `james-key-frames` S3 bucket under `{asset_id}/frame_NNNN.jpg`. A `manifest.json` sentinel file is uploaded last, containing the source asset_id, s3_path, and list of keyframe paths. Downstream functions (`face-detector`, `clip-embedder`) are triggered by the `james-key-frames` bucket, check for `manifest.json` in the object key, and read keyframes from S3 instead of re-extracting.

`hash-generator` still runs in Layer 0 (triggered by the original video PUT) because it needs the full video file for SHA-256 computation. It reads keyframes from the `james-key-frames` bucket for perceptual hashing once they become available, or falls back to local extraction if keyframes aren't ready yet.

---

## Function Pattern

Every function follows the same two-entry-point convention:

```python
def init(ctx):
    config = load_config()
    ctx.user_data = {
        "config": config,
        "s3":   S3Client(config),
        "vast": VastDBClient(config),
    }

def handler(ctx, event):
    s3_path = event.body.decode("utf-8").strip()
    asset_id = hashlib.md5(s3_path.encode()).hexdigest()
    # ... analysis ...
    vast.upsert_asset(asset_id, {column: value, ...})
```

---

## Layer 0 — Foundation

These 6 containers all download the video file and run in parallel on PUT to the `james-media-catalog` bucket. The `keyframe-extractor` additionally uploads extracted frames to the `james-key-frames` bucket, which triggers Layer 0.5 functions.

### metadata-extractor

**File:** `functions/foundation/metadata-extractor/main.py`
**Pip:** base only (ffprobe)

Runs `ffprobe` to extract technical metadata from the video file. Also extracts custom metadata tags embedded by `video-subclip` (via `ffmpeg -metadata`) to reinforce the parent→subclip linkage. If `parent_asset_id` tag is found, sets `is_subclip=True` and `subclip_parent_asset_id` on the asset row.

**Assets columns written:**
`s3_path`, `filename`, `file_size_bytes`, `duration_seconds`, `video_codec`, `audio_codec`, `width`, `height`, `fps`, `bitrate`, `pixel_format`, `audio_channels`, `audio_sample_rate`, `format_name`, `creation_time`, `ingested_at`

When processing subclips, also writes: `is_subclip`, `subclip_parent_asset_id`, `subclip_parent_s3_path`, `subclip_index`, `subclip_start_seconds`

---

### hash-generator

**File:** `functions/foundation/hash-generator/main.py`
**Pip:** base only (imagehash, Pillow)

Computes SHA-256 cryptographic hash and per-frame perceptual hashes (pHash via imagehash).

**Assets columns written:**
`sha256`, `perceptual_hash`, `hash_computed_at`

---

### keyframe-extractor

**File:** `functions/foundation/keyframe-extractor/main.py`
**Pip:** `boto3`, `vastdb`, `pyarrow`
**S3 output bucket:** `james-key-frames`

Extracts I-frame keyframes from video files and uploads them to a dedicated S3 bucket for downstream consumption by `face-detector`, `clip-embedder`, and `hash-generator` (perceptual hash only).

**How it works:**
1. Downloads video from source S3 bucket
2. Runs `ffmpeg -vf select=eq(pict_type,I),scale=320:-1` to extract up to 10 I-frames as JPEGs
3. Uploads each frame to `s3://james-key-frames/{asset_id}/frame_NNNN.jpg`
4. Uploads `manifest.json` as the final file, containing:
   - `asset_id` — MD5 of the source s3_path
   - `source_s3_path` — original video location
   - `source_filename` — basename of the video
   - `keyframe_count` — number of frames extracted
   - `keyframe_paths` — list of `s3://james-key-frames/{asset_id}/frame_NNNN.jpg` paths
   - `extracted_at` — epoch timestamp
5. Upserts keyframe metadata to the assets table

**Why manifest.json is uploaded last:** The `james-key-frames` bucket trigger fires on every PUT. Downstream functions (face-detector, clip-embedder) check the object key for `manifest.json` — non-manifest PUTs are skipped immediately (cheap no-op). Only the manifest upload triggers actual processing, ensuring all frames are available before downstream work begins.

**Assets columns written:**
`keyframe_count`, `keyframe_s3_prefix`, `keyframes_extracted_at`

---

### face-detector

**File:** `functions/foundation/face-detector/main.py`
**Pip:** `face_recognition`
**GPU:** optional (dlib CNN benefits from CUDA)
**Covers:** UC05 (face detection), UC20 (GDPR face tracking)

Downloads video, extracts 10 keyframes, runs `face_recognition`:
1. Detects face locations using HOG model
2. Computes 128-dim face encodings
3. Clusters faces into person IDs by comparing encodings (tolerance=0.6)
4. Records which frames each person appears in

**Assets columns written:**
`faces_detected_count`, `talent_music_scanned_at`, `gdpr_faces_detected`, `gdpr_persons_identified`, `gdpr_blast_radius`, `gdpr_scanned_at`

**Multi-row tables written:**
- `gdpr_personal_data` — per-person records with frame timestamps
- `talent_music` — face detection records (detection_type="face")

---

### audio-analyzer

**File:** `functions/foundation/audio-analyzer/main.py`
**Pip:** `pyacoustid`, `speechbrain`, `torchaudio`
**GPU:** optional (SpeechBrain inference benefits from CUDA)
**Covers:** UC05 (audio fingerprint), UC18 (language detection)

Downloads video, extracts 30s audio segment ONCE, then:
1. Chromaprint audio fingerprinting via `pyacoustid`
2. AcoustID lookup for music identification
3. SpeechBrain ECAPA-TDNN language detection
4. Subtitle track extraction via ffprobe

**Assets columns written:**
`music_detected`, `audio_fingerprint`, `talent_music_scanned_at`, `detected_language`, `language_confidence`, `subtitle_tracks`, `localization_detected_at`

**Multi-row tables written:**
- `talent_music` — audio fingerprint + music match records

---

### video-subclip

**File:** `functions/foundation/video-subclip/main.py`
**Pip:** base only (ffmpeg + boto3)

Splits long videos into fixed-duration subclips (default 30s, configurable via `config.subclip.duration_seconds`) and uploads each subclip to S3. The S3 PUT of each subclip naturally triggers the full function pipeline (Layer 0 → 1 → 2), enabling per-segment analysis (face detection, audio fingerprinting, CLIP embeddings, etc. on each 30s chunk).

**How it works:**
1. Checks if video duration > threshold (default 30s). If not, skips.
2. Computes `N = min(ceil(duration / clip_duration), max_subclips)`.
3. For each subclip: creates via `ffmpeg -c copy` (no re-encoding), embeds parent metadata tags, uploads to hidden subfolder (e.g. `s3://bucket/path/.james.mp4/subclip_001.mp4`).
4. Pre-upserts each subclip's asset row with linkage columns.
5. Writes parent→subclip relationships to `relationships` table.
6. Updates parent asset row with `subclip_count`.

**Recursion prevention (two guards):**
1. **Hidden-path check** — subclips live in `./.<filename>/` folders. Any path with a `.`-prefixed component is skipped before downloading (fast, saves bandwidth).
2. **Duration check** — subclips are ≤ threshold duration, so `duration <= threshold` naturally skips them even if guard #1 were bypassed.

**Parent→subclip linkage (three sources):**
1. ffmpeg `-metadata parent_asset_id=...` embedded in the MP4 container
2. `relationships` table entries (type=`subclip`, confidence=1.0)
3. `subclip_parent_asset_id` column on each subclip's asset row

**Assets columns written (parent):**
`s3_path`, `is_subclip` (False), `subclip_count`

**Assets columns written (each subclip):**
`s3_path`, `filename`, `is_subclip` (True), `subclip_parent_asset_id`, `subclip_parent_s3_path`, `subclip_index`, `subclip_start_seconds`, `subclip_duration_seconds`

**Multi-row tables written:**
- `relationships` — parent→subclip edges (type=`subclip`)

**Configuration:**
- `config.subclip.duration_seconds` — target subclip length AND minimum video length to trigger (default 30)
- `config.subclip.max_subclips` — safety cap for extremely long videos (default 100)

---

## Layer 0.5 — Keyframe Consumers

These functions are triggered by PUT events on the `james-key-frames` bucket. They only process `manifest.json` files (all other PUTs are skipped). They read pre-extracted keyframes from S3 instead of downloading the full video and running ffmpeg independently.

### face-detector (keyframe consumer)

Moved from Layer 0. Now reads keyframes from `james-key-frames` bucket instead of extracting them. Parses the manifest.json to get the source asset_id and keyframe paths, then runs face_recognition on each frame.

### clip-embedder (keyframe consumer)

Moved from Layer 1. Now reads keyframes from `james-key-frames` bucket instead of downloading the full video. Parses the manifest.json to get the source asset_id and keyframe paths, then runs CLIP ViT-B-32 on each frame.

---

## Layer 1 — Analysis

These 2 containers run after Layer 0 completes.

### clip-embedder

**File:** `functions/analysis/clip-embedder/main.py`
**Pip:** `torch`, `torchvision`, `open_clip_torch`
**GPU:** CUDA if available
**Covers:** UC15 (re-use discovery)

Downloads video, extracts 8 keyframes, runs OpenCLIP ViT-B-32 forward pass, L2-normalizes embeddings.

**Assets columns written:**
`has_semantic_embeddings`, `embedding_model_name`, `embedding_frame_count`, `embeddings_extracted_at`

**Multi-row tables written:**
- `semantic_embeddings` — per-frame 512-dim embedding vectors

---

### synthetic-detector

**File:** `functions/analysis/synthetic-detector/main.py`
**Pip:** base only (ffprobe)
**Covers:** UC13 (synthetic content tracking)

Downloads video, runs deep ffprobe metadata extraction, scans for:
- AI tool signatures (Runway, Sora, Stable Diffusion, etc.)
- C2PA content credentials markers
- Encoding anomalies (unusual FPS, missing creation_time)
- AI keywords in tags/comments

**Assets columns written:**
`ai_probability`, `ai_tool_detected`, `ai_model_version`, `ai_detection_method`, `ai_detected_at`

---

### hash-comparator

**File:** `functions/analysis/hash-comparator/main.py`
**Pip:** base only
**Covers:** UC02 (orphan hash match), UC03 (unauthorized use), UC06 (duplicate detection), UC08 (master/derivative), UC09 (reconformation)

DB-only — no video download. Reads assets table ONCE, loops through all other assets' hashes in a single pass:

| Comparison | Threshold | Use Case |
|---|---|---|
| SHA-256 exact match | exact | UC06 duplicate, UC08 duplicate relationship |
| pHash Hamming ≤ 2 | exact_copy | UC03 unauthorized exact copy |
| pHash Hamming ≤ 18 | near_match | UC03 unauthorized, UC06 near-duplicate |
| pHash Hamming ≤ 25 | derivative | UC08 master/derivative classification |
| pHash frame-by-frame 5-30 | reconformation | UC09 archive re-conformation |
| pHash Hamming ≤ 20 + no rels | orphan | UC02 orphan resolution |

Uses quality score (width × bitrate) to determine master vs derivative direction.

**Assets columns written:**
`orphan_resolved_from_asset_id`, `orphan_resolution_method`, `orphan_resolved_at`, `unauthorized_match_count`, `unauthorized_checked_at`, `duplicate_count`, `total_storage_savings_bytes`, `duplicates_checked_at`, `asset_classification`, `classification_confidence`, `classification_at`, `reconformation_match_count`, `reconformation_viable`, `reconformation_checked_at`

**Multi-row tables written:**
- `relationships` — parent/child edges with type + confidence
- `hash_matches` — all match records with similarity scores

---

## Subclip AI Analysis

This function has its own S3 trigger on the `james-media-subclips` bucket, not the main `james-media-catalog` bucket. It runs after `video-subclip` writes a 30-second subclip, and performs sequential inference-endpoint calls to produce human-readable analysis for each subclip.

### subclip-ai-analyzer

**File:** `functions/analysis/subclip-ai-analyzer/main.py`
**Pip:** `boto3`, `botocore`, `vastdb`, `pyarrow`
**Trigger:** `james-subclips-trigger` (S3 PUT on `james-media-subclips`)
**Inference endpoint:** `https://inference.selab.vastdata.com`

For each subclip, the handler runs sequentially (one model at a time):

| Step | Model | Input | Output |
|---|---|---|---|
| 1 | `local-mlx/whisper-turbo` | audio (30s WAV) | `transcript` |
| 2 | `nvidia/llama-3.2-90b-vision-instruct` | keyframe JPEG | `ocr_text` |
| 3 | `nvidia/llama-3.2-90b-vision-instruct` | keyframe JPEG | `scene_description` + `content_tags` |
| 4 | `nvidia/llama-3.2-11b-vision-instruct` | keyframe JPEG | `ai_content_assessment` + `ai_probability_vision` |
| 5 | `nvidia/llama-guard-4-12b` | keyframe JPEG | `content_safety_rating` |
| 6 | `nvidia/llama-3.3-70b-instruct` | all prior results | `content_summary`, `content_category`, `content_mood`, `content_rating`, `searchable_keywords` |
| 7 | `nvidia/nv-embed-v1` | summary + scene + OCR + transcript + keywords (single passage) | 4096-dim vector → **Qdrant point** (payload carries s3_path, summary, category, mood, rating, keywords, embedded_text); `text_embedding_created_at`, `text_embedding_model` |

**Assets columns written** (upserted to the same `assets` row as the subclip):
`transcript`, `ocr_text`, `scene_description`, `content_tags`, `ai_content_assessment`, `ai_probability_vision`, `content_safety_rating`, `content_summary`, `content_category`, `content_mood`, `content_rating`, `searchable_keywords`, `ai_analyzed_at`, `text_embedding_created_at`, `text_embedding_model`

**Reliability/observability:** every inference call goes through `_call_with_retry_and_timing()` — 6 attempts, exponential backoff `[5, 15, 30, 60, 120]s` + jitter, per-call timing logs under the `[timing]` prefix. Step 6 was swapped down from `llama-3.1-405b-instruct` → `llama-3.3-70b-instruct` (summaries don't need a 405B model and the 70B runs ~3–5× faster end-to-end).

**Note:** This function does NOT use the shared `shared/schemas.py` — it ships its own copy of `schemas.py`, `vast_client.py`, `s3_client.py`, `config_loader.py`, and bundles static `ffmpeg`/`ffprobe` binaries in the function directory. Column evolution is handled by `VastDBClient.setup_tables()` on first handler call (adds missing columns via `table.add_column()`).

---

## Layer 2 — Graph

### graph-analyzer

**File:** `functions/analysis/graph-analyzer/main.py`
**Pip:** base only
**Covers:** UC01, UC04, UC07, UC10, UC11, UC12, UC14, UC16, UC17, UC19, UC21, UC22, UC23, UC24, UC25, UC26

DB-only — no video download. Reads `relationships`, `assets`, and `hash_matches` tables ONCE, builds bidirectional adjacency maps ONCE, then runs 16 analysis modules:

| Module | Use Case | Graph Pattern |
|---|---|---|
| Rights Conflict | UC01 | Walk parent chain, check restrictions |
| Orphan Status | UC02 | Check if any relationships exist |
| License Audit | UC04 | Trace ancestors + count descendants |
| Safe Deletion | UC07 | Count transitive dependents |
| Version Control | UC10 | Walk parent chain, assign version numbers |
| Training Provenance | UC11 | Trace to root, check training rights |
| Model Contamination | UC12 | BFS upward for AI relationship types |
| Bias Audit | UC14 | BFS upward for model + training data |
| Clearance Inheritance | UC16 | BFS upward, inherit first clearance |
| Compliance Propagation | UC17 | BFS upward, select most restrictive |
| Leak Investigation | UC19 | BFS downward, extract recipient/date |
| Chain of Custody | UC21 | BFS both directions, record SHA baseline |
| Ransomware Assessment | UC22 | Check uniqueness + backups + descendants |
| Content Valuation | UC23 | BFS downward, classify by relationship type |
| Syndication Tracking | UC24 | BFS downward, extract territory/licensee |
| Insurance Valuation | UC25 | Check irreplaceability + copies |
| Co-Production | UC26 | BFS upward, extract company/crew |

**Assets columns written:** All columns for UC01, UC04, UC07, UC10-UC12, UC14, UC16-UC17, UC19, UC21-UC26 (see `shared/schemas.py` for full list)

**Multi-row tables written:**
- `version_history` — version chain records (UC10)
- `syndication_records` — licensee/territory records (UC24)
- `production_entities` — contributor attribution records (UC26)

---

## Catalog Reconciler Service

### catalog-reconciler

**File:** `services/catalog-reconciler/main.py`
**Pip:** `trino`, `vastdb`, `pyarrow`
**Type:** Long-running service (not a serverless function)

Detects file moves and deletes that happen after initial ingestion by diffing the VAST Big Catalog against the assets table every 30 minutes, then investigating missing assets via the VAST Audit Log.

**Reconciliation cycle (every 30 minutes):**

1. **Catalog scan** — Queries VAST Big Catalog via Trino to get all current files
2. **Assets scan** — Reads `assets` table from VAST DB (active assets only)
3. **Diff** — Computes `missing = assets_paths - catalog_paths`
4. **Investigate** — For each missing asset, queries the VAST Audit Log:
   - First looks for RENAME/move events across NFS, SMB, S3
   - If no rename found, looks for DELETE events across all 3 protocols
   - If nothing found, logs ERROR (audit log integration may be broken)
5. **Update timestamps** — Sets `last_reconciled_at` on all confirmed-present assets

**Move handling:**

When a RENAME/move is detected:
- Old row marked `status = "moved"`
- New row created with `new_asset_id = MD5(new_s3_path)`, all metadata carried over
- Path-dependent columns re-derived from new path (territory, licensee, company, crew, recipient, date)
- Relationship edges duplicated from old to new asset_id
- Event recorded in `asset_moves` table

**Delete handling:**

When a DELETE is detected:
- Row marked `status = "deleted"`, `deleted_at` and `deleted_by` set
- Event recorded in `asset_moves` table

**Audit log protocol coverage:**

| Protocol | Move RPCs | Delete RPCs |
|---|---|---|
| NFS | `RENAME` → `rename_path` + `rename_name` | `REMOVE`, `RMDIR` |
| SMB | `RENAME` → `smb_rename_struct.path` | `CLOSE` with `smb_delete_on_close = true` |
| S3 | `CopyObject` + `DeleteObject` | `DeleteObject` |

**Assets columns written:**
`status`, `original_s3_path`, `move_count`, `last_moved_at`, `last_moved_by`, `deleted_at`, `deleted_by`, `last_reconciled_at`

**Multi-row tables written:**
- `asset_moves` — move and delete event audit trail

**Configuration:**
- `config.reconciler.interval_seconds` — polling interval (default 1800 = 30 min)
- `config.reconciler.lookback_seconds` — audit log search window (default 2100 = 35 min)
- `config.catalog.big_catalog_table` — VAST Big Catalog Trino table path
- `config.catalog.audit_log_table` — VAST Audit Log Trino table path
- `config.catalog.media_search_path` — root path to scan in Big Catalog

---

## Deployment Workflows

### VAST DataEngine CLI Commands

All functions are built, pushed, and deployed using the VAST DataEngine CLI (`vast`).

**Build → Push → Create → Deploy lifecycle:**

```bash
# 1. Build the function container (run from function directory)
vast functions build <function-name>

# 2. Tag and push to the container registry
docker tag <function-name>:latest docker.selab.vastdata.com:5000/james/<function-name>:latest
docker push docker.selab.vastdata.com:5000/james/<function-name>:latest

# 3. Create the function on DataEngine (first time only)
vast functions create \
  --name <de-function-name> \
  --artifact-source james/<function-name> \
  --artifact-type image \
  --container-registry selab-docker \
  --image-tag latest \
  --publish

# 4. Create and deploy the pipeline (first time only)
vast pipelines create --config @pipeline.yaml --deploy

# 5. For updates: rebuild, push, update function, redeploy pipeline
vast functions update <de-function-name> --publish
vast pipelines deploy <de-pipeline-name>
```

**Monitoring and debugging:**

```bash
# Check pipeline status
vast pipelines get <pipeline-name>

# View function execution logs
vast logs get <pipeline-name>

# Local testing (runs container locally with DataEngine runtime)
vast functions localrun <function-name>
```

### Deployed Functions and Pipelines

| Function Name | DataEngine Function | Pipeline | Status |
|---|---|---|---|
| metadata-extractor | `james-metadata-extractor` | `james-media` | Ready |
| hash-generator | `james-hash-generator` | `james-media-hash` | Ready |
| keyframe-extractor | `james-keyframe-extractor` | `james-media-keyframes` | Ready |
| audio-analyzer | `james-audio-analyzer` | `james-media-audio` | Ready |
| video-subclip | `james-video-subclip` | `james-media-subclip` | Ready |
| hash-comparator | `james-hash-comparator` | `james-media-comparator` | Ready |
| synthetic-detector | `james-synthetic-detector` | `james-media-synthetic` | Ready |
| graph-analyzer | `james-graph-analyzer` | `james-media-graph` | Ready |

**Not yet deployed (GPU required):**

| Function Name | Reason |
|---|---|
| face-detector | Requires GPU (dlib CNN / CUDA) — deferred |
| clip-embedder | Requires GPU (CLIP ViT-B-32 / CUDA) — deferred |

All pipelines use:
- **Kubernetes cluster:** `vast:dataengine:kubernetes-clusters:var201-k8s`
- **Trigger:** `vast:dataengine:triggers:james-media-catalog` (PUT events on `james-media-catalog` bucket)
- **Topic:** `vast:dataengine:topics:engine-broker/main`
- **Container registry:** `docker.selab.vastdata.com:5000/james/`

### Pipeline YAML Configuration

Each function has a `pipeline.yaml` that defines its DataEngine pipeline. Key resource settings:

| Function | CPU (min/max) | Memory (min/max) | Timeout | Max Concurrency |
|---|---|---|---|---|
| metadata-extractor | 200m / 1 | 256Mi / 512Mi | 120s | 10 |
| hash-generator | 200m / 1 | 256Mi / 512Mi | 120s | 10 |
| keyframe-extractor | 200m / 1 | 256Mi / 1Gi | 180s | 10 |
| audio-analyzer | 100m / 1 | 256Mi / 1Gi | 180s | 10 |
| video-subclip | 100m / 1 | 256Mi / 1Gi | 180s | 10 |
| hash-comparator | 100m / 500m | 128Mi / 512Mi | 120s | 10 |
| synthetic-detector | 100m / 500m | 128Mi / 512Mi | 120s | 10 |
| graph-analyzer | 100m / 1 | 256Mi / 1Gi | 180s | 10 |

### Schema Evolution

The `vast_client.py` shared by all functions includes automatic schema evolution. When `setup_tables()` runs on container init, it:

1. Opens the existing table
2. Compares actual table columns against the `ASSETS_SCHEMA` in `schemas.py`
3. Adds any missing columns via `table.add_column()`
4. Logs which columns were added

This ensures that when new columns are added to `schemas.py` (e.g., for a new function or use case), they are automatically created in the VAST DB table on the next function invocation — no manual ALTER TABLE needed.

```python
# Schema evolution in setup_tables():
existing_cols = {f.name for f in table.columns()}
missing = pa.schema([f for f in schema if f.name not in existing_cols])
if len(missing) > 0:
    table.add_column(missing)
```

### Aptfile Caveat (ARM builds)

When building on Apple Silicon (ARM), the `Aptfile` must contain only a comment (`# apt packages come here`) — not actual packages. If apt packages are listed, the `apt-buildpack` layer sets `LD_LIBRARY_PATH` to x86 paths, breaking Python on ARM containers. Functions needing system binaries (e.g., ffmpeg, ffprobe) should include them as static binaries in the function directory instead.

---

## Shared Utilities

### `shared/hash_utils.py`

Single-copy hash comparison functions (replaces 5 duplicate implementations):
- `hamming_distance(hash_a, hash_b)` — Hamming distance between hex hash strings
- `compare_video_phashes(phash_a, phash_b)` — average per-frame distance
- `compare_frame_sequences(phash_a, phash_b)` — detailed frame-by-frame with match counting

### `shared/graph_utils.py`

Single-copy graph traversal functions (replaces 19 duplicate implementations):
- `build_adjacency(rel_table)` — builds both parent→children and child→parents maps
- `find_parents()` / `find_children()` — direct neighbors
- `find_all_ancestors()` — BFS upward with depth tracking
- `count_descendants()` — BFS downward count
- `trace_chain_downward()` — BFS downward with full relationship info
- `has_relationship()` — check if asset has any edges
- `trace_root()` — walk to root following first parent

### `shared/path_helpers.py`

Path-parsing utilities for extracting metadata from S3/file paths. Used by graph-analyzer (UC19, UC24, UC26) and catalog-reconciler to derive territory, licensee, company, crew origin, recipient, and date from paths.

**Lookup dicts:**
- `TERRITORY_PATTERNS` — maps codes (`us`, `uk`, `emea`, etc.) to display names
- `LOCATION_MARKERS` — maps city codes (`la`, `nyc`, `london`, etc.) to city names

**Functions:**
- `extract_territory(path)` — extract territory from path segments
- `extract_licensee(path)` — extract licensee from keyword-adjacent directory
- `extract_company(path)` — extract production company from keyword-adjacent directory
- `extract_crew_origin(path)` — extract crew/production location
- `extract_recipient(path)` — extract delivery recipient
- `extract_date(path)` — extract YYYY-MM-DD date
- `classify_contribution(rel_type)` — classify relationship type into contribution category

### `shared/catalog_client.py`

Trino-based client for querying the VAST Big Catalog and VAST Audit Log. Used by catalog-reconciler.

**Class:** `CatalogClient(config)`
- Reads `config["trino"]` for Trino host/port
- Reads `config["catalog"]` for Big Catalog and Audit Log table paths

**Methods:**
- `list_catalog_files(search_path)` — query VAST Big Catalog for all files under a path. Returns dict of `full_path → size`
- `find_rename_events(path, lookback_seconds)` — search audit log for RENAME/move RPCs across NFS, SMB, S3. Returns list of dicts with `protocol`, `rpc_type`, `rename_destination`, `time`, `login_name`, `client_ip`
- `find_delete_events(path, lookback_seconds)` — search audit log for DELETE RPCs across all 3 protocols. Returns list of dicts with same fields
- `query_all_events(path, lookback_seconds)` — get all audit events for a path (debug fallback)

### `shared/vast_client.py`

- `load_table_safe(table_name, logger)` — read table returning None if missing (replaces 17 copies)

---

## Multi-Row Tables Reference

| Table | Written By | Purpose |
|---|---|---|
| `relationships` | hash-comparator, video-subclip, catalog-reconciler | Parent/child edges with type + confidence |
| `hash_matches` | hash-comparator | All hash comparison match records |
| `talent_music` | face-detector, audio-analyzer | Face + audio detection records |
| `semantic_embeddings` | clip-embedder | Per-frame CLIP embedding vectors |
| `gdpr_personal_data` | face-detector | Per-person face tracking records |
| `version_history` | graph-analyzer | Version chain records |
| `syndication_records` | graph-analyzer | Licensee/territory delivery records |
| `production_entities` | graph-analyzer | Contributor attribution records |
| `asset_moves` | catalog-reconciler | Move and delete event audit trail |
