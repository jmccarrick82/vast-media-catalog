# Media Catalog — Content Provenance Graph

A comprehensive content provenance system built on VAST Data Platform. Ingests raw video, **runs automated QC, extracts AI-selected clips from a natural-language prompt, transcodes into delivery-ready renditions, and cryptographically signs every output with an embedded [C2PA](https://c2pa.org) manifest** — then hands the signed content off to the existing 8-function provenance fan-out. A catalog reconciler service continuously tracks post-ingest file moves and deletes across S3, NFS, and SMB protocols.

## Architecture

```
╔════════════════════════════════════════════════════════════════════════════╗
║  PRE-INGEST PIPELINE (Phases 1–3)                                          ║
╠════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║   s3://james-media-inbox/  ─┐                                              ║
║                             │                                              ║
║   Phase 1: QC Inspector ────┘   9 detectors (black, freeze, silence,       ║
║                                 loudness, VFR, interlace, codec, res…)     ║
║     │  passed / warn                                                       ║
║     ▼                                                                      ║
║   s3://james-media-qc-passed/  ──┐  (or  qc-failed/ on policy violation)   ║
║                                  │                                         ║
║   Phase 2: AI Clipper ───────────┘   scene-detect → per-shot vision        ║
║                                      classify (11B→90B escalate) →         ║
║                                      merge+constrain → cut                 ║
║     │  extracted_clips                                                     ║
║     ▼                                                                      ║
║   s3://james-media-clips/<source_id>/clip-NNN.mp4  + _ready.json ──┐       ║
║                                                                    │       ║
║   Phase 3: Media Packager + C2PA ──────────────────────────────────┘       ║
║     Transcode each clip → every rendition preset                           ║
║     Sign EACH rendition with embedded C2PA manifest (c2patool):            ║
║        • c2pa.actions  (created → placed [AI] → edited)                    ║
║        • com.vast.ai_clip_selection  (model + prompt + confidence)         ║
║        • c2pa.training-mining  (notAllowed by default)                     ║
║        • c2pa.creative_work + c2pa.hash.bmff.v2  (tamper-evidence)         ║
║     Build sidecar manifest.json → upload to                                ║
║     s3://james-media-deliveries/<package_id>/                              ║
║                                                                            ║
║   Every threshold, preset, and C2PA setting is runtime-editable via        ║
║   /settings (65 knobs across 4 scopes in function_configs).                ║
╚════════════════════════════════════════════════════════════════════════════╝
                                    │
                                    ▼  (future: hand-off to james-media-catalog)
┌──────────────────────────────────────────────────────────────────────────┐
│  EXISTING CATALOG PIPELINE (8 provenance functions + subclip AI)          │
│  Raw uploads to s3://james-media-catalog/ still run the main fan-out.     │
└──────────────┬────────────────────────────────────────────────────────────┘
               │ Object PUT trigger
               ▼
┌──────────────────────────┐   ┌─────────────────────────────────────────────┐
│  Layer 0 (Foundation)    │   │  Unified Assets Table (VAST DB)             │
│  ┌────────────────────┐  │   │  ┌───────────────────────────────────────┐  │
│  │ metadata-extractor  │──┼──▶│  │ One row per asset, ~150 columns     │  │
│  │ hash-generator      │──┼──▶│  │ Each function upserts its columns   │  │
│  │ face-detector       │──┼──▶│  └───────────────────────────────────────┘  │
│  │ audio-analyzer [GPU]│──┼──▶│  ┌───────────────────────────────────────┐  │
│  │ video-subclip       │──┼──▶│  │ Relationship tables (multi-row)       │  │
│  └────────────────────┘  │   │  │ • relationships    • hash_matches     │  │
└──────────────┬───────────┘   │  │ • talent_music     • semantic_embeddings│ │
               ▼               │  │ • gdpr_personal_data                  │  │
┌──────────────────────────┐   │  │ • syndication_records                 │  │
│  Layer 1 (Analysis)      │   │  │ • production_entities                 │  │
│  ┌────────────────────┐  │   │  │ • version_history  • asset_moves     │  │
│  │ clip-embedder [GPU] │──┼──▶│  └───────────────────────────────────────┘  │
│  │ synthetic-detector  │──┼──▶│  ┌───────────────────────────────────────┐  │
│  │ hash-comparator     │──┼──▶│  │ Pre-ingest tables (NEW)               │  │
│  └────────────────────┘  │   │  │ • source_videos     • extracted_clips │  │
└──────────────┬───────────┘   │  │ • delivery_packages • package_renditions│ │
               ▼               │  │ • function_configs  (65 editable knobs)│  │
┌──────────────────────────┐   │  └───────────────────────────────────────┘  │
│  Layer 2 (Graph)         │   └─────────────────────────────────────────────┘
│  └── graph-analyzer      │
└──────────────────────────┘
                                  ┌──────────────────────────────────┐
                                  │  Webapp (Flask + React + Trino)  │
                                  │  /packages  ← signed C2PA viewer │
                                  │  /search    ← semantic clip find │
                                  │  /settings  ← live config editor │
                                  │  /architecture  ← explainer      │
                                  └──────────────────────────────────┘
```

## Quick Start

### 1. Configure

Edit `config/config.json` with your VAST cluster details:

```json
{
  "vast": {
    "endpoint": "http://YOUR_VAST_VIP",
    "access_key": "YOUR_ACCESS_KEY",
    "secret_key": "YOUR_SECRET_KEY",
    "bucket": "media-catalog"
  },
  "s3": {
    "endpoint": "http://YOUR_VAST_VIP",
    "access_key": "YOUR_ACCESS_KEY",
    "secret_key": "YOUR_SECRET_KEY"
  },
  "trino": {
    "host": "localhost",
    "port": 8080,
    "catalog": "vast",
    "schema": "media-catalog/provenance"
  },
  "catalog": {
    "big_catalog_table": "vast-big-catalog-bucket/vast_big_catalog_schema",
    "audit_log_table": "vast-audit-log-bucket/vast_audit_log_schema",
    "media_search_path": "/media-catalog"
  },
  "reconciler": {
    "interval_seconds": 1800,
    "lookback_seconds": 2100
  },
  "subclip": {
    "duration_seconds": 30,
    "max_subclips": 100
  },
  "inference": {
    "host": "inference.selab.vastdata.com",
    "api_key": "YOUR_INFERENCE_KEY",
    "embed_model": "nvidia/nv-embed-v1",
    "embed_dim": 4096
  },
  "qdrant": {
    "url": "http://YOUR_DOCKER_HOST:6333",
    "collection": "subclips"
  },
  "gpu": {
    "host": "GPU_HOST_IP",
    "user": "GPU_HOST_USER",
    "password": "GPU_HOST_PASSWORD",
    "deploy_path": "/home/GPU_HOST_USER/media-catalog",
    "containers": ["audio-analyzer", "clip-embedder"]
  }
}
```

The `inference` block is used by `subclip-ai-analyzer` (Whisper, Vision, Llama,
and — as of the latest revision — embeddings) and by the webapp backend at
query time. The `qdrant` block points the function at the vector DB container
so it can write a point per subclip at the end of each AI run. The same URL is
read by the webapp for the `/search` page. See
[Semantic search](#semantic-search) below.

### 2. Build the Base Image

```bash
docker build -t media-catalog-base -f shared/Dockerfile.base .
```

### 3. Build Function Containers

```bash
docker compose -f docker-compose-functions.yml build
```

### 4. Build & Run the Webapp

```bash
docker compose up --build
# Webapp: http://localhost:3000
# Trino:  http://localhost:8080
```

### 5. Build & Run the Catalog Reconciler

```bash
docker build -t media-catalog-reconciler services/catalog-reconciler/
docker run -d --name catalog-reconciler \
  -v $(pwd)/config/config.json:/app/config/config.json:ro \
  -v $(pwd)/shared:/app/shared:ro \
  media-catalog-reconciler
```

## Function Interface

Every function uses the VAST serverless function framework:

```python
def init(ctx):
    """Called once when the pipeline loads."""
    # Initialize DB clients, load config
    ctx.logger.info("Initialized")

def handler(ctx, event):
    """Called for each new video file."""
    s3_path = event.body.decode("utf-8")
    # Process video, upsert to unified assets table
    return json.dumps({"asset_id": "...", "status": "ok"})
```

The `event.body` contains the S3 path to the new video file.

## Trigger Chain

Functions are deployed as a single unified pipeline (`james-media-unified`) with **5 S3 triggers**: 3 for the pre-ingest pipeline (new) and 2 for the existing catalog pipeline.

```
PRE-INGEST PIPELINE (Phases 1–3)

Trigger: james-inbox-trigger           james-media-inbox/* PUT
  james-qc-inspector                   9 non-AI detectors → source_videos row → move to qc-passed/qc-failed

Trigger: james-qc-passed-trigger       james-media-qc-passed/* PUT
  james-ai-clipper                     scene detect + vision classify (11B→90B) → extracted_clips rows → cut to clips bucket + _ready.json marker

Trigger: james-clips-ready-trigger     james-media-clips/* PUT (handler skips non-marker events)
  james-media-packager                 transcode → C2PA sign every rendition → sidecar manifest → delivery_packages + package_renditions rows

CATALOG PIPELINE (existing)

Trigger: james-media-catalog           james-media-catalog/* PUT → 8 provenance functions (9 counting keyframe-extractor)

  Layer 0 — Foundation (parallel):
    metadata-extractor                 ffprobe metadata + subclip tag extraction
    hash-generator                     SHA-256 + perceptual hash
    keyframe-extractor                 I-frame extraction → james-key-frames bucket
    audio-analyzer                     audio fingerprint + language detection  [GPU]
    video-subclip                      splits >30s videos into subclips → writes to james-media-subclips

  Layer 0.5 — Keyframe Consumers (triggered by james-key-frames PUT):
    face-detector                      reads keyframes → face_recognition  [GPU]
    clip-embedder                      reads keyframes → CLIP ViT-B-32     [GPU]

  Layer 1 — Analysis:
    synthetic-detector                 AI-generated content detection
    hash-comparator                    all hash comparisons in one pass

  Layer 2 — Graph:
    graph-analyzer                     all 16 graph-based analyses in one pass

Trigger: james-subclips-trigger        james-media-subclips/* PUT → subclip AI analysis
  subclip-ai-analyzer                  Whisper + Vision + LLM + embed → assets + Qdrant point
```

**Pipeline:** `james-media-unified` (VRN: `vast:dataengine:pipelines:james-media-unified`)
**Triggers:** `james-inbox-trigger`, `james-qc-passed-trigger`, `james-clips-ready-trigger`, `james-media-catalog`, `james-subclips-trigger`

## C2PA Content Credentials

Every rendition the pre-ingest pipeline emits carries an **embedded, cryptographically signed C2PA manifest**. Open any file in `s3://james-media-deliveries/` with the [Adobe Content Credentials panel](https://contentcredentials.org) or run `c2patool file.mp4` locally and you see:

- **Actions chain** — `c2pa.created` → `c2pa.placed` (AI clip-selection with source timespan) → `c2pa.edited` (transcode, per rendition).
- **AI disclosure** (`com.vast.ai_clip_selection`) — vision model name, the exact prompt used to select the clip, match confidence, source timespan. This is the machine-readable signal regulators are aligning on (EU AI Act, California AB 942).
- **Training/mining policy** (`c2pa.training-mining`) — every axis (generative training, AI training, data mining, inference) defaults to `notAllowed`; editable per-package.
- **Creative work + attribution** (`c2pa.creative_work`) — source filename + VAST attribution.
- **Tamper-evidence** (`c2pa.hash.bmff.v2`) — auto-generated hash over the MP4 container. Any byte-level edit invalidates the signature.

Signed with a self-signed ES256 X.509 cert (demo). Verify locally:

```bash
c2patool your-rendition.mp4 | jq '.manifests[] | .signature_info, .assertions[].label'
```

Or inspect in the webapp at `/packages/<package_id>` — the detail page runs c2patool server-side against the selected rendition and renders the full assertion tree, including AI disclosure, action chain, and training-mining flags.

## Project Structure

```
catalog/
├── config/config.json              # VAST, S3, Trino, catalog, reconciler, GPU config
├── shared/                         # Shared Python libraries
│   ├── Dockerfile.base             # Base image (Python + ffmpeg + libs)
│   ├── schemas.py                  # Unified assets schema (~150 cols) + 9 relation schemas
│   ├── vast_client.py              # VAST DB client with upsert_asset()
│   ├── s3_client.py                # S3 video file access
│   ├── video_analyzer.py           # ffprobe, hashing, frame extraction
│   ├── config_loader.py            # Config file parser
│   ├── hash_utils.py               # Hash comparison functions
│   ├── graph_utils.py              # Graph traversal functions
│   ├── path_helpers.py             # Path-parsing utilities (territory, licensee, etc.)
│   └── catalog_client.py           # Trino client for VAST Big Catalog + Audit Log
├── functions/                      # 11 function containers
│   ├── foundation/                 # metadata-extractor, hash-generator, keyframe-extractor,
│   │                               # face-detector, audio-analyzer, video-subclip
│   ├── analysis/                   # clip-embedder, synthetic-detector,
│   │                               # hash-comparator, graph-analyzer, subclip-ai-analyzer
│   └── pipeline-unified.yaml       # Unified pipeline with both triggers
├── services/                       # Long-running services
│   └── catalog-reconciler/         # Detects moves & deletes via Big Catalog + Audit Log
│       ├── main.py
│       └── Dockerfile
├── webapp/                         # Flask + React + Trino
│   ├── backend/                    # Flask API
│   ├── frontend/                   # React + Vite
│   └── trino/                      # Trino-VAST connector config
├── docs/                           # Documentation
│   ├── DEPLOYMENT.md               # Build, deploy, verify
│   ├── FUNCTIONS.md                # Function + reconciler reference
│   ├── SCHEMA.md                   # All table schemas + query examples
│   └── WEBAPP.md                   # API endpoints and frontend pages
├── docker-compose.yml              # Webapp
└── docker-compose-functions.yml    # All function containers
```

## VAST DB Tables

### Pre-ingest pipeline

| Table | Rows | Written By |
|---|---|---|
| `source_videos` | 1 per raw upload (50 cols) | qc-inspector, ai-clipper, media-packager |
| `extracted_clips` | Many per source (21 cols) | ai-clipper |
| `delivery_packages` | 1 per delivery bundle (24 cols) | media-packager |
| `package_renditions` | Many per package (24 cols, incl. C2PA manifest label) | media-packager |
| `function_configs` | 65 rows (4 scopes: qc-inspector, ai-clipper, packager, provenance) | `scripts/seed_function_configs.py` + `/settings` UI |

### Existing catalog pipeline

| Table | Rows | Written By |
|---|---|---|
| `assets` | 1 per asset (~165 columns) | All 11 functions + reconciler |
| `relationships` | Many per asset | hash-comparator, video-subclip, reconciler |
| `hash_matches` | Many per asset | hash-comparator |
| `talent_music` | Many per asset | face-detector, audio-analyzer |
| `semantic_embeddings` | Many per asset | clip-embedder |
| `gdpr_personal_data` | Many per asset | face-detector |
| `syndication_records` | Many per asset | graph-analyzer |
| `production_entities` | Many per asset | graph-analyzer |
| `version_history` | Many per asset | graph-analyzer |
| `asset_moves` | 1 per move/delete event | catalog-reconciler |

## Inference Endpoint

An OpenAI-compatible inference API is available for AI/ML experimentation:

| | |
|---|---|
| **Endpoint** | `https://inference.selab.vastdata.com` |
| **API Key** | `YOUR_INFERENCE_KEY` |
| **Compatibility** | OpenAI API (v1/models, v1/chat/completions) |
| **Models** | 221 models available |

### Quick Test

```bash
# List models
curl -s https://inference.selab.vastdata.com/v1/models \
  -H "Authorization: Bearer YOUR_INFERENCE_KEY" | python3 -m json.tool

# Chat completion
curl -s https://inference.selab.vastdata.com/v1/chat/completions \
  -H "Authorization: Bearer YOUR_INFERENCE_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model": "nvidia/llama-3.1-405b-instruct", "messages": [{"role": "user", "content": "Hello"}]}'
```

### Notable Models

| Category | Models |
|----------|--------|
| **Large LLMs** | `nvidia/llama-3.1-405b-instruct`, `nvidia/deepseek-ai-deepseek-v3-1`, `cerebras/deepseek-v3-0324` |
| **DeepSeek** | `ollama/deepseek-r1`, `nvidia/deepseek-ai-deepseek-r1`, `nvidia/deepseek-ai-deepseek-v3-2` |
| **Qwen** | `ollama/qwen3`, `nvidia/qwen-qwen3-235b-a22b-instruct`, `nvidia/qwen-qwen3-30b-a3b` |
| **Coding** | `ollama/qwen2.5-coder`, `nvidia/qwen-qwen2.5-coder-32b-instruct` |
| **Vision** | `ollama/gemma3`, `nvidia/google-gemma-3-27b-it`, `nvidia/mistralai-pixtral-large-2501` |
| **Small/Fast** | `ollama/llama3.2`, `ollama/phi4-mini`, `ollama/gemma3:4b` |
| **Reasoning** | `nvidia/kimi-k2-instruct`, `nvidia/meta-llama-4-maverick-17b-128e-instruct` |
| **MLX (local)** | `local-mlx/gemma-2-2b-it`, `local-mlx/Llama-3.2-1B-Instruct` |

Full model list: `curl -s .../v1/models -H "Authorization: Bearer $KEY" | jq '.data[].id'`

### Subclip AI Analysis Pipeline

The `subclip-ai-analyzer` function runs the following models sequentially on every subclip that lands in the `james-media-subclips` bucket. Results are written to the existing `assets` row for that subclip.

| Step | Model | Input | Output Columns |
|------|-------|-------|----------------|
| 1 | `local-mlx/whisper-turbo` | 30s WAV audio | `transcript` |
| 2 | `nvidia/llama-3.2-90b-vision-instruct` | keyframe | `ocr_text` |
| 3 | `nvidia/llama-3.2-90b-vision-instruct` | keyframe | `scene_description`, `content_tags` |
| 4 | `nvidia/llama-3.2-11b-vision-instruct` | keyframe | `ai_content_assessment` |
| 5 | `nvidia/llama-guard-4-12b` | keyframe | `content_safety_rating` |
| 6 | `nvidia/llama-3.3-70b-instruct` | prior results | `content_summary`, `content_category`, `content_mood`, `content_rating`, `searchable_keywords` |
| 7 | `nvidia/nv-embed-v1` (4096-dim) | summary + scene + OCR + transcript + keywords | `text_embedding_created_at`, `text_embedding_model` + **Qdrant point** |

All calls are sequential (not parallel) and cached on the same assets row keyed by subclip `asset_id`.

### Reliability + observability

Every inference call (whisper, vision-90b, vision-11b, llama-guard, summary-70b, embed, Qdrant upsert, VAST upsert) goes through a central retry+timing wrapper (`_call_with_retry_and_timing()` in `main.py`):

- **6 attempts** per call with exponential backoff `[5, 15, 30, 60, 120]s` plus a few seconds of random jitter so parallel subclip workers don't retry in lockstep and re-hammer the endpoint.
- **Per-call timing log lines** of the form:
  ```
  [timing] vision-90b-ocr OK in 12.34s (total 14.56s, attempt 1/6)
  [timing] summary-70b attempt 1/6 FAIL in 2.10s (RuntimeError: Inference 429: …); backoff 5.4s
  [timing] summary-70b OK in 3.21s (total 10.72s, attempt 2/6)
  ```
  plus a final `total handler time: …` at the end of each subclip. `grep [timing]` the function logs for a CSV-ish view of latency per step.

This replaced an earlier 3-attempt/flat-60s retry that silently dropped subclip analyses when the shared endpoint throttled under parallel load.

## Semantic search

The last step of `subclip-ai-analyzer` concatenates every text output from
the prior 6 inference calls into a single passage and embeds it via
`/v1/embeddings` on the shared inference endpoint. The resulting 4096-dim
vector (from `nvidia/nv-embed-v1`, NVIDIA's flagship general-purpose
embedder) is upserted as a point in a Qdrant collection, with a payload
that carries everything the UI needs to render a hit (summary, category,
mood, rating, keywords, s3_path, the exact passage that was embedded).

No separate indexing worker — embedding happens inline at the tail of the
function so the index is always consistent with the assets table. Qdrant
point IDs equal the `asset_id`, making retries idempotent.

### Components

| Piece | Where it lives | Role |
|---|---|---|
| Embedding at write time | `functions/analysis/subclip-ai-analyzer/main.py` (step 10) | Builds passage + `POST /v1/embeddings` (`input_type=passage`) + Qdrant upsert |
| Vector DB | `qdrant/qdrant:v1.12.4` container (see `docker-compose.yml`) | Cosine similarity search on 4096-dim vectors; persistent volume `qdrant_data` |
| Query endpoint | `webapp/backend/app.py` — `GET /api/semantic-search?q=...&limit=N` | Embeds query (`input_type=query`) + Qdrant top-K + returns payloads |
| Video proxy | `webapp/backend/app.py` — `GET /api/video?path=s3://...` | Range-enabled S3 proxy so `<video>` seeking works across the internal VAST S3 endpoint |
| Search UI | `webapp/frontend/src/pages/SearchPage.jsx` (route `/search`) | Query box, ranked results with score %, expandable "text the match was based on", inline `<video>` player per hit |

### Bring Qdrant up

The `docker-compose.yml` already declares a `qdrant` service with a
persistent volume. On the deployment host:

```bash
docker compose up -d qdrant
curl -s http://localhost:6333/collections   # should list "subclips" after first ingest
```

Qdrant needs to be reachable from both:

1. **The DataEngine function** — so set `qdrant.url` in `config/config.json`
   to a hostname/IP that the Kubernetes function pods can reach (e.g.
   `http://10.143.11.91:6333`).
2. **The webapp backend container** — which can use the Docker-internal
   hostname (`http://qdrant:6333`) since it's on the same compose network.

### Seed a demo corpus

To populate the catalog with ~20 diverse clips (cooking, wildlife, space,
sports, music, travel, etc.) for live-demoing search:

```bash
scripts/seed-demo-corpus.sh
```

Each clip is downloaded via `yt-dlp` (first result of a topic-specific
search), trimmed to 90s, transcoded to 480p @ 400kbps, and uploaded to
`s3://james-media-catalog/` via the `.91` host. Tolerates per-query
failures — rerun the script and it resumes from wherever it left off.

### Try it

```bash
# Web UI
open http://10.143.11.91:3001/search

# Or via curl
curl -s 'http://10.143.11.91:3001/api/semantic-search?q=peaceful+beach&limit=3' \
  | python3 -m json.tool
```

## Documentation

- **[Pre-Ingest Pipeline — Phases 1–3](docs/INGEST_PIPELINE.md)** — QC inspector, AI
  clipper, media packager + C2PA signing. Living doc with current build state, library
  layout under `shared/ingest/`, runtime config system (`function_configs` + `/settings`),
  and the full table schemas. **Start here for the pre-ingest stages.**
- [Function Reference](docs/FUNCTIONS.md) — Detailed docs for all 11 functions + reconciler
- [Schema Reference](docs/SCHEMA.md) — All table schemas + Trino query examples
- [Webapp Guide](docs/WEBAPP.md) — API endpoints and frontend pages (incl. `/packages`, `/settings`)
- [Deployment Guide](docs/DEPLOYMENT.md) — Building, deploying, and verifying
