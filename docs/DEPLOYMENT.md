# Deployment Guide

## Prerequisites

- Docker and Docker Compose
- VAST Data cluster with S3 API, Trino connector, and audit logging enabled
- Network access from Docker host to VAST cluster
- GPU host with NVIDIA drivers + nvidia-container-toolkit (for audio-analyzer, clip-embedder)

## Configuration

Edit `config/config.json` with your environment details:

```json
{
  "vast": {
    "endpoint": "http://VASTDB_VIP_OR_IP",
    "access_key": "VASTDB_ACCESS_KEY",
    "secret_key": "VASTDB_SECRET_KEY",
    "bucket": "james-db",
    "schema": "media-catalog"
  },
  "s3": {
    "endpoint": "http://S3_VIP_OR_HOSTNAME",
    "access_key": "S3_ACCESS_KEY",
    "secret_key": "S3_SECRET_KEY"
  },
  "trino": {
    "host": "localhost",
    "port": 8080,
    "catalog": "vast",
    "schema": "james-db/media-catalog"
  },
  "catalog": {
    "big_catalog_table": "vast-big-catalog-bucket/vast_big_catalog_schema",
    "audit_log_table": "vast-audit-log-bucket/vast_audit_log_schema",
    "media_search_path": "/james-media-catalog"
  },
  "reconciler": {
    "interval_seconds": 1800,
    "lookback_seconds": 2100
  },
  "subclip": {
    "duration_seconds": 30,
    "max_subclips": 100
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

### Config Sections

| Section | Purpose |
|---|---|
| `vast` | VAST DB connection (vastdb library) — endpoint, credentials, bucket, schema name |
| `s3` | S3 API access (boto3) — file download/upload (can be a different cluster/VIP than VAST DB) |
| `trino` | Trino SQL engine — host, port, catalog, schema path for SQL queries |
| `catalog` | VAST Big Catalog + Audit Log Trino table paths for reconciler |
| `reconciler` | Reconciler interval (1800s = 30 min) and lookback window (2100s = 35 min) |
| `subclip` | Video subclipping: clip duration (30s) and max subclips cap (100) |
| `gpu` | GPU host for deploying ML containers (audio-analyzer, clip-embedder) |

> **Note:** The `vast` and `s3` sections can use different endpoints and credentials. In the current lab environment, S3 file access goes to VAR201 (dataengine-sandbox) while VAST DB table operations go to VAR202 (172.200.202.1). Migrated from VAR204 on 2026-04-14 after VAR204 was rebuilt.

---

## One-Time VAST DB Setup

Before running any functions, the VAST DB bucket, schema, and tables must exist. Each function's `init()` will auto-create missing tables on startup, but if you're starting from a wiped cluster, run this setup script to create everything at once.

### Prerequisites

- Python 3.9+ with `vastdb` and `pyarrow` installed
- Network access to the VAST DB endpoint
- Valid VAST DB credentials with create permissions

### Setup Script

SSH into a host with `vastdb` installed (or run locally) and execute:

```bash
python3 scripts/setup_vastdb.py
```

This script:
1. Connects to VAST DB at the endpoint in `config/config.json`
2. Verifies the bucket exists (default: `james-db`)
3. Creates the `media-catalog` schema if missing
4. Creates all 10 tables with correct PyArrow schemas

### Manual Setup (if no script runner available)

```python
import vastdb
import pyarrow as pa

session = vastdb.connect(
    endpoint="http://172.200.202.1",      # VAST DB endpoint
    access="VASTDB_ACCESS_KEY",
    secret="VASTDB_SECRET_KEY",
)

with session.transaction() as tx:
    bucket = tx.bucket("james-db")
    schema = bucket.create_schema("media-catalog")  # or bucket.schema("media-catalog")

    # Then create each table — see shared/schemas.py for full schema definitions
    # schema.create_table("assets", ASSETS_SCHEMA)
    # schema.create_table("relationships", RELATIONSHIPS_SCHEMA)
    # ... etc for all 10 tables
```

### Tables Created

| # | Table | Columns | Written By |
|---|---|---|---|
| 1 | `assets` | 152 | All 9 functions + reconciler |
| 2 | `relationships` | 6 | hash-comparator, video-subclip, reconciler |
| 3 | `hash_matches` | 8 | hash-comparator |
| 4 | `talent_music` | 9 | face-detector, audio-analyzer |
| 5 | `semantic_embeddings` | 7 | clip-embedder |
| 6 | `gdpr_personal_data` | 8 | face-detector |
| 7 | `syndication_records` | 8 | graph-analyzer |
| 8 | `production_entities` | 8 | graph-analyzer |
| 9 | `version_history` | 7 | graph-analyzer |
| 10 | `asset_moves` | 12 | catalog-reconciler |

### Verifying Setup

```python
import vastdb

session = vastdb.connect(endpoint="http://172.200.202.1", access="...", secret="...")
with session.transaction() as tx:
    schema = tx.bucket("james-db").schema("media-catalog")
    for t in schema.tables():
        print(f"  {t.name}")
```

Expected output: all 10 tables listed above.

### Wiping and Recreating

If the cluster is wiped, simply re-run the setup script. All table schemas are defined in `shared/schemas.py` — the script reads those definitions and creates tables with the exact same column types. No data migration is needed since all data is re-generated from video files via the function pipeline.

---

## Building & Running the Webapp

```bash
# Build and start the webapp (Flask + Trino)
docker compose up --build -d

# Check health
curl http://localhost:3000/api/personas

# View logs
docker compose logs -f
```

The webapp runs on:
- **Port 3000** — Web UI and API
- **Port 8080** — Trino query engine

---

## Building Function Container Images

```bash
# Build the shared base image first
docker compose -f docker-compose-functions.yml build base

# Build all function images
docker compose -f docker-compose-functions.yml build

# Build a specific function
docker compose -f docker-compose-functions.yml build hash-comparator
```

### Container GPU Requirements

| Container | GPU | ML Libraries | Notes |
|---|---|---|---|
| metadata-extractor | No | — | ffprobe only |
| hash-generator | No | imagehash | CPU pHash |
| face-detector | Optional | face_recognition (dlib) | HOG model runs on CPU; CNN mode benefits from CUDA |
| audio-analyzer | **Yes** | torchaudio, speechbrain | SpeechBrain ECAPA-TDNN language detection |
| video-subclip | No | — | ffmpeg stream copy + S3 upload |
| clip-embedder | **Yes** | torch, torchvision, open_clip_torch | OpenCLIP ViT-B-32 forward pass |
| synthetic-detector | No | — | ffprobe metadata scan |
| hash-comparator | No | — | DB-only hash math |
| graph-analyzer | No | — | DB-only graph traversal |
| catalog-reconciler | No | — | Trino queries only |

**GPU containers** (`audio-analyzer`, `clip-embedder`) should be deployed to a host with NVIDIA GPU and `nvidia-container-toolkit` installed. Set the GPU host details in `config.gpu`.

---

## VAST Serverless Function Registration

Each function container implements the `init(ctx)` / `handler(ctx, event)` interface for VAST's serverless function engine. Register each container image with VAST and configure triggers for video file PUT events.

### Trigger Chain Order (9 containers, 3 layers)

Functions must be chained in dependency order:

**Layer 0 — Foundation (trigger directly from PUT, run in parallel):**
- `media-catalog-metadata-extractor` — ffprobe metadata extraction (also extracts embedded subclip tags)
- `media-catalog-hash-generator` — SHA-256 + perceptual hash
- `media-catalog-face-detector` — face detection + GDPR person tracking (UC05, UC20)
- `media-catalog-audio-analyzer` — audio fingerprint + language detection (UC05, UC18) **[GPU]**
- `media-catalog-video-subclip` — splits long videos into 30s subclips, uploads each to S3 (triggers full pipeline per subclip)

**Layer 1 — Analysis (trigger after Layer 0, run in parallel):**
- `media-catalog-clip-embedder` — CLIP ViT-B-32 semantic embeddings (UC15) **[GPU]**
- `media-catalog-synthetic-detector` — AI-generated content detection (UC13)
- `media-catalog-hash-comparator` — all hash comparisons in one pass (UC02, UC03, UC06, UC08, UC09)

**Layer 2 — Graph (trigger after hash-comparator + synthetic-detector):**
- `media-catalog-graph-analyzer` — all 16 graph-based analyses in one pass (UC01, UC04, UC07, UC10-UC12, UC14, UC16-UC17, UC19, UC21-UC26)

### Use Case Coverage

| Container | Use Cases Covered |
|---|---|
| metadata-extractor | Foundation metadata + subclip tag extraction |
| hash-generator | Foundation hashes |
| face-detector | UC05 (face detection), UC20 (GDPR face tracking) |
| audio-analyzer | UC05 (audio fingerprint), UC18 (language detection) |
| video-subclip | Foundation subclipping (enables per-segment analysis) |
| clip-embedder | UC15 (re-use discovery) |
| synthetic-detector | UC13 (synthetic content) |
| hash-comparator | UC02, UC03, UC06, UC08, UC09 |
| graph-analyzer | UC01, UC04, UC07, UC10-UC12, UC14, UC16-UC17, UC19, UC21-UC26 |

### Handler Interface

Each function receives the S3 path of the new video file via `event.body`:

```python
def init(ctx):
    # Called once when pipeline loads
    # Initialize clients, store in ctx.user_data

def handler(ctx, event):
    # Called per video file
    s3_path = event.body.decode("utf-8").strip()
    # Process and write to VAST DB
```

---

## Catalog Reconciler Service

The catalog reconciler is a **long-running service** (not a serverless function) that detects file moves and deletes that happen after initial ingestion.

### How It Works

Every 30 minutes:

1. **Queries VAST Big Catalog** via Trino — gets all current files on the cluster
2. **Reads the assets table** from VAST DB — gets all tracked assets
3. **Diffs the two sets:**
   - **Missing** (in assets but not catalog) → investigate via VAST Audit Log
   - **New** (in catalog but not assets) → ignore (PUT pipeline trigger handles these)
   - **Present** (in both) → update `last_reconciled_at` timestamp
4. **Investigates missing assets** by querying the VAST Audit Log via Trino:
   - Searches for RENAME/move RPCs across S3, NFS, and SMB protocols
   - Searches for DELETE RPCs across all 3 protocols
   - If nothing found → logs ERROR (audit log integration may be broken)

### Move Handling

When a RENAME/move is detected:
- Old asset row is marked `status = "moved"`
- New asset row is created with:
  - New `asset_id` (MD5 of new path)
  - All metadata carried over from old row
  - Path-dependent columns re-derived (territory, licensee, company, crew, recipient, date)
  - `original_s3_path` preserved for lineage tracking
- Relationship edges are duplicated from old to new `asset_id`
- Event recorded in `asset_moves` table

### Delete Handling

When a DELETE is detected:
- Asset row is marked `status = "deleted"`, `deleted_at` and `deleted_by` set
- Event recorded in `asset_moves` table

### Audit Log Protocol Coverage

| Protocol | Move RPCs | Delete RPCs |
|---|---|---|
| NFS | `RENAME` → `rename_path` + `rename_name` | `REMOVE`, `RMDIR` |
| SMB | `RENAME` → `smb_rename_struct.path` | `CLOSE` with `smb_delete_on_close = true` |
| S3 | `CopyObject` + `DeleteObject` | `DeleteObject` |

### Running the Reconciler

```bash
# Build
docker build -t media-catalog-reconciler services/catalog-reconciler/

# Run
docker run -d --name catalog-reconciler \
  -v $(pwd)/config/config.json:/app/config/config.json:ro \
  -v $(pwd)/shared:/app/shared:ro \
  media-catalog-reconciler
```

### VAST Data Sources (Trino)

| Table | Purpose |
|---|---|
| `vast."vast-big-catalog-bucket/vast_big_catalog_schema".vast_big_catalog_table` | Live file inventory (name, parent_path, size, mtime) |
| `vast."vast-audit-log-bucket/vast_audit_log_schema".vast_audit_log_table` | Protocol-level RPC event log (49 columns) |

---

## VAST DB Table Summary

Tables are created by the one-time setup script (see above) or auto-created by each function's `init()` on first run. The system uses:

| Table | Rows | Written By |
|---|---|---|
| `assets` | 1 per asset (~150 columns) | All 9 functions + reconciler |
| `relationships` | Many per asset | hash-comparator, video-subclip, reconciler |
| `hash_matches` | Many per asset | hash-comparator |
| `talent_music` | Many per asset | face-detector, audio-analyzer |
| `semantic_embeddings` | Many per asset | clip-embedder |
| `gdpr_personal_data` | Many per asset | face-detector |
| `syndication_records` | Many per asset | graph-analyzer |
| `production_entities` | Many per asset | graph-analyzer |
| `version_history` | Many per asset | graph-analyzer |
| `asset_moves` | 1 per move/delete event | catalog-reconciler |

See `docs/SCHEMA.md` for the full schema reference.

---

## Verifying the Deployment

```bash
# Check webapp is serving
curl http://localhost:3000/api/stats

# Check Trino connectivity
curl -X POST http://localhost:8080/v1/statement \
  -H "X-Trino-User: trino" \
  -d 'SELECT count(*) FROM vast."james-db/media-catalog".assets'

# Check a specific use case endpoint
curl http://localhost:3000/api/usecases/1/data

# Check VAST Big Catalog is queryable
curl -X POST http://localhost:8080/v1/statement \
  -H "X-Trino-User: trino" \
  -d 'SELECT count(*) FROM vast."vast-big-catalog-bucket/vast_big_catalog_schema".vast_big_catalog_table WHERE element_type = '\''FILE'\'''

# Check VAST Audit Log is queryable
curl -X POST http://localhost:8080/v1/statement \
  -H "X-Trino-User: trino" \
  -d 'SELECT count(*) FROM vast."vast-audit-log-bucket/vast_audit_log_schema".vast_audit_log_table WHERE time >= current_timestamp - interval '\''1'\'' hour'

# Check reconciler logs
docker logs catalog-reconciler --tail 50
```
