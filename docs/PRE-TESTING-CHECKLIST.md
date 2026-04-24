# Pre-Testing Checklist

Everything needed before we can start testing the media catalog system.

---

## 1. VAST Cluster Credentials

Edit `config/config.json` with real values for:

- [ ] `vast.endpoint` — VAST VIP or IP (e.g. `http://172.200.202.1`)
- [ ] `vast.access_key` — VAST access key
- [ ] `vast.secret_key` — VAST secret key
- [ ] `vast.bucket` — VAST bucket name (default: `media-catalog`)
- [ ] `s3.endpoint` — same VIP as `vast.endpoint` (S3 API)
- [ ] `s3.access_key` — same as `vast.access_key`
- [ ] `s3.secret_key` — same as `vast.secret_key`

## 2. VAST Bucket & Schema

- [ ] Bucket `media-catalog` exists on the VAST cluster (or whatever `vast.bucket` is set to)
- [ ] Schema `provenance` exists in that bucket (tables are auto-created on first write, but the schema must exist)

## 3. Trino Connectivity

- [ ] `trino.host` — Trino host IP or `localhost` if running locally via Docker Compose
- [ ] `trino.port` — Trino port (default: 8080)
- [ ] Trino has the VAST connector configured for the `vast` catalog
- [ ] Trino can reach the VAST cluster endpoint

## 4. VAST Big Catalog (for Reconciler)

- [ ] VAST Big Catalog feature is **enabled** on the cluster
- [ ] The Big Catalog Trino table path is correct in `catalog.big_catalog_table` (default: `vast-big-catalog-bucket/vast_big_catalog_schema`)
- [ ] Trino can query: `SELECT count(*) FROM vast."vast-big-catalog-bucket/vast_big_catalog_schema".vast_big_catalog_table WHERE element_type = 'FILE'`
- [ ] `catalog.media_search_path` matches the VAST path prefix where media files live (default: `/media-catalog`)

## 5. VAST Audit Log (for Reconciler)

- [ ] VAST Audit Logging is **enabled** on the cluster
- [ ] The Audit Log Trino table path is correct in `catalog.audit_log_table` (default: `vast-audit-log-bucket/vast_audit_log_schema`)
- [ ] Trino can query: `SELECT count(*) FROM vast."vast-audit-log-bucket/vast_audit_log_schema".vast_audit_log_table WHERE time >= current_timestamp - interval '1' hour`
- [ ] Audit log is capturing events for the `media-catalog` bucket/path (confirm with a test file PUT + query)

## 6. GPU Host (for audio-analyzer + clip-embedder)

Edit `config/config.json` → `gpu` section:

- [ ] `gpu.host` — IP address of GPU host with NVIDIA GPU
- [ ] `gpu.user` — SSH username on GPU host
- [ ] `gpu.password` — SSH password on GPU host
- [ ] `gpu.deploy_path` — path on GPU host to deploy containers (e.g. `/home/user/media-catalog`)
- [ ] GPU host has NVIDIA drivers installed
- [ ] GPU host has `nvidia-container-toolkit` installed
- [ ] GPU host has Docker installed
- [ ] GPU host is reachable from Docker build host (SSH + Docker commands)

## 7. Sample Video Files

- [ ] At least 3-5 video files uploaded to VAST at the `media_search_path` (e.g. `/media-catalog/...`)
- [ ] Files should be in a hierarchy with meaningful directory names (territory, company, etc.) to test path parsing
- [ ] At least 2 files that are near-duplicates (same content, different encoding) to test hash matching
- [ ] At least 1 file with faces visible to test face detection
- [ ] At least 1 file with spoken audio to test language detection
- [ ] At least 1 file longer than 30 seconds to test video subclipping (will be split into 30s chunks)

## 8. Docker Host

- [ ] Docker installed on the machine that will build/run containers
- [ ] Docker Compose installed
- [ ] Network access from Docker host to VAST cluster VIP
- [ ] At least 16 GB RAM available for all containers
- [ ] Ports 3000 (webapp) and 8080 (Trino) available

## 9. Reconciler Testing Prerequisites

These are needed specifically to test the catalog-reconciler service:

- [ ] VAST Big Catalog is populated (files exist at the `media_search_path`)
- [ ] VAST Audit Log is actively recording events
- [ ] At least one asset has been processed by the function pipeline (row exists in `assets` table)
- [ ] Ability to move a file via S3 (CopyObject + DeleteObject), NFS (rename), or SMB (rename) to test move detection
- [ ] Ability to delete a file via S3/NFS/SMB to test delete detection

---

## Quick Verification Commands

Once config is filled in, run these to verify connectivity:

```bash
# 1. Check Trino is up
curl http://TRINO_HOST:8080/v1/info

# 2. Check VAST DB connectivity (from a container with vastdb installed)
python3 -c "
import vastdb
session = vastdb.connect(endpoint='http://VAST_VIP', access='ACCESS_KEY', secret='SECRET_KEY')
print('Connected to VAST DB')
"

# 3. Check Big Catalog is queryable
curl -X POST http://TRINO_HOST:8080/v1/statement \
  -H "X-Trino-User: trino" \
  -d 'SELECT count(*) FROM vast."vast-big-catalog-bucket/vast_big_catalog_schema".vast_big_catalog_table WHERE element_type = '\''FILE'\'''

# 4. Check Audit Log is queryable
curl -X POST http://TRINO_HOST:8080/v1/statement \
  -H "X-Trino-User: trino" \
  -d 'SELECT count(*) FROM vast."vast-audit-log-bucket/vast_audit_log_schema".vast_audit_log_table WHERE time >= current_timestamp - interval '\''1'\'' hour'

# 5. Check GPU host is reachable (if applicable)
ssh GPU_USER@GPU_HOST "nvidia-smi"
```
