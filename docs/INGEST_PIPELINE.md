# Pre-Ingest Pipeline — Plan & Current State

> Living document. Updated as phases land. See the "Current state" section
> for exactly what's done, what's in flight, and what's next — use that
> to pick up work across context boundaries.

## Purpose

Add a set of stages **before** raw video hits `s3://james-media-catalog`
(which already fires the existing 8-function provenance fan-out). The
new stages do:

1. **QC** — is this file even usable? (dead audio/video, codec sanity,
   resolution checks, loudness, VFR, interlacing — all non-AI)
2. **AI clip selection** *(later phase)* — given a prompt, extract clips
   where people are fighting, a goal is scored, a car drives by, etc.
3. **Packaging** *(later phase)* — transcode to delivery formats, build
   a JSON manifest with licensing fields, emit to a delivery bucket.
4. **Hand-off** — copy approved clips into `james-media-catalog` with
   S3 object metadata tags so the existing pipeline stamps them with
   source/clip/package IDs.

Design constraint from the user: **most "building blocks" should be
reusable library primitives, not separately deployed DataEngine
functions.** Only things that genuinely need their own trigger/scaling
profile become functions. Everything else is in `shared/ingest/`.

Second design constraint: **all thresholds, paths, codec allow-lists,
and other numbers are runtime-editable via a GUI — never hardcoded.**

## High-level architecture

```
┌───────────────────────────────────────────────────────────────────────┐
│   NEW BUCKETS (created 2026-04-21)                                    │
│                                                                       │
│     s3://james-media-inbox/         ← raw uploads (entry point)       │
│     s3://james-media-qc-passed/     ← QC-approved source videos       │
│     s3://james-media-qc-failed/     ← QC-failed (quarantine)          │
│                                                                       │
│   EXISTING BUCKETS (unchanged)                                        │
│     s3://james-media-catalog/       ← feed to 8-function fan-out      │
│     s3://james-media-subclips/      ← 30s auto-subclips               │
└──────────────────────────┬────────────────────────────────────────────┘
                           │
                           ▼
┌───────────────────────────────────────────────────────────────────────┐
│   PRE-INGEST STAGES                                                   │
│                                                                       │
│   inbox PUT  ─▶  james-qc-inspector  (DataEngine fn, PHASE 1)         │
│                   ├─ reads knobs from `function_configs` table        │
│                   ├─ runs ffprobe/ffmpeg detectors                    │
│                   ├─ writes row to `source_videos` table              │
│                   └─ moves file → qc-passed OR qc-failed              │
│                                                                       │
│   qc-passed PUT  ─▶  james-ai-clipper  (PHASE 2)                      │
│   clips ready  ─▶    james-media-packager  (PHASE 3)                  │
│                                                                       │
│   Hand-off: packager copies renditions to james-media-catalog         │
│   with x-amz-meta-source-id / clip-id / package-id S3 tags.           │
│   Existing metadata-extractor is patched (Phase 3) to read those      │
│   tags and populate new columns on the `assets` table.                │
└───────────────────────────────────────────────────────────────────────┘
```

## Shared library — `shared/ingest/`

The reusability story. Every function composes primitives from here.

| Module | Status | Purpose |
|---|---|---|
| `shared/config.py` | ✅ built | Config loader: DB → coerced value, with caching, module-declared defaults, and `register_defaults(scope, schema)` |
| `shared/ingest/__init__.py` | ✅ built | Package marker + docstring explaining the rules |
| `shared/ingest/qc.py` | 🟡 schema done, impls stubbed | `probe_metadata`, `detect_silence/black/freeze`, `measure_loudness`. Declares 27 config knobs under scope `qc-inspector`. |
| `shared/ingest/qc_policy.py` | ⏳ not yet | Pure function `evaluate_qc(probe, detections, cfg) → (status, reason)` |
| `shared/ingest/ffprobe.py` | ⏳ not yet | Probe wrapper, separate so non-QC callers can import just this |
| `shared/ingest/clips.py` | ⏳ not yet | `cut_clip(src, start, end, out, spec)`, `merge_matching_shots`, `constrain_clips` |
| `shared/ingest/scene.py` | ⏳ not yet | `detect_scenes`, `extract_keyframe(s)` |
| `shared/ingest/vision.py` | ⏳ not yet | AI stuff — only when we get to Phase 2 (AI clipper). Wraps inference endpoint. |
| `shared/ingest/transcode.py` | ⏳ not yet | Preset-driven transcoding |
| `shared/ingest/s3_helpers.py` | ⏳ not yet | `copy_with_tags`, `move_to_bucket` |
| `shared/ingest/manifest.py` | ⏳ not yet | `build_package_manifest`, `normalize_licensing` |

### Library rules

1. Each module that has runtime-editable knobs **calls
   `register_defaults(CONFIG_SCOPE, CONFIG_SCHEMA)` at import time.**
   That makes its knobs discoverable by the seed script and the
   `/settings` UI with zero manual registration.
2. Library modules **do not** import from DataEngine functions — this is
   the shared layer.
3. Library modules are **pure** unless explicitly documented — that is,
   they take paths as input, return structured data, and don't hit S3
   or VAST DB unless that's their single job.

## New VAST DB tables

| Table | Status | Purpose |
|---|---|---|
| `function_configs` | ✅ created + seeded (65 rows across 4 scopes: `qc-inspector`, `ai-clipper`, `packager`, `provenance`) | One row per editable knob. Seeded by `scripts/seed_function_configs.py` which walks every module's `CONFIG_SCHEMA`. |
| `source_videos` | ✅ created + in use | The "spine" of a pre-ingest run. One row per raw upload. Updated through QC → clip-extraction → packaging. 50 columns. |
| `extracted_clips` | ✅ created + in use | Many per source. AI-selected clip timestamps + vision verdict + prompt used. |
| `delivery_packages` | ✅ created + in use | One per packaged delivery. Licensing + C2PA signer + signed counts. |
| `package_renditions` | ✅ created + in use | Many per package. One row per (clip × rendition preset) pair. Includes C2PA manifest label, signer, sha256, size. |

Existing tables (unchanged for now; Phase 3 adds 3 columns to `assets`):
`assets`, `relationships`, `hash_matches`, `talent_music`,
`semantic_embeddings`, `gdpr_personal_data`, `syndication_records`,
`production_entities`, `version_history`, `asset_moves`.

## Config system

The whole pipeline's runtime knobs live in **one generic table**
(`function_configs`) that every function reads from. Rows are keyed by
`(scope, key)`. Values are JSON-encoded strings so the same table holds
ints/floats/bools/arrays/dicts without a schema change.

Schema columns:
```
config_id       (string PK, = "scope:key")
scope           (e.g. "qc-inspector", "subclipper", "global")
key
value           (JSON-encoded)
value_type      (int|float|bool|string|duration_seconds|percent|db|json)
default_value   (JSON-encoded factory default)
description
min_value, max_value  (optional UI range bounds, JSON-encoded)
ui_group, ui_order    (for /settings GUI layout)
updated_at, updated_by
```

### Read path (in a function)

```python
from shared.config import load_config

cfg = load_config(scope="qc-inspector")
cfg.get_float("black_frame_max_ratio_fail")       # → 0.5
cfg.get_percent("black_frame_max_ratio_warn")     # → 0.1
cfg.get_duration("silence_min_run_seconds")       # → 1.0
cfg.get_list("video_codec_allowlist")             # → ["h264","hevc","vp9","av1"]
cfg.get_bool("require_video_stream")              # → True
cfg.snapshot()                                    # → dict of everything, for logs
```

Behaviors:
- 60s per-pod cache (configurable via `CONFIG_CACHE_TTL_SECONDS`)
- Missing row → falls back to the module-declared default
- Undeclared + missing key → raises `KeyError` (typos are loud)
- DB unreachable → logs a warning, returns last known values

### Write path

Either:
- `scripts/seed_function_configs.py` — walks every
  `CONFIG_SCHEMA` and inserts any missing rows. Idempotent. Safe to
  rerun whenever a module ships a new knob.
- Webapp `/api/configs/<scope>/<key>` PUT — user edits from `/settings` GUI
- Webapp `/api/configs/<scope>/<key>/reset` POST — reset to default

## Knob declaration — example

```python
# shared/ingest/qc.py
from shared.config import register_defaults

CONFIG_SCOPE = "qc-inspector"
CONFIG_SCHEMA = [
    {"key": "black_frame_min_run_seconds",
     "type": "duration_seconds", "default": 1.0,
     "min": 0.1, "max": 60.0,
     "group": "Black frames", "order": 10,
     "description": "Minimum continuous run to count as a black-frame event."},
    {"key": "black_frame_max_ratio_fail",
     "type": "percent", "default": 0.50,
     "min": 0.0, "max": 1.0,
     "group": "Black frames", "order": 40,
     "description": "Fail (quarantine) if black-frame runs exceed this fraction."},
    # ... 25 more entries
]
register_defaults(CONFIG_SCOPE, CONFIG_SCHEMA)
```

## DataEngine functions

Only things that genuinely need their own trigger become DataEngine
functions. Thin glue — load primitives, compose, write DB, emit next event.

| Function | Status | Trigger | Composes |
|---|---|---|---|
| `james-qc-inspector` | ✅ Phase 1 (revision 6 deployed) | PUT on `james-media-inbox/*` | `ffprobe`, `qc.*`, `qc_policy`, `s3_helpers`, `tables.upsert_source_video` |
| `james-ai-clipper` | ✅ Phase 2 (revision 3 deployed) | PUT on `james-media-qc-passed/*` | `scene.*`, `vision.*`, `clips.*`, `tables.upsert_extracted_clip` |
| `james-media-packager` | ✅ Phase 3 (revision 4 deployed) | PUT on `james-media-clips/*/_ready.json` | `transcode.*`, `thumbnail.*`, `manifest.*`, `provenance.*` (C2PA sign), `s3_helpers`, `tables.upsert_delivery_package`, `tables.upsert_package_rendition` |
| `james-subclipper` | ⏳ Phase 4 | Invoked directly (invoke API) | `clips.cut_clip` — generic reusable subclipper |

Existing functions (`metadata-extractor`, `hash-generator`, `keyframe-extractor`, `video-subclip`, `audio-analyzer`, `synthetic-detector`, `hash-comparator`, `graph-analyzer`, `subclip-ai-analyzer`) — unchanged for now. Phase 3 adds a tiny patch to `metadata-extractor` that reads `x-amz-meta-source-id / clip-id / package-id` tags off the incoming S3 object and stores them as columns on the `assets` row.

## Phased build plan

### Phase 0 — Config infrastructure (this work)

Goal: runtime-editable knobs, ready for any future function to use.

- [x] `FUNCTION_CONFIGS_SCHEMA` in `shared/schemas.py`
- [x] `shared/config.py` loader (cache, type coercion, `register_defaults`, `snapshot`, `invalidate_cache`)
- [x] `shared/ingest/__init__.py` + `shared/ingest/qc.py` (schema-only; 27 knobs declared)
- [x] `scripts/seed_function_configs.py` — idempotent, discovers via `iter_registered_schemas()`
- [x] Table created in VAST DB; 27 rows seeded (`scope="qc-inspector"`)
- [x] 3 new S3 buckets created on `.91`: `james-media-inbox`, `james-media-qc-passed`, `james-media-qc-failed`
- [🟡] `/api/configs`, `/api/configs/<scope>`, `/api/configs/<scope>/<key>` (PUT), `/api/configs/<scope>/<key>/reset` (POST) — **code written in `webapp/backend/app.py` but not yet deployed to .91**. Webapp `requirements.txt` needs `vastdb` + `ibis-framework` added.
- [ ] `/settings` page in webapp frontend — schema-driven, widgets per `value_type`, groups, "Reset to default", "Last modified by/at"

### Phase 1 — QC inspector ✅ COMPLETE (v5 deployed 2026-04-22)

Goal: raw uploads land in `inbox`, get audited, and end up in `qc-passed` or `qc-failed` with a `source_videos` row recording what happened.

- [x] `shared/ingest/ffprobe.py` — `probe_metadata(path) → dict` wrapping ffprobe
- [x] `shared/ingest/qc.py` — `detect_silence`, `detect_black_frames`, `detect_freeze_frames`, `measure_loudness`, `detect_vfr`, `detect_interlaced`
- [x] `shared/ingest/qc_policy.py` — pure `evaluate_qc(probe, detections, cfg) → {status, reason, issues, ratios}`
- [x] `shared/ingest/s3_helpers.py` — `parse_s3_path`, `copy_object`, `move_object`, `download_to_temp`
- [x] `SOURCE_VIDEOS_SCHEMA` in `shared/schemas.py` (50 columns)
- [x] `shared/ingest/tables.py` — `upsert_source_video(session, bucket, schema, fields)`
- [x] `james-qc-inspector` function (revision 5 live):
      - init() loads config snapshot, logs
      - handler() seeds row → downloads → probes → runs detectors (each checkpointed) → evaluate_qc → upsert → move
      - `-nostdin` + `stdin=DEVNULL` + `timeout=120` on every ffmpeg/ffprobe call
      - Per-detector try/except marks `failed:{detector}` before raising
- [x] Pipeline YAML: `james-inbox-trigger` + `james-qc-inspector` revision 5, max_concurrency 4, timeout 600s
- [x] Smoke test: 320x240 → `failed` moved to qc-failed; 1280x720 → `passed` moved to qc-passed. ~5s end-to-end. VFR detector bug fixed (f-string vs `%` formatting collision).

### Phase 2 — AI clip selection ✅ COMPLETE (v3 deployed 2026-04-22)

Goal: per-clip AI-driven extraction given a natural-language prompt.

- [x] `shared/ingest/scene.py` — ffmpeg `scene` filter; returns (start,end) shot boundaries; respects min/max shot length
- [x] `shared/ingest/vision.py` — wraps OpenAI-compatible inference endpoint; `classify_frame` + `classify_with_escalation` (two-tier 11B→90B)
- [x] `shared/ingest/clips.py` — `merge_matching_shots`, `constrain_clips`, `cut_clip`
- [x] `EXTRACTED_CLIPS_SCHEMA` in `shared/schemas.py` (21 columns incl. frame_scores_json, prompt_source, vision_model)
- [x] `james-ai-clipper` function (revision 3 live): scene detect → per-shot vision classify → merge/constrain → cut + upload → emit `_ready.json` marker
- [x] Prompt sourcing: S3 object metadata `x-amz-meta-clip-prompt` > sidecar JSON (not yet) > `ai-clipper:default_clip_prompt` config
- [x] Two-pass strategy: 11B primary, borderline band (low ≤ conf < high) escalates to 90B; configurable
- [x] Editorial buffer (v4, 2026-04-25): `clip_buffer_pre_seconds` + `clip_buffer_post_seconds` knobs add lead-in / tail-out around the matched span. Applied AFTER constrain — additive on top of `max_clip_seconds`. Clamped to `[0, source_duration]`.
- [x] Smoke test: basketball clip with prompt `"A person handling a basketball"` → 4 shots → shot 0 MATCH 0.90 → 1 clip cut in ~18s

### Phase 3 — Packaging + C2PA provenance + hand-off ✅ COMPLETE (v4 deployed 2026-04-22)

Goal: transcode to delivery formats, **embed C2PA signed provenance in each
rendition**, produce a JSON manifest with licensing, hand approved clips off
to the existing catalog pipeline.

**C2PA is the headline.** Every rendition MP4 carries a cryptographically
signed manifest with 5 assertions:
  - `c2pa.actions.v2` — `c2pa.created` / `c2pa.placed` (AI clip-selection with timespan) / `c2pa.edited` (transcode)
  - `c2pa.creative_work` — source filename + attribution
  - `c2pa.training-mining` — all 4 modes (training, generative training, data mining, inference) set to `notAllowed` by default
  - `com.vast.ai_clip_selection` — vision model + prompt + confidence + source timespan (the regulatory AI-disclosure signal)
  - `c2pa.hash.bmff.v2` — auto-generated BMFF container hash (tamper-evidence)

Signed with a self-signed ES256 dev cert; verifiable via `c2patool` CLI,
Adobe Content Credentials panel, or contentcredentials.org web verifier.

- [x] `c2patool` linux-amd64 binary (v0.9.12) bundled in packager (same pattern as ffmpeg)
- [x] Self-signed X.509 cert + EC prime256v1 private key with C2PA-required extensions (`emailProtection` EKU, `digitalSignature,nonRepudiation` KU); generated via openssl with custom `openssl.cnf`
- [x] `shared/ingest/transcode.py` — preset-driven (4 defaults: `h264-1080p`, `h264-720p`, `proxy-360p`, `hevc-4k` with `min_source_height=2160` gate)
- [x] `shared/ingest/thumbnail.py` — middle-frame JPEG per clip
- [x] `shared/ingest/manifest.py` — JSON sidecar (covers formats that don't embed C2PA) + `build_c2pa_claim_for_rendition` shared by provenance.py
- [x] `shared/ingest/provenance.py` — `sign_rendition(...)` → `c2patool` CLI wrapper; `verify_c2pa(path)` for re-reading
- [x] `DELIVERY_PACKAGES_SCHEMA` (24 cols), `PACKAGE_RENDITIONS_SCHEMA` (24 cols) in schemas.py
- [x] `shared/ingest/tables.py` — `upsert_delivery_package`, `upsert_package_rendition`, `ensure_*_table`
- [x] New bucket: `s3://james-media-deliveries/` on `.91`
- [x] ai-clipper (v3) emits `_ready.json` marker into `s3://james-media-clips/<source_id>/` when done
- [x] Trigger `james-clips-ready-trigger` on `james-media-clips` bucket; packager skips any PUT that isn't a `_ready.json` sentinel
- [x] `james-media-packager` function (revision 4 live): transcode → sign → upload → DB rows; 4 renditions × 1 clip = 4 outputs in ~145s
- [x] Config rows seeded under two new scopes (22 new rows: 12 `packager` + 10 `provenance`)
- [x] Smoke-tested end-to-end: basketball clip → `c2pa signed: 4/4`, manifest label `urn:uuid:9067d200-...`, full assertion tree confirmed via c2patool verify

**What's NOT implemented yet (explicitly deferred):**
- `assets` table: `source_video_id` / `clip_id` / `package_id` columns — the delivery bundle doesn't yet copy renditions INTO `james-media-catalog` for the existing 8-function fan-out to pick up.
- Patch to `metadata-extractor` that reads those S3 metadata tags.
- "Hand-off" step that stitches the delivery back to the provenance pipeline. The packager currently stops at `s3://james-media-deliveries/<package_id>/` — a clean terminal state, just not yet wired to the downstream catalog.

### Phase 2.5 — AI Clipper tab in webapp ✅ DEPLOYED (2026-04-25)

A new `/ai-clipper` page surfaces every source the pre-ingest pipeline
has touched, alongside its AI-extracted clips. Useful to compare the
matched spans against the original at a glance — and to demo the
clip-extraction story without digging through Trino.

- [x] Backend `GET /api/sources` — list of all `source_videos` rows with
      summary fields (qc_status, clip_count, clip_prompt, etc).
- [x] Backend `GET /api/sources/<source_id>` — one source's row + every
      `extracted_clips` row for it, sorted by `clip_index`.
- [x] Frontend `/ai-clipper`:
      - Sidebar list of sources, sortable + filter by filename/prompt/status
      - Detail pane with header (prompt, qc/clip status, durations),
        full-video player streamed from the source's current bucket
        (qc-passed/qc-failed), and a row per clip with mini-player + a
        "position-in-source" timeline strip + confidence chip + vision
        verdict reason
- [x] `/api/video?path=...` already streams from every bucket the pre-ingest
      pipeline touches (added to `STREAMABLE_BUCKETS` earlier)
- [x] Nav link added between Search and Packages

### Phase 3.5 — Webapp UI for packages ✅ COMPLETE (not yet deployed)

- [x] Backend endpoints:
  - `GET /api/packages` — list all packages
  - `GET /api/packages/<id>` — full detail (source + clips + renditions)
  - `GET /api/packages/<id>/manifest` — passthrough of sidecar manifest.json from S3
  - `GET /api/packages/<id>/renditions/<rendition_id>/c2pa` — **live** c2patool verify: downloads rendition, runs c2patool, returns parsed report (active manifest, signature info, every assertion, extracted AI disclosure)
- [x] `c2patool` bundled in the webapp image so the C2PA verify endpoint works out of the box
- [x] Added `james-media-clips`, `james-media-deliveries`, and the 3 pre-ingest buckets to `STREAMABLE_BUCKETS` — the existing `/api/video?path=...` range-enabled proxy serves signed MP4s
- [x] Frontend `/packages` (PackagesPage.jsx) — grid of package cards with source filename, clip/rendition counts, C2PA sign status badge
- [x] Frontend `/packages/<id>` (PackageDetailPage.jsx):
  - Header with source + licensing summary + counts (signed/total)
  - Video player with rendition picker (buttons per preset under each clip)
  - Clip list with timestamps, confidence, vision verdict reason, shot count
  - **C2PA panel** — live-verifies the selected rendition via `/api/packages/.../c2pa`; shows signer/algorithm/cert info, AI disclosure (model+prompt+confidence), the 3-step action chain with software agent names, training-mining policy table, creative work block, and full assertion list
  - Licensing card (rights-cleared-for, restrictions, clearance expiry)
  - Sidecar manifest.json viewer (collapsed; fetches on expand)
  - Link to contentcredentials.org/verify for an independent check
- [x] `/packages` nav link added to the header
- [ ] **Build + deploy on `.91`** — blocked behind user preference (pending .204 cluster migration; deferred for now)

### Phase 4 — Generic reusable subclipper ✅ BUILT (not deployed)

Goal: a stateless "chop this video at these timestamps with these encoding overrides" function, invokable directly from scripts or other workflows without going through S3 triggers.

- [x] `shared/ingest/clips.py::cut_clip(src, start, end, out, …)` — library primitive (already in use by Phase 2 ai-clipper and Phase 3 packager)
- [x] `shared/ingest/subclipper.py` — 7 config knobs under scope `subclipper` (default bucket, stream-copy toggle, codecs, CRF, timeout, max-clips guard)
- [x] `james-subclipper` function (`functions/foundation/subclipper/`) — direct-invoke handler. Event JSON: `{src, out_bucket?, out_prefix?, stream_copy?, clips: [{start, end, name?, width?, height?, crf?, stream_copy?, video_codec?, audio_codec?}]}`. Per-clip overrides beat event-level, which beats config defaults.
- [ ] Build + register + publish the function image (deferred with the webapp deploy)
- [ ] Invoke flow: `vast functions invoke james-subclipper --body '{...}'` returns a structured JSON with `{src, out_bucket, requested, ok, failed, clips:[{out, size_bytes, start, end, …}]}`

**Why this exists:** keeps the clip-cutting primitive addressable without
needing to rig an S3 trigger. Scripts, the webapp, or future orchestration
(e.g. a human editor who marks up a rough cut) can invoke it directly.
Library primitive already in use — this just exposes it as a callable surface.

**Not doing (yet):** VAST DB writes. The caller already knows what it asked
for; it can track results from the invoke response. If we ever want audit
trails for invoke calls, add a `subclipper_jobs` table later.

## Table schemas to add (Phase 1+)

### `source_videos` (Phase 1)

Spine of a pre-ingest run. Updated by each stage.

```
source_id                  string   (PK, md5 of inbox s3 path)
s3_inbox_path              string
filename                   string
file_size_bytes            int64
sha256                     string
uploaded_at                float64
uploaded_by                string

duration_seconds           float64
video_codec                string
width, height              int32
fps                        float64
pixel_format               string
audio_codec                string
audio_channels             int32
audio_sample_rate          int32
loudness_integrated_lufs   float64
loudness_true_peak_dbtp    float64

qc_status                  string   (pending|passed|warn|failed)
qc_verdict_reason          string
qc_black_runs_json         string   (JSON array of {start,end})
qc_freeze_runs_json        string
qc_silence_runs_json       string
qc_is_vfr                  bool
qc_is_interlaced           bool
qc_checked_at              float64

clip_prompt                string
clip_prompt_source         string   (s3_metadata|sidecar|default)
clip_extraction_status     string   (pending|done|failed)
clip_count                 int32
clip_extracted_at          float64

packaging_status           string   (pending|done|failed)
package_id                 string
packaged_at                float64

catalog_handoff_at         float64
status                     string   (active|quarantined|archived)
created_at                 float64
updated_at                 float64
```

### `extracted_clips` (Phase 2)

```
clip_id                    string   (PK, md5 of clip s3 path)
source_id                  string   (fk)
clip_index                 int32
start_seconds              float64
end_seconds                float64
duration_seconds           float64
shot_count                 int32
clip_s3_path               string
prompt                     string
match_confidence           float64
match_reason               string
vision_model               string
frame_scores_json          string
created_at                 float64
```

### `delivery_packages` (Phase 3)

```
package_id                 string   (PK, uuid)
source_id                  string   (fk)
package_root_s3_path       string
clip_count                 int32
rendition_count            int32
total_size_bytes           int64
rights_cleared_for_json    string   (JSON array)
restrictions_json          string   (JSON array)
source_attribution         string
clearance_expires_at       float64
licensing_notes            string
delivered_to               string
notified_at                float64
created_at                 float64
```

### `package_renditions` (Phase 3)

```
rendition_id               string   (PK)
package_id                 string   (fk)
clip_id                    string   (fk)
rendition_name             string   (e.g. h264-1080p)
codec, container           string
width, height              int32
fps                        float64
bitrate_video, bitrate_audio int32
rendition_s3_path          string
file_size_bytes            int64
created_at                 float64
```

## Webapp changes

| Change | Status |
|---|---|
| `/api/configs` — list scopes | 🟡 written, not deployed |
| `/api/configs/<scope>` — grouped settings | 🟡 written, not deployed |
| `/api/configs/<scope>/<key>` (PUT) — update value | 🟡 written, not deployed |
| `/api/configs/<scope>/<key>/reset` (POST) — reset to default | 🟡 written, not deployed |
| `webapp/backend/requirements.txt` — add `vastdb`, `ibis-framework` | ⏳ needed |
| `webapp/frontend/src/pages/SettingsPage.jsx` — new page | ⏳ not yet |
| `webapp/frontend/src/App.jsx` — new `Settings` nav link + route | ⏳ not yet |
| Bump webapp image + redeploy on `.91` | ⏳ after frontend is ready |

Settings page (schema-driven):
- Sidebar = list of scopes
- Main pane = selected scope, grouped by `ui_group`, sorted by `ui_order`
- Widget per `value_type`:
  - `bool` → toggle
  - `int`/`float` → number input
  - `duration_seconds` → number + "s" suffix
  - `percent` → number (0–100) + "%" suffix (stored as 0–1 float)
  - `db` → number + "dB" suffix
  - `string` → text
  - `json` → JSON textarea with syntax validation
- Each row: label, help text (description), current value, default value, "Reset" link, "last updated by/at" tooltip
- "Save all changes" / "Apply now" button (latter hits an `/api/configs/reload` to bust function-side caches — not yet implemented)

## How to pick up where we left off (2026-04-23)

**Pipeline status:** Phases 1–3 deployed and end-to-end verified. Webapp
UI for packages + Phase 4 generic subclipper both code-complete, awaiting
next deploy pass. All functions + DB writes still target `172.200.202.1` —
the `.204` migration was considered and dropped (no `.204` DB available).

**Immediate next step options:**

1. **Phase 4 — generic `james-subclipper`** (no DB needed):
   A stateless direct-invoke function that takes
   `{src, out_bucket, clips: [{start, end, name?, width?, height?, crf?}]}`
   and loops `clips.cut_clip(...)` for each entry with per-clip overrides
   + sensible defaults. The primitive already lives in `shared/ingest/clips.py`
   — Phase 4 is just a thin function wrapper + direct-invoke event shape.

2. **Deploy the new webapp** with `/packages` + Architecture updates. Code
   is all in `webapp/`; Dockerfile already pulls in `c2patool`. Command:
   ```
   docker buildx build --platform linux/amd64 \
     -f webapp/Dockerfile \
     -t docker.selab.vastdata.com:5000/james/media-catalog-webapp:packages-$(date +%s) \
     --load ./webapp
   docker push ...
   (redeploy on .91 with the new tag)
   ```
   Deferred while `.202 → .204` cluster migration is pending — don't want to
   thrash the deploy twice.

3. **Phase 3 hand-off to catalog** (if you want the delivery renditions to
   flow through the existing 8-function pipeline): add 3 columns to `assets`,
   patch `metadata-extractor` to read `x-amz-meta-source-id/clip-id/package-id`,
   have the packager copy each signed rendition into `s3://james-media-catalog/`
   with those tags.

4. ~~`.202 → .204` cluster migration~~ — aborted 2026-04-23: `.204` DB not
   available; staying on `.202`. All config files still point at
   `172.200.202.1`, bucket `james-db`, schema `media-catalog`.

## Files that exist on disk today (2026-04-23)

```
/Users/james/projects/media/catalog/
├── shared/
│   ├── schemas.py                       ✅ all 5 new schemas added (source_videos, extracted_clips, delivery_packages, package_renditions, function_configs)
│   ├── config.py                        ✅ loader + register_defaults + snapshot
│   └── ingest/
│       ├── __init__.py                  ✅
│       ├── ffprobe.py                   ✅ Phase 1
│       ├── qc.py                        ✅ Phase 1 (27 knobs)
│       ├── qc_policy.py                 ✅ Phase 1
│       ├── s3_helpers.py                ✅ parse_s3_path, copy_object, move_object (metadata-preserving), download_to_temp, put_bytes, get_object_tags
│       ├── tables.py                    ✅ upserts for all 4 tables + ensure_*_table helpers
│       ├── scene.py                     ✅ Phase 2 (3 knobs)
│       ├── vision.py                    ✅ Phase 2 (8 knobs)
│       ├── clips.py                     ✅ Phase 2 (5 knobs) — merge_matching_shots, constrain_clips, cut_clip
│       ├── transcode.py                 ✅ Phase 3 (3 knobs) — preset-driven
│       ├── thumbnail.py                 ✅ Phase 3 (3 knobs)
│       ├── manifest.py                  ✅ Phase 3 (6 knobs) — sidecar JSON + build_c2pa_claim_for_rendition
│       └── provenance.py                ✅ Phase 3 (10 knobs) — c2patool CLI wrapper, sign_rendition + verify_c2pa
├── scripts/
│   └── seed_function_configs.py         ✅ idempotent; discovers scopes via iter_registered_schemas
├── functions/foundation/
│   ├── qc-inspector/                    ✅ revision 6 deployed
│   ├── ai-clipper/                      ✅ revision 3 deployed
│   └── media-packager/                  ✅ revision 4 deployed
│       ├── c2patool                     ← bundled linux-amd64 binary
│       └── c2pa-signing/
│           ├── signing.key              ← ES256 private key (perms chmod'd 0644 in init())
│           ├── signing.pub              ← X.509 cert with emailProtection EKU
│           └── openssl.cnf              ← config used to generate the cert
├── webapp/
│   ├── c2patool                         ← bundled for /api/packages/.../c2pa endpoint
│   ├── Dockerfile                       ← copies c2patool into /usr/local/bin
│   ├── backend/app.py                   ✅ /api/packages* endpoints added
│   └── frontend/src/pages/
│       ├── PackagesPage.jsx             ✅ grid view
│       ├── PackageDetailPage.jsx        ✅ player + C2PA panel
│       └── ArchitecturePage.jsx         ✅ updated with Phase 1/2/3 sections
├── config/config.json                   ← canonical creds (currently .202)
└── docs/INGEST_PIPELINE.md              ← this file
```

## Files on `.91` today (2026-04-23)

```
S3 buckets:
  s3://james-media-inbox/                ✅ active
  s3://james-media-qc-passed/            ✅ active
  s3://james-media-qc-failed/            ✅ active
  s3://james-media-clips/                ✅ active (AI-extracted clips + _ready.json markers)
  s3://james-media-deliveries/           ✅ active (C2PA-signed delivery bundles)
  s3://james-media-catalog/              (unchanged; existing 8-function trigger)
  s3://james-media-subclips/             (unchanged; subclip-ai-analyzer trigger)

DataEngine triggers:
  james-inbox-trigger                    ✅ james-media-inbox → qc-inspector
  james-qc-passed-trigger                ✅ james-media-qc-passed → ai-clipper
  james-clips-ready-trigger              ✅ james-media-clips → media-packager (handler filters for _ready.json)
  james-media-catalog                    (unchanged)
  james-subclips-trigger                 (unchanged)

Pipeline: james-media-unified            ✅ Status: Ready — 11 function deployments

VAST DB: james-db / media-catalog (on 172.200.202.1 — migration to .204 pending):
  function_configs                       ✅ 65 rows across 4 scopes
  source_videos                          ✅ ~5 rows (demo uploads)
  extracted_clips                        ✅ ~5 rows
  delivery_packages                      ✅ ~4 rows
  package_renditions                     ✅ ~16 rows (4 per package)
  assets + others                        (unchanged — legacy pipeline)

/tmp/seed-cfg/                           — scratch copy of shared/ + scripts on .91 used to run the seeder
```

## Reminders for future-me

- **Never move the inference API key** — it's hardcoded in the existing
  `subclip-ai-analyzer/main.py`, and also in `config/config.json`.
  Phase 2 (ai-clipper) should read it from `config.json`, not redefine it.
- **The pod-caching TTL is 60s** — after a user hits "Save" in the
  Settings UI, functions can see stale values for up to 60s. We may
  need a `/api/configs/reload` endpoint that posts a broker message
  functions listen for. For now, 60s is acceptable.
- **Test the Settings UI early** — adding new knobs is cheap once the
  editor is schema-driven, so getting the editor right up front is the
  highest-leverage work.
- **QC: don't double-download** — all detectors in `qc-inspector`
  operate on one locally-downloaded copy. The handler should do the
  download once and pass the path to every detector.
- **VAST DB reachability** — Mac can't hit `172.200.202.1` directly.
  Any script that touches VAST DB gets pushed to `.91` and runs there.
  Webapp backend runs on `.91` so it reaches VAST fine.
