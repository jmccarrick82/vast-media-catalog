# Webapp Documentation

## Architecture

The webapp runs as a single Docker container with three services managed by supervisord:

1. **Flask API** (port 3000) — Backend serving REST endpoints and built React frontend
2. **Trino** (port 8080) — SQL query engine with VAST connector
3. **React Frontend** — Built at container build time, served as static files

The container also bundles `c2patool` (the C2PA reference CLI) at
`/usr/local/bin/c2patool` so the `/api/packages/<id>/renditions/<rid>/c2pa`
endpoint can live-verify signed MP4s on demand.

## API Endpoints

### Assets & use cases (original catalog pipeline)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `GET /api/personas` | GET | List all 6 personas with use case IDs |
| `GET /api/personas/:id/usecases` | GET | List use cases for a persona |
| `GET /api/usecases/:id/data` | GET | Query Trino for use case visualization data |
| `GET /api/assets` | GET | List recent assets with core metadata |
| `GET /api/assets/:id` | GET | Full asset detail (all ~120 columns) |
| `GET /api/assets/:id/relationships` | GET | Relationship graph edges for an asset |
| `GET /api/assets/:id/detail` | GET | Composite detail (asset + related rows + relationships) |
| `GET /api/stats` | GET | System-wide statistics |
| `POST /api/upload` | POST | Upload a file into `james-media-catalog` (multipart/form-data) |
| `GET /api/video?path=s3://…` | GET | Range-enabled S3 proxy for the HTML `<video>` tag. Allowed buckets: `james-media-catalog`, `james-media-subclips`, `james-media-clips`, `james-media-deliveries`, `james-media-inbox`, `james-media-qc-passed`, `james-media-qc-failed`. |

### Semantic search (Qdrant-backed)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `GET /api/semantic-search?q=<text>&limit=N` | GET | Embed query with `nvidia/nv-embed-v1`, cosine-search the `subclips` collection, return top-K hits with summary, category, s3_path, and the passage that was embedded. |

### Delivery packages + C2PA provenance (Phase 3)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `GET /api/packages` | GET | List all delivery packages with source filename + clip/rendition counts + C2PA signed counts. Sorted newest first. |
| `GET /api/packages/<package_id>` | GET | Full detail: `delivery_packages` row (licensing decoded from JSON) + `source_videos` row + clips (ordered by `clip_index`) with renditions grouped under each clip. |
| `GET /api/packages/<package_id>/manifest` | GET | Fetch the sidecar `manifest.json` from S3 and return as `application/json`. |
| `GET /api/packages/<package_id>/renditions/<rendition_id>/c2pa` | GET | **Live C2PA verify** — downloads the rendition from S3, runs `c2patool` locally, returns the parsed report. Response shape: `{signed, rendition_id, active_manifest, manifests, active, ai_disclosure}` where `ai_disclosure` is the extracted `com.vast.ai_clip_selection` assertion (model + prompt + confidence + source span) for quick UI access. |

### Runtime function configs (`function_configs` table)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `GET /api/configs` | GET | List all scopes with row counts. |
| `GET /api/configs/<scope>` | GET | All settings for one scope, sorted by `ui_group` + `ui_order`. `value` / `default_value` / `min` / `max` are JSON-decoded server-side. |
| `PUT /api/configs/<scope>/<key>` | PUT | Update one setting. Body: `{value: <any JSON>, updated_by?: "webapp"}`. |
| `PUT /api/configs/<scope>` | PUT | Bulk-update many settings in one transaction. Body: `{updates: [{key, value}, ...], updated_by?}`. Returns `{applied, skipped}`. |
| `POST /api/configs/<scope>/<key>/reset` | POST | Reset one setting to its stored `default_value`. |
| `POST /api/configs/<scope>/reset` | POST | **Reset every setting in a scope** to factory defaults. No body required. Returns `{reset: [keys…]}`. |

## Frontend Pages

| Route | Component | Description |
|-------|-----------|-------------|
| `/` | HomePage | Dashboard with 6 persona cards and system stats |
| `/assets/:id` | AssetDossier | Full asset detail with relationship graph |
| `/search` | SearchPage | Semantic search over subclip embeddings |
| `/packages` | PackagesPage | Grid of delivery packages with C2PA signed-status badge (new) |
| `/packages/:packageId` | PackageDetailPage | Video player + rendition picker + **live C2PA panel** + licensing card + sidecar manifest viewer (new) |
| `/settings` | SettingsPage | Schema-driven editor for every knob in `function_configs` (new) |
| `/architecture` | ArchitecturePage | End-to-end pipeline walkthrough including all 3 pre-ingest phases + C2PA |
| `/persona/:id` | PersonaPage | Legacy — use case list for a persona |
| `/usecase/:id` | UseCasePage | Legacy — visualization for a specific use case |

### `/packages/:packageId` — the C2PA detail view

Left column:

- **Video player** that plays whichever rendition is selected (streams via `/api/video`, range-enabled).
- **Rendition picker**: one row per extracted clip, with buttons for each preset (`h264-1080p`, `h264-720p`, `proxy-360p`, `hevc-4k`). Signed renditions show a small shield icon on the button.
- **Sidecar manifest** viewer (collapsed by default; fetches on expand).

Right column:

- **Content Credentials (C2PA) panel** — re-runs `c2patool` live against the selected rendition via `/api/packages/.../c2pa` and renders:
  - Signature info (signer CN, algorithm, signed timestamp, cert serial, manifest label with copy button).
  - AI Disclosure — `com.vast.ai_clip_selection` assertion pulled out for prominence: model, prompt in quotes, confidence %, source span.
  - Actions — the 3-step provenance chain (`created → placed → edited`) with software-agent names + parameter descriptions.
  - Training & Data Mining — per-mode allowed/notAllowed policy table.
  - Creative Work — source name + authors.
  - Full assertions list (collapsed).
  - Link to contentcredentials.org/verify for a second opinion.
- **Licensing card** — attribution, rights cleared for, restrictions, clearance expiry, notes.

### `/settings` — schema-driven config editor

Sidebar lists all scopes with counts. Main pane groups settings by `ui_group` and renders a widget per `value_type`:

- `bool` → toggle
- `int` / `float` → number input with optional min/max
- `duration_seconds` / `db` → number + unit suffix
- `percent` → 0–100 input with `%` suffix (stored as 0–1 float)
- `string` → text input
- `json` → textarea with JSON-parse validation

Each row shows current value, default, min/max, last-updated-by/at. Actions:

- Per-row **Save** (immediate PUT) and **Reset** (to default).
- Top-bar **Update** — bulk-apply only the rows with unsaved edits in a single transaction.
- Top-bar **Restore defaults** — with a confirm dialog, resets every key in the scope to its `default_value`.
- Top-bar **Refresh** — reload current values from DB (discards unsaved draft edits).

## Visualization Types by Use Case

| UC | Type | Components Used |
|----|------|-----------------|
| UC01, UC06, UC08, UC11, UC14, UC16, UC17 | Graph | ForceGraph2D + MetricsCard + DataTable |
| UC02, UC03, UC05, UC09, UC12, UC15, UC20, UC24 | Table | DataTable + MetricsCard |
| UC04, UC10, UC19, UC21 | Timeline | Timeline + MetricsCard |
| UC13, UC26 | Pie Chart | Recharts PieChart + DataTable |
| UC23 | Bar Chart | Recharts BarChart + DataTable |
| UC22, UC25 | Dashboard | MetricsCard + Recharts + DataTable |
| UC07, UC18 | Tree/Table | MetricsCard + DataTable |

## Shared Components

### `DataTable`
Renders tabular data with formatted headers, truncation for long values, and optional row click handler.

### `MetricsCard`
Displays KPI metric cards in a responsive grid. Each item has `label`, `value`, and optional `color`.

### `Timeline`
Vertical timeline with dot markers. Configurable `labelKey`, `timeKey`, and `detailKey` props.

### `Graph`
Force-directed graph using `react-force-graph-2d`. Accepts `nodes` (with id, label, color) and `links` (with source, target).

## SQL Queries

Each use case has a dedicated SQL file in `webapp/backend/queries/ucXX.sql`. Queries use:
- `{{LIMIT}}` placeholder — replaced with the `limit` query parameter
- `{{ASSET_ID}}` placeholder — replaced with the `asset_id` query parameter
- Table path format: `vast."james-db/media-catalog".table_name`

## Styling

The frontend uses VAST Data's dark theme:
- Background: `#03142c` (VAST Dark)
- Accent: `#1fd9fe` (VAST Blue)
- Surface: `#0a1e3d`, `#112a4a`
- Text: `#e8edf3` (primary), `#8b9ab5` (dim)
- Status colors: green (success), amber (warning), red (danger)
