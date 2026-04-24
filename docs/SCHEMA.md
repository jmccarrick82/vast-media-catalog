# Database Schema Reference

Tables are split across two logical groups:

1. **Pre-ingest tables** (§1 below) — added in Phases 1–3 of the pre-ingest
   pipeline. Track raw uploads, AI-selected clips, signed delivery packages,
   and runtime config knobs. Live in VAST bucket `james-db`, schema `media-catalog`.
2. **Catalog tables** (§2 onward) — the original 10 provenance tables written
   by the 11-function catalog fan-out. Also in `james-db` / `media-catalog` on
   the live cluster.

Trino paths (live deployment):

```
vast."james-db/media-catalog".<table>
```

The doc uses the generic `<bucket>/<schema>.<table>` form below where paths
are stable across clusters.

---

## 1. Pre-Ingest Tables (Phases 1–3)

### 1.1 `source_videos`

**Written by:** qc-inspector (Phase 1), ai-clipper (Phase 2), media-packager (Phase 3)
**Rows:** one per raw upload to `s3://james-media-inbox/`
**Primary key:** `source_id` (MD5 of the original inbox s3 path)

The "spine" of a pre-ingest run. Every stage progressively adds columns to
the same row so it acts as the asset's state machine.

| Column | Type | Description |
|---|---|---|
| `source_id` | `string` | MD5(s3_inbox_path). Primary key. |
| `s3_inbox_path` | `string` | Original inbox location. |
| `filename` | `string` | Basename. |
| `file_size_bytes` | `int64` | Probed size. |
| `sha256` | `string` | File hash (populated during packaging). |
| `uploaded_at` | `float64` | Epoch of first row write. |
| `uploaded_by` | `string` | S3 metadata or "unknown". |
| `duration_seconds` | `float64` | From ffprobe. |
| `video_codec` / `video_profile` | `string` | From ffprobe. |
| `width` / `height` | `int32` | Video dims. |
| `fps` | `float64` | Frame rate. |
| `pixel_format` / `color_space` / `color_range` | `string` | |
| `bitrate_total` | `int64` | Overall bitrate. |
| `audio_codec` | `string` | |
| `audio_channels` / `audio_sample_rate` | `int32` | |
| `audio_layout` | `string` | e.g. `stereo`, `5.1`. |
| `qc_status` | `string` | `pending:<step>` / `passed` / `warn` / `failed` / `failed:<detector>`. Terminal values: passed/warn/failed. |
| `qc_verdict_reason` | `string` | Short string e.g. `"WARN: true peak -0.2 dBTP > -1.0 (clipping risk)"`. |
| `qc_issues_json` | `string` | JSON array of all issues found. |
| `qc_black_runs_json` / `qc_freeze_runs_json` / `qc_silence_runs_json` | `string` | JSON `[{start,end}, …]` arrays of detected events. |
| `qc_black_ratio` / `qc_freeze_ratio` / `qc_silence_ratio` | `float64` | Fraction of duration. |
| `qc_loudness_lufs` | `float64` | Integrated loudness from `ebur128`. |
| `qc_true_peak_dbtp` | `float64` | True peak. |
| `qc_is_vfr` / `qc_is_interlaced` | `bool` | |
| `qc_config_snapshot_json` | `string` | Full config the handler used — audit trail. |
| `qc_checked_at` / `qc_elapsed_seconds` | `float64` | |
| `clip_prompt` | `string` | Prompt ai-clipper used. |
| `clip_prompt_source` | `string` | `s3_metadata` / `sidecar` / `default`. |
| `clip_extraction_status` | `string` | `pending:started` / `done` / `failed:<why>`. |
| `clip_count` | `int32` | Number of extracted clips. |
| `clip_extracted_at` | `float64` | |
| `packaging_status` | `string` | `pending:*` / `done` / `failed`. |
| `package_id` | `string` | FK → delivery_packages. |
| `packaged_at` | `float64` | |
| `catalog_handoff_at` | `float64` | When rendition copies landed in james-media-catalog (future). |
| `status` | `string` | `active` / `quarantined` / `archived`. |
| `current_s3_path` | `string` | Where the file is now (updated on move). |
| `created_at` / `updated_at` | `float64` | |

### 1.2 `extracted_clips`

**Written by:** ai-clipper (Phase 2)
**Rows:** many per source — one per AI-selected clip span
**Primary key:** `clip_id` (MD5 of the clip's s3 path)

| Column | Type | Description |
|---|---|---|
| `clip_id` | `string` | MD5(clip_s3_path). Primary key. |
| `source_id` | `string` | FK → source_videos. |
| `clip_index` | `int32` | 0-based order within the source. |
| `clip_s3_path` | `string` | `s3://james-media-clips/<source_id>/clip-NNN.mp4`. |
| `start_seconds` / `end_seconds` / `duration_seconds` | `float64` | |
| `shot_count` | `int32` | # adjacent matching shots merged into this clip. |
| `file_size_bytes` | `int64` | |
| `prompt` | `string` | The exact prompt sent to the vision model. |
| `prompt_source` | `string` | `s3_metadata` / `sidecar` / `default`. |
| `match_confidence` | `float64` | 0–1. Max across merged shots. |
| `match_reason` | `string` | Vision model's one-line explanation. |
| `vision_model` | `string` | e.g. `nvidia/llama-3.2-11b-vision-instruct`. |
| `frame_scores_json` | `string` | JSON `[{t, confidence, reason}, …]` for audit. |
| `status` | `string` | `active` / `superseded` / `deleted`. |
| `created_at` / `updated_at` | `float64` | |

### 1.3 `delivery_packages`

**Written by:** media-packager (Phase 3)
**Rows:** one per delivery bundle
**Primary key:** `package_id` (UUID4 hex, first 12 chars)

| Column | Type | Description |
|---|---|---|
| `package_id` | `string` | 12-char uuid hex. Primary key. |
| `source_id` | `string` | FK → source_videos. |
| `package_root_s3_path` | `string` | `s3://james-media-deliveries/<package_id>/`. |
| `manifest_s3_path` | `string` | `.../manifest.json`. |
| `clip_count` / `rendition_count` | `int32` | Totals for the bundle. |
| `total_size_bytes` | `int64` | Sum of all rendition sizes. |
| `rights_cleared_for_json` | `string` | JSON list. Uploader-overridable via `x-amz-meta-rights-cleared-for`. |
| `restrictions_json` | `string` | JSON list. Uploader-overridable. |
| `source_attribution` | `string` | From template or uploader override. |
| `clearance_expires_at` | `float64` | Epoch — packaged_at + clearance_days. |
| `licensing_notes` | `string` | Freeform. |
| `c2pa_enabled` | `bool` | Was signing attempted. |
| `c2pa_signer` | `string` | CN of the signing cert when any rendition signed successfully. |
| `c2pa_claim_generator` | `string` | e.g. `vast-media-catalog`. |
| `c2pa_signed_count` | `int32` | How many renditions got signed. |
| `c2pa_errors_json` | `string` | JSON list of `{clip_id, rendition, error}` if any failed. |
| `delivered_to` / `notified_at` | `string` / `float64` | Populated when the bundle is handed off (future). |
| `status` / `status_reason` | `string` | `building` / `ready` / `delivered` / `expired` / `failed`. |
| `created_at` / `updated_at` | `float64` | |

### 1.4 `package_renditions`

**Written by:** media-packager (Phase 3)
**Rows:** many per package — one per (clip × rendition preset) pair
**Primary key:** `rendition_id` (MD5 of the rendition s3 path)

| Column | Type | Description |
|---|---|---|
| `rendition_id` | `string` | Primary key. |
| `package_id` | `string` | FK → delivery_packages. |
| `clip_id` | `string` | FK → extracted_clips. |
| `source_id` | `string` | FK (denormalized for query convenience). |
| `rendition_name` | `string` | Preset name, e.g. `h264-1080p`. |
| `container` | `string` | `mp4`. |
| `video_codec` / `audio_codec` | `string` | |
| `width` / `height` | `int32` | |
| `fps` | `float64` | Reserved — caller can probe post-write. |
| `video_bitrate` / `audio_bitrate` | `int32` | |
| `rendition_s3_path` | `string` | `s3://james-media-deliveries/<pkg>/clips/clip-NNN/<preset>.mp4`. |
| `file_size_bytes` | `int64` | Post-sign size. |
| `sha256` | `string` | Post-sign content hash. |
| `c2pa_signed` | `bool` | Was signing successful for this rendition. |
| `c2pa_manifest_label` | `string` | `urn:uuid:...` label from c2patool. |
| `c2pa_signer` | `string` | CN of signer cert. |
| `c2pa_signed_at` | `float64` | |
| `c2pa_error` | `string` | Non-null on signing failure. |
| `status` | `string` | `ready` / `failed` / `superseded`. |
| `created_at` / `updated_at` | `float64` | |

### 1.5 `function_configs`

**Written by:** `scripts/seed_function_configs.py` (idempotent seed) + webapp `/settings` UI
**Rows:** currently **72** across 5 scopes (`qc-inspector`, `ai-clipper`, `packager`, `provenance`, `subclipper`)
**Primary key:** `config_id` = `<scope>:<key>`

Runtime-editable knobs for every pre-ingest function. Read with a 60-second
per-pod cache; any write through `/settings` takes effect on the next handler
invocation without a redeploy.

| Column | Type | Description |
|---|---|---|
| `config_id` | `string` | `<scope>:<key>`. Primary key. |
| `scope` | `string` | e.g. `qc-inspector`, `ai-clipper`, `packager`, `provenance`, `subclipper`. |
| `key` | `string` | Short key within the scope. |
| `value` | `string` | JSON-encoded current value. |
| `value_type` | `string` | `int` / `float` / `bool` / `string` / `duration_seconds` / `percent` / `db` / `json`. |
| `default_value` | `string` | JSON-encoded factory default. |
| `description` | `string` | Human explanation shown in the UI. |
| `min_value` / `max_value` | `string` | Optional UI range bounds (JSON-encoded). |
| `ui_group` | `string` | e.g. `"Black frames"`. |
| `ui_order` | `int32` | Sort within group. |
| `updated_at` | `float64` | |
| `updated_by` | `string` | `"seed"` / `"webapp"` / `"webapp(reset)"` / `"webapp(reset_all)"`. |

Type-aware accessors on the read path:
```python
from shared.config import load_config
cfg = load_config(scope="qc-inspector")
cfg.get_float("black_frame_max_ratio_fail")     # → 0.5
cfg.get_percent("black_frame_max_ratio_warn")   # → 0.1 (stored 0-1, UI 0-100)
cfg.get_duration("silence_min_run_seconds")     # → 1.0
cfg.get_list("video_codec_allowlist")           # → ["h264","hevc","vp9","av1"]
cfg.get_bool("require_video_stream")            # → True
cfg.snapshot()                                  # → dict of all knobs in scope
```

---

## 2. Unified Assets Table

**Table name:** `assets`
**Trino path:** `vast."media-catalog/provenance".assets`

This is the single wide table where every function upserts the columns it owns
for a given asset. Each row is keyed by `asset_id` (the MD5 hex digest of
`s3_path`). Most columns will be `NULL` until the responsible function has
processed the asset.

### Foundation columns -- metadata-extractor

| Column | Type | Description |
|---|---|---|
| `asset_id` | `string` | MD5 of `s3_path`. Primary key. |
| `s3_path` | `string` | Full S3 object path (e.g. `s3://bucket/path/file.mxf`). |
| `filename` | `string` | Basename of the file. |
| `file_size_bytes` | `int64` | Raw file size in bytes. |
| `duration_seconds` | `float64` | Media duration in seconds. |
| `video_codec` | `string` | Video codec name (e.g. `h264`, `prores`). |
| `audio_codec` | `string` | Audio codec name (e.g. `aac`, `pcm_s24le`). |
| `width` | `int32` | Video frame width in pixels. |
| `height` | `int32` | Video frame height in pixels. |
| `fps` | `float64` | Frames per second. |
| `bitrate` | `int64` | Overall bitrate in bits/second. |
| `pixel_format` | `string` | Pixel format string (e.g. `yuv420p`). |
| `audio_channels` | `int32` | Number of audio channels. |
| `audio_sample_rate` | `int32` | Audio sample rate in Hz. |
| `format_name` | `string` | Container format (e.g. `mov`, `mxf`). |
| `creation_time` | `string` | Embedded creation timestamp (ISO-8601 string). |
| `ingested_at` | `float64` | Unix epoch when the row was first created. |

### Foundation columns -- hash-generator

| Column | Type | Description |
|---|---|---|
| `sha256` | `string` | SHA-256 hex digest of file contents. |
| `perceptual_hash` | `string` | Perceptual hash for visual similarity comparison. |
| `hash_computed_at` | `float64` | Unix epoch when hashes were computed. |

### Foundation columns -- video-subclip

| Column | Type | Description |
|---|---|---|
| `is_subclip` | `bool` | `true` if this asset is a subclip created by video-subclip. |
| `subclip_parent_asset_id` | `string` | `asset_id` of the parent video this subclip was extracted from. |
| `subclip_parent_s3_path` | `string` | S3 path of the parent video. |
| `subclip_index` | `int32` | Zero-based index of this subclip within the parent (0, 1, 2, ...). |
| `subclip_start_seconds` | `float64` | Start time of this subclip within the parent video (in seconds). |
| `subclip_duration_seconds` | `float64` | Duration of this subclip in seconds. |
| `subclip_count` | `int32` | (Parent only) Number of subclips created from this video. |

Subclip linkage is also stored in the `relationships` table (type=`subclip`) and embedded as ffmpeg metadata tags in the subclip MP4 file.

### UC01 -- Rights Conflict Detection

| Column | Type | Description |
|---|---|---|
| `license_type` | `string` | License classification (e.g. `exclusive`, `non-exclusive`, `public_domain`). |
| `territories` | `string` | Comma-separated or JSON list of licensed territories. |
| `restrictions` | `string` | Usage restrictions text. |
| `rights_expiry` | `string` | Expiry date of the rights grant (ISO-8601). |
| `conflict_detected` | `bool` | `true` if a rights conflict exists with another asset. |
| `conflict_details` | `string` | Human-readable description of the conflict. |
| `rights_checked_at` | `float64` | Unix epoch of the last rights check. |

### UC02 -- Orphaned Asset Resolution

| Column | Type | Description |
|---|---|---|
| `orphan_resolved_from_asset_id` | `string` | `asset_id` of the parent this orphan was linked back to. |
| `orphan_resolution_method` | `string` | Method used to resolve (e.g. `hash_match`, `metadata_match`). |
| `orphan_resolved_at` | `float64` | Unix epoch when orphan resolution completed. |

### UC03 -- Unauthorized Use Detection (summary)

| Column | Type | Description |
|---|---|---|
| `unauthorized_match_count` | `int32` | Number of unauthorized copies found. |
| `unauthorized_checked_at` | `float64` | Unix epoch of the last unauthorized-use scan. |

Detail rows are written to the `hash_matches` table.

### UC04 -- License Audit Trail (summary)

| Column | Type | Description |
|---|---|---|
| `licensor` | `string` | Name of the licensor entity. |
| `usage_type` | `string` | Permitted usage type (e.g. `broadcast`, `streaming`). |
| `audit_derivative_count` | `int32` | Number of derivatives found during audit. |
| `license_audit_at` | `float64` | Unix epoch of the last license audit run. |

### UC05 -- Talent & Music Residuals (summary)

| Column | Type | Description |
|---|---|---|
| `faces_detected_count` | `int32` | Total number of distinct faces detected. |
| `music_detected` | `bool` | `true` if copyrighted music was detected in audio. |
| `audio_fingerprint` | `string` | Audio fingerprint identifier for music matching. |
| `talent_music_scanned_at` | `float64` | Unix epoch of the last talent/music scan. |

Per-detection rows are written to the `talent_music` table.

### UC06 -- Duplicate Storage Elimination (summary)

| Column | Type | Description |
|---|---|---|
| `duplicate_count` | `int32` | Number of duplicates found for this asset. |
| `total_storage_savings_bytes` | `int64` | Total bytes recoverable by deduplicating. |
| `duplicates_checked_at` | `float64` | Unix epoch of the last deduplication check. |

Detail rows are written to the `hash_matches` table.

### UC07 -- Safe Deletion

| Column | Type | Description |
|---|---|---|
| `dependent_count` | `int32` | Number of assets that depend on this one. |
| `is_leaf` | `bool` | `true` if the asset has no children. |
| `is_root` | `bool` | `true` if the asset has no parents. |
| `deletion_safe` | `bool` | `true` if deleting this asset will not orphan dependents. |
| `deletion_evaluated_at` | `float64` | Unix epoch of the last safe-deletion evaluation. |

### UC08 -- Master vs Derivative Classification

| Column | Type | Description |
|---|---|---|
| `asset_classification` | `string` | One of: `root`, `intermediate`, `leaf`, `duplicate`. |
| `classification_confidence` | `float64` | Confidence score (0.0 -- 1.0). |
| `classification_at` | `float64` | Unix epoch of classification. |

Graph edges are written to the `relationships` table.

### UC09 -- Archive Re-Conformation (summary)

| Column | Type | Description |
|---|---|---|
| `reconformation_match_count` | `int32` | Number of candidate matches for re-conformation. |
| `reconformation_viable` | `bool` | `true` if re-conformation from archive is viable. |
| `reconformation_checked_at` | `float64` | Unix epoch of the last re-conformation check. |

Detail rows are written to the `hash_matches` table.

### UC10 -- Version Control

| Column | Type | Description |
|---|---|---|
| `version_number` | `int32` | Sequential version number. |
| `previous_version_id` | `string` | `asset_id` of the prior version. |
| `version_label` | `string` | Human-readable label (e.g. `v2-color-grade`). |
| `version_recorded_at` | `float64` | Unix epoch when the version was recorded. |

Full version chains are stored in the `version_history` table.

### UC11 -- Training Data Provenance

| Column | Type | Description |
|---|---|---|
| `training_dataset_id` | `string` | Identifier of the training dataset this asset belongs to. |
| `is_training_original` | `bool` | `true` if the asset is the unmodified original used for training. |
| `rights_cleared_for_training` | `bool` | `true` if the rights holder has cleared this asset for ML training. |
| `training_processing_chain` | `string` | JSON description of processing steps applied for training. |
| `training_logged_at` | `float64` | Unix epoch when training provenance was logged. |

### UC12 -- Model Contamination Detection

| Column | Type | Description |
|---|---|---|
| `contamination_risk` | `string` | Risk level: `none`, `low`, `medium`, or `high`. |
| `has_ai_processing_upstream` | `bool` | `true` if any ancestor was AI-generated or AI-processed. |
| `processing_depth` | `int32` | Number of processing hops from original to this asset. |
| `contamination_checked_at` | `float64` | Unix epoch of the last contamination check. |

### UC13 -- Synthetic Content Tracking

| Column | Type | Description |
|---|---|---|
| `ai_probability` | `float64` | Probability (0.0 -- 1.0) that the content is AI-generated. |
| `ai_tool_detected` | `string` | Detected AI generation tool (e.g. `Stable Diffusion`, `Sora`). |
| `ai_model_version` | `string` | Detected model version. |
| `ai_detection_method` | `string` | JSON array of detection methods applied. |
| `ai_detected_at` | `float64` | Unix epoch of the AI content scan. |

### UC14 -- Bias Audit

| Column | Type | Description |
|---|---|---|
| `bias_model_id` | `string` | Identifier of the ML model being audited. |
| `bias_ai_tool_used` | `string` | AI tool that produced the content under audit. |
| `bias_training_data_ids` | `string` | Comma-separated list of training dataset identifiers. |
| `bias_audit_result` | `string` | Audit result description. |
| `bias_risk_level` | `string` | Risk level: `none`, `low`, `medium`, or `high`. |
| `bias_audited_at` | `float64` | Unix epoch of the bias audit. |

### UC15 -- Re-Use Discovery (summary)

| Column | Type | Description |
|---|---|---|
| `has_semantic_embeddings` | `bool` | `true` once embeddings have been extracted for this asset. |
| `embedding_model_name` | `string` | Name of the embedding model (e.g. `openai/clip-vit-large-patch14`). |
| `embedding_frame_count` | `int32` | Number of frames for which embeddings were extracted. |
| `embeddings_extracted_at` | `float64` | Unix epoch when embeddings were written. |

Per-frame embedding vectors are stored in the `semantic_embeddings` table.

### UC16 -- Clearance Inheritance

| Column | Type | Description |
|---|---|---|
| `clearance_status` | `string` | Current clearance status (e.g. `cleared`, `pending`, `denied`). |
| `clearance_type` | `string` | Type of clearance (e.g. `talent`, `music`, `location`). |
| `clearance_inherited_from` | `string` | `asset_id` from which clearance was inherited. |
| `clearance_recorded_at` | `float64` | Unix epoch when clearance was recorded. |

### UC17 -- Compliance Propagation

| Column | Type | Description |
|---|---|---|
| `compliance_rating` | `string` | Rating (e.g. `G`, `PG`, `PG-13`, `R`, `NC-17`). |
| `content_warnings` | `string` | Comma-separated warning tags. |
| `compliance_inherited_from` | `string` | `asset_id` from which compliance was inherited. |
| `compliance_propagated_at` | `float64` | Unix epoch when compliance was propagated. |

### UC18 -- Localization Management

| Column | Type | Description |
|---|---|---|
| `detected_language` | `string` | ISO-639 language code of the primary audio language. |
| `language_confidence` | `float64` | Confidence score (0.0 -- 1.0). |
| `dubbed_from_asset_id` | `string` | `asset_id` of the original asset this was dubbed from. |
| `subtitle_tracks` | `string` | JSON array of subtitle track language codes. |
| `localization_detected_at` | `float64` | Unix epoch of language detection. |

### UC19 -- Leak Investigation (summary)

| Column | Type | Description |
|---|---|---|
| `delivery_recipient` | `string` | Name or identifier of the delivery recipient. |
| `delivery_date` | `string` | ISO-8601 date of the delivery. |
| `leak_hash_fingerprint` | `string` | Hash fingerprint used for leak matching. |
| `delivery_chain` | `string` | JSON object describing the full delivery chain. |
| `leak_indexed_at` | `float64` | Unix epoch when the leak index was built. |

### UC20 -- GDPR Compliance (summary)

| Column | Type | Description |
|---|---|---|
| `gdpr_faces_detected` | `int32` | Number of faces detected in the asset. |
| `gdpr_persons_identified` | `int32` | Number of distinct persons identified. |
| `gdpr_blast_radius` | `int32` | Number of derivative assets affected by a GDPR takedown. |
| `gdpr_scanned_at` | `float64` | Unix epoch of the GDPR scan. |

Per-person detection rows are written to the `gdpr_personal_data` table.

### UC21 -- Chain of Custody

| Column | Type | Description |
|---|---|---|
| `legal_hold_active` | `bool` | `true` if the asset is under legal hold. |
| `sha256_at_hold` | `string` | SHA-256 at the time hold was placed (tamper detection). |
| `hold_placed_at` | `float64` | Unix epoch when the legal hold was placed. |
| `integrity_verified` | `bool` | `true` if current SHA-256 matches `sha256_at_hold`. |
| `related_asset_count` | `int32` | Number of related assets in the hold scope. |
| `custody_verified_at` | `float64` | Unix epoch of the last integrity verification. |

### UC22 -- Ransomware Impact Assessment

| Column | Type | Description |
|---|---|---|
| `is_unique_original` | `bool` | `true` if no other copy of this original exists. |
| `has_backup` | `bool` | `true` if a backup/duplicate is known to exist. |
| `surviving_derivatives_count` | `int32` | Number of derivatives that survive if this asset is lost. |
| `recovery_priority` | `string` | One of: `CRITICAL`, `HIGH`, `MEDIUM`, `LOW`. |
| `ransomware_assessed_at` | `float64` | Unix epoch of the assessment. |

### UC23 -- Content Valuation

| Column | Type | Description |
|---|---|---|
| `valuation_derivative_count` | `int32` | Number of derivatives contributing to valuation. |
| `reuse_count` | `int32` | Number of times the asset has been re-used. |
| `delivery_count` | `int32` | Number of distinct deliveries of this asset. |
| `commercial_value_score` | `float64` | Computed commercial value score. |
| `value_tier` | `string` | One of: `PREMIUM`, `HIGH`, `MEDIUM`, `LOW`. |
| `valued_at` | `float64` | Unix epoch of the valuation. |

### UC24 -- Syndication Revenue Tracking (summary)

| Column | Type | Description |
|---|---|---|
| `syndication_licensee_count` | `int32` | Number of distinct licensees. |
| `syndication_territory_count` | `int32` | Number of distinct territories. |
| `primary_licensee` | `string` | Licensee with the highest revenue share. |
| `primary_territory` | `string` | Territory with the highest revenue share. |
| `syndication_tracked_at` | `float64` | Unix epoch of the last syndication tracking run. |

Per-licensee/territory rows are written to the `syndication_records` table.

### UC25 -- Insurance & Disaster Recovery

| Column | Type | Description |
|---|---|---|
| `is_irreplaceable` | `bool` | `true` if the asset cannot be recreated. |
| `has_digital_copies` | `bool` | `true` if digital copies exist. |
| `digital_copy_count` | `int32` | Number of known digital copies. |
| `replacement_cost_tier` | `string` | Cost tier for replacement (e.g. `HIGH`, `LOW`). |
| `commercial_history_score` | `float64` | Score reflecting the asset's commercial track record. |
| `insurance_valued_at` | `float64` | Unix epoch of the insurance valuation. |

### UC26 -- Co-Production Attribution (summary)

| Column | Type | Description |
|---|---|---|
| `primary_production_company` | `string` | Name of the lead production company. |
| `crew_origin` | `string` | Country/region of the primary crew. |
| `ownership_split_pct` | `float64` | Ownership percentage of the primary company. |
| `contribution_type` | `string` | Type of contribution (e.g. `financing`, `creative`, `distribution`). |
| `attribution_at` | `float64` | Unix epoch of the attribution run. |

Per-company rows are written to the `production_entities` table.

### Catalog Reconciler -- Lifecycle Tracking

| Column | Type | Description |
|---|---|---|
| `status` | `string` | Asset lifecycle status: `active`, `moved`, or `deleted`. |
| `original_s3_path` | `string` | First known S3 path. Set on move to preserve lineage. |
| `move_count` | `int32` | Number of times this asset has been moved. |
| `last_moved_at` | `float64` | Unix epoch of the most recent move. |
| `last_moved_by` | `string` | User (login_name or uid) who performed the move, from audit log. |
| `deleted_at` | `float64` | Unix epoch when the asset was deleted. |
| `deleted_by` | `string` | User who deleted the asset, from audit log. |
| `last_reconciled_at` | `float64` | Unix epoch of the last successful reconciler cycle that confirmed this asset exists. |

---

## 3. Relationship Tables

Each table below stores multiple rows per asset and lives alongside `assets` in
the same VAST bucket and schema (`media-catalog/provenance`).

---

### 2.1 `relationships`

**Trino path:** `vast."media-catalog/provenance".relationships`
**Written by:** UC08 (Master vs Derivative Classification)

Asset graph edges describing parent/child and derivative lineage.

| Column | Type | Description |
|---|---|---|
| `relationship_id` | `string` | Unique ID for this edge. |
| `parent_asset_id` | `string` | `asset_id` of the parent (upstream) asset. |
| `child_asset_id` | `string` | `asset_id` of the child (downstream) asset. |
| `relationship_type` | `string` | Edge type (e.g. `master_derivative`, `duplicate`, `transcode`). |
| `confidence` | `float64` | Confidence score for the relationship (0.0 -- 1.0). |
| `created_at` | `float64` | Unix epoch when the relationship was created. |

---

### 2.2 `hash_matches`

**Trino path:** `vast."media-catalog/provenance".hash_matches`
**Written by:** UC03 (Unauthorized Use Detection), UC06 (Duplicate Storage Elimination), UC09 (Archive Re-Conformation)

Pairs of assets that match on hash or perceptual similarity.

| Column | Type | Description |
|---|---|---|
| `match_id` | `string` | Unique ID for this match pair. |
| `asset_a_id` | `string` | `asset_id` of the first asset. |
| `asset_b_id` | `string` | `asset_id` of the second asset. |
| `match_type` | `string` | How they matched (e.g. `sha256_exact`, `perceptual_hash`, `content_fingerprint`). |
| `similarity_score` | `float64` | Similarity score (1.0 = identical). |
| `storage_savings_bytes` | `int64` | Bytes saved if deduplicated. Null when not applicable. |
| `reconformation_viable` | `bool` | `true` if the match supports archive re-conformation. |
| `detected_at` | `float64` | Unix epoch when the match was detected. |

---

### 2.3 `talent_music`

**Trino path:** `vast."media-catalog/provenance".talent_music`
**Written by:** UC05 (Talent & Music Residuals)

Per-timestamp face and music detections within an asset.

| Column | Type | Description |
|---|---|---|
| `detection_id` | `string` | Unique ID for this detection. |
| `asset_id` | `string` | `asset_id` of the scanned asset. |
| `s3_path` | `string` | S3 path of the scanned asset. |
| `detection_type` | `string` | `face` or `music`. |
| `label` | `string` | Detected identity or track name. |
| `confidence` | `float64` | Detection confidence (0.0 -- 1.0). |
| `start_time_sec` | `float64` | Start time in seconds within the media. |
| `end_time_sec` | `float64` | End time in seconds within the media. |
| `detected_at` | `float64` | Unix epoch when the detection was recorded. |

---

### 2.4 `semantic_embeddings`

**Trino path:** `vast."media-catalog/provenance".semantic_embeddings`
**Written by:** UC15 (Re-Use Discovery)

CLIP embedding vectors extracted per video frame.

| Column | Type | Description |
|---|---|---|
| `embedding_id` | `string` | Unique ID for this embedding row. |
| `asset_id` | `string` | `asset_id` of the source asset. |
| `s3_path` | `string` | S3 path of the source asset. |
| `frame_index` | `int32` | Zero-based frame index within the video. |
| `embedding` | `list<float32>` | Embedding vector (e.g. 768-dim CLIP). |
| `model_name` | `string` | Embedding model name. |
| `extracted_at` | `float64` | Unix epoch when the embedding was extracted. |

---

### 2.5 `gdpr_personal_data`

**Trino path:** `vast."media-catalog/provenance".gdpr_personal_data`
**Written by:** UC20 (GDPR Compliance)

Per-person detection records for GDPR takedown and audit.

| Column | Type | Description |
|---|---|---|
| `detection_id` | `string` | Unique ID for this detection. |
| `asset_id` | `string` | `asset_id` of the scanned asset. |
| `s3_path` | `string` | S3 path of the scanned asset. |
| `person_id` | `string` | Identifier for the detected person. |
| `data_type` | `string` | Type of personal data (e.g. `face`, `voice`, `name_overlay`). |
| `face_detected` | `bool` | `true` if a face was detected for this person. |
| `frame_timestamps` | `string` | JSON array of timestamps where the person appears. |
| `detected_at` | `float64` | Unix epoch when the detection was recorded. |

---

### 2.6 `syndication_records`

**Trino path:** `vast."media-catalog/provenance".syndication_records`
**Written by:** UC24 (Syndication Revenue Tracking)

Per-licensee/territory records for syndication revenue tracking.

| Column | Type | Description |
|---|---|---|
| `record_id` | `string` | Unique ID for this syndication record. |
| `asset_id` | `string` | `asset_id` of the syndicated asset. |
| `s3_path` | `string` | S3 path of the syndicated asset. |
| `licensee` | `string` | Name of the licensee. |
| `territory` | `string` | Geographic territory of the license. |
| `delivery_version_id` | `string` | `asset_id` of the specific version delivered. |
| `license_status` | `string` | Status (e.g. `active`, `expired`, `pending`). |
| `tracked_at` | `float64` | Unix epoch when the record was tracked. |

---

### 2.7 `production_entities`

**Trino path:** `vast."media-catalog/provenance".production_entities`
**Written by:** UC26 (Co-Production Attribution)

Per-company attribution records for co-productions.

| Column | Type | Description |
|---|---|---|
| `attribution_id` | `string` | Unique ID for this attribution record. |
| `asset_id` | `string` | `asset_id` of the attributed asset. |
| `s3_path` | `string` | S3 path of the attributed asset. |
| `production_company` | `string` | Name of the production company. |
| `crew_origin` | `string` | Country/region of the company's crew. |
| `ownership_split_pct` | `float64` | Percentage ownership (0.0 -- 100.0). |
| `contribution_type` | `string` | Type of contribution (e.g. `financing`, `creative`, `distribution`). |
| `attributed_at` | `float64` | Unix epoch when the attribution was recorded. |

---

### 2.8 `version_history`

**Trino path:** `vast."media-catalog/provenance".version_history`
**Written by:** UC10 (Version Control)

Full version chain entries linking successive versions of an asset.

| Column | Type | Description |
|---|---|---|
| `version_id` | `string` | Unique ID for this version entry. |
| `asset_id` | `string` | `asset_id` of this version. |
| `s3_path` | `string` | S3 path of this version. |
| `version_number` | `int32` | Sequential version number (1-based). |
| `previous_version_id` | `string` | `asset_id` of the immediately preceding version. Null for version 1. |
| `version_label` | `string` | Human-readable label (e.g. `v2-color-grade`). |
| `created_at` | `float64` | Unix epoch when the version entry was created. |

---

### 2.9 `asset_moves`

**Trino path:** `vast."media-catalog/provenance".asset_moves`
**Written by:** catalog-reconciler

Audit trail of asset move and delete events detected by the reconciler. One row per lifecycle event.

| Column | Type | Description |
|---|---|---|
| `event_id` | `string` | UUID for this event. |
| `asset_id` | `string` | `asset_id` at time of event (before move/delete). |
| `new_asset_id` | `string` | New `asset_id` after move. Null for deletes. |
| `event_type` | `string` | `move` or `delete`. |
| `old_s3_path` | `string` | Original S3 path before the event. |
| `new_s3_path` | `string` | Destination S3 path after move. Null for deletes. |
| `protocol` | `string` | Protocol used: `S3`, `NFS`, or `SMB`. |
| `rpc_type` | `string` | Specific RPC operation (e.g. `RENAME`, `DeleteObject`, `REMOVE`). |
| `performed_by` | `string` | User who performed the operation (login_name or uid from audit log). |
| `client_ip` | `string` | Client IP address from audit log. |
| `detected_at` | `float64` | Unix epoch when the reconciler discovered the event. |
| `audit_timestamp` | `float64` | Original timestamp from the VAST audit log. |

---

## 4. Trino Query Examples

### 3.1 Basic asset lookup

```sql
SELECT asset_id, filename, file_size_bytes, duration_seconds,
       video_codec, width, height, fps
FROM   vast."media-catalog/provenance".assets
WHERE  asset_id = 'abc123def456'
```

### 3.2 Find all assets with rights conflicts

```sql
SELECT asset_id, filename, license_type, territories,
       conflict_details, rights_checked_at
FROM   vast."media-catalog/provenance".assets
WHERE  conflict_detected = true
ORDER  BY rights_checked_at DESC
```

### 3.3 High-value assets with many derivatives

```sql
SELECT asset_id, filename, value_tier, commercial_value_score,
       valuation_derivative_count, reuse_count
FROM   vast."media-catalog/provenance".assets
WHERE  value_tier = 'PREMIUM'
ORDER  BY commercial_value_score DESC
LIMIT  50
```

### 3.4 Assets unsafe to delete (have dependents)

```sql
SELECT asset_id, filename, dependent_count, is_root, is_leaf
FROM   vast."media-catalog/provenance".assets
WHERE  deletion_safe = false
ORDER  BY dependent_count DESC
```

### 3.5 Join assets with their parent/child relationships

```sql
SELECT a.asset_id,
       a.filename          AS asset_name,
       r.relationship_type,
       r.confidence,
       p.filename          AS parent_name
FROM   vast."media-catalog/provenance".assets        a
JOIN   vast."media-catalog/provenance".relationships  r
       ON a.asset_id = r.child_asset_id
JOIN   vast."media-catalog/provenance".assets        p
       ON r.parent_asset_id = p.asset_id
WHERE  a.asset_classification = 'leaf'
ORDER  BY r.confidence DESC
```

### 3.6 Find duplicate pairs with storage savings

```sql
SELECT h.match_type,
       h.similarity_score,
       h.storage_savings_bytes,
       a.filename AS file_a,
       b.filename AS file_b
FROM   vast."media-catalog/provenance".hash_matches h
JOIN   vast."media-catalog/provenance".assets       a ON h.asset_a_id = a.asset_id
JOIN   vast."media-catalog/provenance".assets       b ON h.asset_b_id = b.asset_id
WHERE  h.match_type = 'sha256_exact'
ORDER  BY h.storage_savings_bytes DESC
LIMIT  20
```

### 3.7 Talent detections for a specific asset

```sql
SELECT tm.detection_type, tm.label, tm.confidence,
       tm.start_time_sec, tm.end_time_sec
FROM   vast."media-catalog/provenance".talent_music tm
WHERE  tm.asset_id = 'abc123def456'
ORDER  BY tm.start_time_sec
```

### 3.8 GDPR blast radius -- all assets containing a given person

```sql
SELECT g.asset_id, a.filename, g.data_type, g.frame_timestamps
FROM   vast."media-catalog/provenance".gdpr_personal_data g
JOIN   vast."media-catalog/provenance".assets              a ON g.asset_id = a.asset_id
WHERE  g.person_id = 'person-uuid-here'
ORDER  BY g.detected_at DESC
```

### 3.9 Syndication overview -- assets licensed to a specific territory

```sql
SELECT s.asset_id, a.filename, s.licensee, s.license_status, s.territory
FROM   vast."media-catalog/provenance".syndication_records s
JOIN   vast."media-catalog/provenance".assets              a ON s.asset_id = a.asset_id
WHERE  s.territory = 'EMEA'
  AND  s.license_status = 'active'
ORDER  BY s.tracked_at DESC
```

### 3.10 Version history chain for an asset

```sql
SELECT vh.version_number, vh.version_label, vh.previous_version_id,
       a.filename, a.file_size_bytes
FROM   vast."media-catalog/provenance".version_history vh
JOIN   vast."media-catalog/provenance".assets          a ON vh.asset_id = a.asset_id
WHERE  vh.asset_id = 'abc123def456'
ORDER  BY vh.version_number ASC
```

### 3.11 Co-production attribution breakdown

```sql
SELECT pe.production_company, pe.crew_origin,
       pe.ownership_split_pct, pe.contribution_type,
       a.filename
FROM   vast."media-catalog/provenance".production_entities pe
JOIN   vast."media-catalog/provenance".assets              a ON pe.asset_id = a.asset_id
WHERE  pe.asset_id = 'abc123def456'
ORDER  BY pe.ownership_split_pct DESC
```

### 3.12 Ransomware triage -- critical assets without backups

```sql
SELECT asset_id, filename, recovery_priority,
       is_unique_original, has_backup, surviving_derivatives_count
FROM   vast."media-catalog/provenance".assets
WHERE  recovery_priority = 'CRITICAL'
  AND  has_backup = false
ORDER  BY file_size_bytes DESC
```

### 3.13 AI-generated content report

```sql
SELECT asset_id, filename, ai_probability,
       ai_tool_detected, ai_model_version, ai_detection_method
FROM   vast."media-catalog/provenance".assets
WHERE  ai_probability > 0.8
ORDER  BY ai_probability DESC
```

### 3.14 Aggregate storage savings from deduplication

```sql
SELECT COUNT(*)                       AS total_duplicate_pairs,
       SUM(storage_savings_bytes)     AS total_savings_bytes,
       SUM(storage_savings_bytes) / POWER(1024, 3) AS total_savings_gb
FROM   vast."media-catalog/provenance".hash_matches
WHERE  match_type IN ('sha256_exact', 'perceptual_hash')
```

### 3.15 List all subclips for a parent video

```sql
SELECT asset_id, filename, subclip_index,
       subclip_start_seconds, subclip_duration_seconds
FROM   vast."media-catalog/provenance".assets
WHERE  subclip_parent_asset_id = 'parent_asset_id_here'
ORDER  BY subclip_index ASC
```

### 3.16 Find parent videos that have been subclipped

```sql
SELECT asset_id, filename, s3_path,
       duration_seconds, subclip_count
FROM   vast."media-catalog/provenance".assets
WHERE  subclip_count IS NOT NULL
  AND  subclip_count > 0
ORDER  BY subclip_count DESC
```

### 3.17 Join subclips with their parent via relationships table

```sql
SELECT r.parent_asset_id,
       p.filename AS parent_name,
       r.child_asset_id,
       c.filename AS subclip_name,
       c.subclip_index,
       c.subclip_start_seconds
FROM   vast."media-catalog/provenance".relationships r
JOIN   vast."media-catalog/provenance".assets p ON r.parent_asset_id = p.asset_id
JOIN   vast."media-catalog/provenance".assets c ON r.child_asset_id = c.asset_id
WHERE  r.relationship_type = 'subclip'
ORDER  BY p.filename, c.subclip_index
```

### 3.18 List all asset moves and deletes (reconciler)

```sql
SELECT am.event_type, am.protocol, am.rpc_type,
       am.old_s3_path, am.new_s3_path,
       am.performed_by, am.client_ip,
       am.detected_at, am.audit_timestamp
FROM   vast."media-catalog/provenance".asset_moves am
ORDER  BY am.detected_at DESC
LIMIT  50
```

### 3.19 Track a specific asset's move history

```sql
SELECT am.event_type, am.old_s3_path, am.new_s3_path,
       am.protocol, am.performed_by, am.audit_timestamp,
       a.filename, a.status
FROM   vast."media-catalog/provenance".asset_moves am
JOIN   vast."media-catalog/provenance".assets      a ON am.asset_id = a.asset_id
WHERE  am.asset_id = 'abc123def456'
   OR  am.new_asset_id = 'abc123def456'
ORDER  BY am.audit_timestamp ASC
```

### 3.20 Find all deleted assets in the last 24 hours

```sql
SELECT asset_id, filename, s3_path,
       deleted_at, deleted_by
FROM   vast."media-catalog/provenance".assets
WHERE  status = 'deleted'
  AND  deleted_at > (CAST(to_unixtime(current_timestamp) AS double) - 86400)
ORDER  BY deleted_at DESC
```

### 3.21 Assets that have been moved but never re-reconciled

```sql
SELECT asset_id, filename, s3_path, original_s3_path,
       move_count, last_moved_at, last_reconciled_at
FROM   vast."media-catalog/provenance".assets
WHERE  status = 'moved'
  AND  (last_reconciled_at IS NULL OR last_reconciled_at < last_moved_at)
ORDER  BY last_moved_at DESC
```

### 3.22 Reconciler activity summary — moves and deletes by protocol

```sql
SELECT protocol,
       event_type,
       COUNT(*)  AS event_count
FROM   vast."media-catalog/provenance".asset_moves
GROUP  BY protocol, event_type
ORDER  BY event_count DESC
```
