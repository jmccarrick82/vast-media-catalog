"""Unified asset schema and relationship table schemas for the media catalog.

The `ASSETS_SCHEMA` is the single superset table where every function
writes/updates columns for a given asset. Each function only sets the
columns it owns — the rest remain null until another function fills them.

Separate multi-row tables (relationships, hash_matches, etc.) are defined
here as well for functions that produce multiple rows per asset.
"""

import pyarrow as pa

# ---------------------------------------------------------------------------
# UNIFIED ASSETS TABLE — one row per asset, superset of all metadata columns
# ---------------------------------------------------------------------------
ASSETS_SCHEMA = pa.schema([
    # ── Foundation: metadata-extractor ──
    pa.field("asset_id", pa.string()),           # MD5 of s3_path, primary key
    pa.field("s3_path", pa.string()),
    pa.field("filename", pa.string()),
    pa.field("file_size_bytes", pa.int64()),
    pa.field("duration_seconds", pa.float64()),
    pa.field("video_codec", pa.string()),
    pa.field("audio_codec", pa.string()),
    pa.field("width", pa.int32()),
    pa.field("height", pa.int32()),
    pa.field("fps", pa.float64()),
    pa.field("bitrate", pa.int64()),
    pa.field("pixel_format", pa.string()),
    pa.field("audio_channels", pa.int32()),
    pa.field("audio_sample_rate", pa.int32()),
    pa.field("format_name", pa.string()),
    pa.field("creation_time", pa.string()),
    pa.field("title", pa.string()),
    pa.field("encoder", pa.string()),
    pa.field("ingested_at", pa.float64()),

    # ── Foundation: hash-generator ──
    pa.field("sha256", pa.string()),
    pa.field("perceptual_hash", pa.string()),
    pa.field("hash_computed_at", pa.float64()),

    # ── UC01: Rights Conflict Detection ──
    pa.field("license_type", pa.string()),
    pa.field("territories", pa.string()),
    pa.field("restrictions", pa.string()),
    pa.field("rights_expiry", pa.string()),
    pa.field("conflict_detected", pa.bool_()),
    pa.field("conflict_details", pa.string()),
    pa.field("rights_checked_at", pa.float64()),

    # ── UC02: Orphaned Asset Resolution ──
    pa.field("orphan_resolved_from_asset_id", pa.string()),
    pa.field("orphan_resolution_method", pa.string()),
    pa.field("orphan_resolved_at", pa.float64()),

    # ── UC03: Unauthorized Use Detection (summary) ──
    pa.field("unauthorized_match_count", pa.int32()),
    pa.field("unauthorized_checked_at", pa.float64()),

    # ── UC04: License Audit Trail (summary) ──
    pa.field("licensor", pa.string()),
    pa.field("usage_type", pa.string()),
    pa.field("audit_derivative_count", pa.int32()),
    pa.field("license_audit_at", pa.float64()),

    # ── UC05: Talent & Music Residuals (summary) ──
    pa.field("faces_detected_count", pa.int32()),
    pa.field("music_detected", pa.bool_()),
    pa.field("audio_fingerprint", pa.string()),
    pa.field("talent_music_scanned_at", pa.float64()),

    # ── UC06: Duplicate Storage Elimination (summary) ──
    pa.field("duplicate_count", pa.int32()),
    pa.field("total_storage_savings_bytes", pa.int64()),
    pa.field("duplicates_checked_at", pa.float64()),

    # ── UC07: Safe Deletion ──
    pa.field("dependent_count", pa.int32()),
    pa.field("is_leaf", pa.bool_()),
    pa.field("is_root", pa.bool_()),
    pa.field("deletion_safe", pa.bool_()),
    pa.field("deletion_evaluated_at", pa.float64()),

    # ── UC08: Master vs Derivative Classification ──
    pa.field("asset_classification", pa.string()),   # root / intermediate / leaf / duplicate
    pa.field("classification_confidence", pa.float64()),
    pa.field("classification_at", pa.float64()),

    # ── UC09: Archive Re-Conformation (summary) ──
    pa.field("reconformation_match_count", pa.int32()),
    pa.field("reconformation_viable", pa.bool_()),
    pa.field("reconformation_checked_at", pa.float64()),

    # ── UC10: Version Control ──
    pa.field("version_number", pa.int32()),
    pa.field("previous_version_id", pa.string()),
    pa.field("version_label", pa.string()),
    pa.field("version_recorded_at", pa.float64()),

    # ── UC11: Training Data Provenance ──
    pa.field("training_dataset_id", pa.string()),
    pa.field("is_training_original", pa.bool_()),
    pa.field("rights_cleared_for_training", pa.bool_()),
    pa.field("training_processing_chain", pa.string()),
    pa.field("training_logged_at", pa.float64()),

    # ── UC12: Model Contamination Detection ──
    pa.field("contamination_risk", pa.string()),     # none / low / medium / high
    pa.field("has_ai_processing_upstream", pa.bool_()),
    pa.field("processing_depth", pa.int32()),
    pa.field("contamination_checked_at", pa.float64()),

    # ── UC13: Synthetic Content Tracking ──
    pa.field("ai_probability", pa.float64()),
    pa.field("ai_tool_detected", pa.string()),
    pa.field("ai_model_version", pa.string()),
    pa.field("ai_detection_method", pa.string()),    # JSON array of methods
    pa.field("ai_detected_at", pa.float64()),

    # ── UC14: Bias Audit ──
    pa.field("bias_model_id", pa.string()),
    pa.field("bias_ai_tool_used", pa.string()),
    pa.field("bias_training_data_ids", pa.string()),
    pa.field("bias_audit_result", pa.string()),
    pa.field("bias_risk_level", pa.string()),
    pa.field("bias_audited_at", pa.float64()),

    # ── UC15: Re-Use Discovery (summary) ──
    pa.field("has_semantic_embeddings", pa.bool_()),
    pa.field("embedding_model_name", pa.string()),
    pa.field("embedding_frame_count", pa.int32()),
    pa.field("embeddings_extracted_at", pa.float64()),

    # ── UC16: Clearance Inheritance ──
    pa.field("clearance_status", pa.string()),
    pa.field("clearance_type", pa.string()),
    pa.field("clearance_inherited_from", pa.string()),
    pa.field("clearance_recorded_at", pa.float64()),

    # ── UC17: Compliance Propagation ──
    pa.field("compliance_rating", pa.string()),
    pa.field("content_warnings", pa.string()),
    pa.field("compliance_inherited_from", pa.string()),
    pa.field("compliance_propagated_at", pa.float64()),

    # ── UC18: Localization Management ──
    pa.field("detected_language", pa.string()),
    pa.field("language_confidence", pa.float64()),
    pa.field("dubbed_from_asset_id", pa.string()),
    pa.field("subtitle_tracks", pa.string()),        # JSON array
    pa.field("localization_detected_at", pa.float64()),

    # ── UC19: Leak Investigation (summary) ──
    pa.field("delivery_recipient", pa.string()),
    pa.field("delivery_date", pa.string()),
    pa.field("leak_hash_fingerprint", pa.string()),
    pa.field("delivery_chain", pa.string()),         # JSON
    pa.field("leak_indexed_at", pa.float64()),

    # ── UC20: GDPR Compliance (summary) ──
    pa.field("gdpr_faces_detected", pa.int32()),
    pa.field("gdpr_persons_identified", pa.int32()),
    pa.field("gdpr_blast_radius", pa.int32()),
    pa.field("gdpr_scanned_at", pa.float64()),

    # ── UC21: Chain of Custody ──
    pa.field("legal_hold_active", pa.bool_()),
    pa.field("sha256_at_hold", pa.string()),
    pa.field("hold_placed_at", pa.float64()),
    pa.field("integrity_verified", pa.bool_()),
    pa.field("related_asset_count", pa.int32()),
    pa.field("custody_verified_at", pa.float64()),

    # ── UC22: Ransomware Impact Assessment ──
    pa.field("is_unique_original", pa.bool_()),
    pa.field("has_backup", pa.bool_()),
    pa.field("surviving_derivatives_count", pa.int32()),
    pa.field("recovery_priority", pa.string()),      # CRITICAL / HIGH / MEDIUM / LOW
    pa.field("ransomware_assessed_at", pa.float64()),

    # ── UC23: Content Valuation ──
    pa.field("valuation_derivative_count", pa.int32()),
    pa.field("reuse_count", pa.int32()),
    pa.field("delivery_count", pa.int32()),
    pa.field("commercial_value_score", pa.float64()),
    pa.field("value_tier", pa.string()),             # PREMIUM / HIGH / MEDIUM / LOW
    pa.field("valued_at", pa.float64()),

    # ── UC24: Syndication Revenue Tracking (summary) ──
    pa.field("syndication_licensee_count", pa.int32()),
    pa.field("syndication_territory_count", pa.int32()),
    pa.field("primary_licensee", pa.string()),
    pa.field("primary_territory", pa.string()),
    pa.field("syndication_tracked_at", pa.float64()),

    # ── UC25: Insurance & Disaster Recovery ──
    pa.field("is_irreplaceable", pa.bool_()),
    pa.field("has_digital_copies", pa.bool_()),
    pa.field("digital_copy_count", pa.int32()),
    pa.field("replacement_cost_tier", pa.string()),
    pa.field("commercial_history_score", pa.float64()),
    pa.field("insurance_valued_at", pa.float64()),

    # ── UC26: Co-Production Attribution (summary) ──
    pa.field("primary_production_company", pa.string()),
    pa.field("crew_origin", pa.string()),
    pa.field("ownership_split_pct", pa.float64()),
    pa.field("contribution_type", pa.string()),
    pa.field("attribution_at", pa.float64()),

    # ── Foundation: video-subclip ──
    pa.field("is_subclip", pa.bool_()),                 # True if this asset is a subclip
    pa.field("subclip_parent_asset_id", pa.string()),   # asset_id of parent video
    pa.field("subclip_parent_s3_path", pa.string()),    # s3_path of parent video
    pa.field("subclip_index", pa.int32()),               # 0-based index
    pa.field("subclip_start_seconds", pa.float64()),     # start time in parent video
    pa.field("subclip_duration_seconds", pa.float64()),  # duration of this subclip
    pa.field("subclip_count", pa.int32()),               # (parent only) number of subclips created

    # ── Catalog Reconciler: lifecycle tracking ──
    pa.field("status", pa.string()),              # "active" | "moved" | "deleted"
    pa.field("original_s3_path", pa.string()),    # set on move, tracks first known path
    pa.field("move_count", pa.int32()),            # incremented on each move
    pa.field("last_moved_at", pa.float64()),       # epoch
    pa.field("last_moved_by", pa.string()),        # uid/login_name from audit log
    pa.field("deleted_at", pa.float64()),          # epoch
    pa.field("deleted_by", pa.string()),           # uid/login_name from audit log
    pa.field("last_reconciled_at", pa.float64()),  # last time reconciler confirmed this asset

    # ── AI Analysis: subclip-ai-analyzer (inference endpoint) ──
    pa.field("transcript", pa.string()),             # Whisper transcription of audio
    pa.field("ocr_text", pa.string()),               # Visible text extracted from keyframe (lower thirds, logos, etc.)
    pa.field("scene_description", pa.string()),      # Detailed scene description from vision model
    pa.field("content_tags", pa.string()),            # JSON array of auto-generated tags
    pa.field("ai_content_assessment", pa.string()),  # AI-generated vs real footage analysis
    pa.field("ai_probability_vision", pa.float64()), # Vision model AI probability 0.0-1.0
    pa.field("content_safety_rating", pa.string()),  # "safe" or category of unsafe content
    pa.field("content_summary", pa.string()),         # 1-2 sentence summary
    pa.field("content_category", pa.string()),        # e.g. "Wildlife/Nature", "News", "Music"
    pa.field("content_mood", pa.string()),            # e.g. "Serene", "Energetic", "Tense"
    pa.field("content_rating", pa.string()),          # e.g. "G", "PG", "PG-13", "R"
    pa.field("searchable_keywords", pa.string()),     # JSON array of keywords for search
    pa.field("ai_analyzed_at", pa.float64()),         # epoch timestamp

    # ── Analysis: subclip-ai-analyzer (text embedding → Qdrant) ──
    pa.field("text_embedding_created_at", pa.float64()),
    pa.field("text_embedding_model", pa.string()),
])


# ---------------------------------------------------------------------------
# SEPARATE MULTI-ROW TABLES — relationships and per-item detail tables
# ---------------------------------------------------------------------------

RELATIONSHIPS_SCHEMA = pa.schema([
    pa.field("relationship_id", pa.string()),
    pa.field("parent_asset_id", pa.string()),
    pa.field("child_asset_id", pa.string()),
    pa.field("relationship_type", pa.string()),
    pa.field("confidence", pa.float64()),
    pa.field("created_at", pa.float64()),
])

HASH_MATCHES_SCHEMA = pa.schema([
    pa.field("match_id", pa.string()),
    pa.field("asset_a_id", pa.string()),
    pa.field("asset_b_id", pa.string()),
    pa.field("match_type", pa.string()),
    pa.field("similarity_score", pa.float64()),
    pa.field("storage_savings_bytes", pa.int64()),
    pa.field("reconformation_viable", pa.bool_()),
    pa.field("detected_at", pa.float64()),
])

TALENT_MUSIC_SCHEMA = pa.schema([
    pa.field("detection_id", pa.string()),
    pa.field("asset_id", pa.string()),
    pa.field("s3_path", pa.string()),
    pa.field("detection_type", pa.string()),
    pa.field("label", pa.string()),
    pa.field("confidence", pa.float64()),
    pa.field("start_time_sec", pa.float64()),
    pa.field("end_time_sec", pa.float64()),
    pa.field("detected_at", pa.float64()),
])

SEMANTIC_EMBEDDINGS_SCHEMA = pa.schema([
    pa.field("embedding_id", pa.string()),
    pa.field("asset_id", pa.string()),
    pa.field("s3_path", pa.string()),
    pa.field("frame_index", pa.int32()),
    pa.field("embedding", pa.list_(pa.float32())),
    pa.field("model_name", pa.string()),
    pa.field("extracted_at", pa.float64()),
])

GDPR_PERSONAL_DATA_SCHEMA = pa.schema([
    pa.field("detection_id", pa.string()),
    pa.field("asset_id", pa.string()),
    pa.field("s3_path", pa.string()),
    pa.field("person_id", pa.string()),
    pa.field("data_type", pa.string()),
    pa.field("face_detected", pa.bool_()),
    pa.field("frame_timestamps", pa.string()),
    pa.field("detected_at", pa.float64()),
])

SYNDICATION_RECORDS_SCHEMA = pa.schema([
    pa.field("record_id", pa.string()),
    pa.field("asset_id", pa.string()),
    pa.field("s3_path", pa.string()),
    pa.field("licensee", pa.string()),
    pa.field("territory", pa.string()),
    pa.field("delivery_version_id", pa.string()),
    pa.field("license_status", pa.string()),
    pa.field("tracked_at", pa.float64()),
])

PRODUCTION_ENTITIES_SCHEMA = pa.schema([
    pa.field("attribution_id", pa.string()),
    pa.field("asset_id", pa.string()),
    pa.field("s3_path", pa.string()),
    pa.field("production_company", pa.string()),
    pa.field("crew_origin", pa.string()),
    pa.field("ownership_split_pct", pa.float64()),
    pa.field("contribution_type", pa.string()),
    pa.field("attributed_at", pa.float64()),
])

VERSION_HISTORY_SCHEMA = pa.schema([
    pa.field("version_id", pa.string()),
    pa.field("asset_id", pa.string()),
    pa.field("s3_path", pa.string()),
    pa.field("version_number", pa.int32()),
    pa.field("previous_version_id", pa.string()),
    pa.field("version_label", pa.string()),
    pa.field("created_at", pa.float64()),
])

ASSET_MOVES_SCHEMA = pa.schema([
    pa.field("event_id", pa.string()),         # UUID
    pa.field("asset_id", pa.string()),         # asset_id at time of event
    pa.field("new_asset_id", pa.string()),     # new asset_id after move (null for delete)
    pa.field("event_type", pa.string()),       # "move" | "delete"
    pa.field("old_s3_path", pa.string()),
    pa.field("new_s3_path", pa.string()),      # null for delete
    pa.field("protocol", pa.string()),         # "S3" | "NFS" | "SMB"
    pa.field("rpc_type", pa.string()),         # e.g., "RENAME", "DeleteObject"
    pa.field("performed_by", pa.string()),     # uid or login_name from audit log
    pa.field("client_ip", pa.string()),
    pa.field("detected_at", pa.float64()),     # when reconciler found it
    pa.field("audit_timestamp", pa.float64()), # original timestamp from audit log
])
