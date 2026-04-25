"""Flask backend for the Media Catalog Content Provenance webapp.

Serves API endpoints that query VAST DB via Trino for each use case visualization.
Also serves the built React frontend as static files.
"""

import http.client
import json
import os
import ssl
from urllib.parse import urlparse

import boto3
from botocore.config import Config as BotoConfig
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

from trino_client import TrinoClient

app = Flask(__name__, static_folder="../frontend/dist", static_url_path="/")
CORS(app)

# Load config
CONFIG_PATH = os.environ.get("CONFIG_PATH", "/app/config.json")
with open(CONFIG_PATH) as f:
    config = json.load(f)

trino = TrinoClient(config.get("trino", {}))

# S3 client for file uploads
_s3_cfg = config.get("s3", {})
s3_client = boto3.client(
    "s3",
    endpoint_url=_s3_cfg.get("endpoint"),
    aws_access_key_id=_s3_cfg.get("access_key"),
    aws_secret_access_key=_s3_cfg.get("secret_key"),
    config=BotoConfig(signature_version="s3v4"),
)
S3_UPLOAD_BUCKET = "james-media-catalog"

# Fully-qualified schema path for SQL queries
_bucket = config["vast"]["bucket"]
_schema = config["vast"]["schema"]
_fq = f'{_bucket}/{_schema}'

# All known asset columns grouped by category for the column picker
ASSET_COLUMNS = {
    "Foundation": [
        "filename", "s3_path", "asset_id", "file_size_bytes", "duration_seconds",
        "video_codec", "audio_codec", "width", "height", "fps", "bitrate",
        "pixel_format", "audio_channels", "audio_sample_rate", "format_name",
        "creation_time", "title", "encoder", "ingested_at",
    ],
    "Hashes": ["sha256", "perceptual_hash", "hash_computed_at"],
    "Rights & Licensing": [
        "license_type", "territories", "restrictions", "rights_expiry",
        "conflict_detected", "conflict_details", "rights_checked_at",
    ],
    "Orphan Resolution": [
        "orphan_resolved_from_asset_id", "orphan_resolution_method", "orphan_resolved_at",
    ],
    "Classification": [
        "asset_classification", "classification_confidence", "classification_at",
    ],
    "Duplicates": ["duplicate_count", "total_storage_savings_bytes", "duplicates_checked_at"],
    "Safe Deletion": ["dependent_count", "is_leaf", "is_root", "deletion_safe", "deletion_evaluated_at"],
    "AI Detection": [
        "ai_probability", "ai_tool_detected", "ai_model_version",
        "ai_detection_method", "ai_detected_at",
    ],
    "Training Provenance": [
        "training_dataset_id", "is_training_original", "rights_cleared_for_training",
        "training_processing_chain", "training_logged_at",
    ],
    "Contamination": [
        "contamination_risk", "has_ai_processing_upstream", "processing_depth",
        "contamination_checked_at",
    ],
    "Security & Legal": [
        "legal_hold_active", "sha256_at_hold", "hold_placed_at",
        "integrity_verified", "related_asset_count", "custody_verified_at",
    ],
    "Ransomware Recovery": [
        "is_unique_original", "has_backup", "surviving_derivatives_count", "recovery_priority",
    ],
    "Business Value": [
        "commercial_value_score", "value_tier", "reuse_count",
        "replacement_cost_tier", "is_irreplaceable",
    ],
    "Localization": [
        "detected_language", "language_confidence", "dubbed_from_asset_id",
        "subtitle_tracks", "localization_detected_at",
    ],
    "GDPR": [
        "gdpr_faces_detected", "gdpr_persons_identified", "gdpr_blast_radius", "gdpr_scanned_at",
    ],
    "Leak Investigation": [
        "delivery_recipient", "delivery_date", "leak_hash_fingerprint",
        "delivery_chain", "leak_indexed_at",
    ],
    "Talent & Music": [
        "faces_detected_count", "music_detected", "audio_fingerprint", "talent_music_scanned_at",
    ],
    "Compliance": [
        "compliance_rating", "content_warnings", "compliance_inherited_from", "compliance_propagated_at",
    ],
    "Versioning": [
        "version_number", "previous_version_id", "version_label", "version_recorded_at",
    ],
}

# Default columns shown on the Assets page
DEFAULT_ASSET_COLUMNS = [
    "filename", "s3_path", "asset_id", "file_size_bytes", "duration_seconds",
    "video_codec", "width", "height", "ingested_at",
    "asset_classification", "sha256",
]

# Flat set for validation
_ALL_COLUMNS = {col for cols in ASSET_COLUMNS.values() for col in cols}


def _dedup_asset_rows(rows):
    """Merge duplicate asset rows (same asset_id) by picking non-null values.

    The DataEngine upsert pattern can leave multiple rows per asset_id.
    This merges them so each asset_id appears once with the best available data.
    """
    if not rows:
        return rows
    by_id = {}
    for row in rows:
        aid = row.get("asset_id")
        if aid not in by_id:
            by_id[aid] = dict(row)
        else:
            merged = by_id[aid]
            for k, v in row.items():
                if v is not None and v != "" and (merged.get(k) is None or merged.get(k) == ""):
                    merged[k] = v
    return list(by_id.values())

# ── Persona & Use Case metadata ──────────────────────────────────────────

PERSONAS = [
    {
        "id": 1,
        "name": "Legal & Business Affairs",
        "description": "Rights managers, licensing teams, business affairs executives, general counsel",
        "icon": "scale",
        "use_cases": [1, 2, 3, 4, 5, 21, 26],
    },
    {
        "id": 2,
        "name": "Archive & Library",
        "description": "Archive managers, library directors, media asset managers, digitization teams",
        "icon": "archive",
        "use_cases": [6, 7, 8, 9, 10],
    },
    {
        "id": 3,
        "name": "AI & Data Science",
        "description": "ML engineers, data scientists, AI pipeline architects, model governance teams",
        "icon": "cpu",
        "use_cases": [11, 12, 13, 14],
    },
    {
        "id": 4,
        "name": "Production & Post-Production",
        "description": "Producers, editors, post-production supervisors, localization teams",
        "icon": "film",
        "use_cases": [15, 16, 17, 18],
    },
    {
        "id": 5,
        "name": "Security & IT",
        "description": "CISOs, IT directors, infrastructure security teams, compliance officers",
        "icon": "shield",
        "use_cases": [19, 20, 22],
    },
    {
        "id": 6,
        "name": "Business & Finance",
        "description": "CFOs, COOs, heads of content strategy, M&A teams, insurance",
        "icon": "trending-up",
        "use_cases": [23, 24, 25],
    },
]

USE_CASES = {
    1: {"name": "Rights Conflict Detection", "persona_id": 1, "viz": "graph"},
    2: {"name": "Orphaned Asset Resolution", "persona_id": 1, "viz": "table"},
    3: {"name": "Unauthorized Use Detection", "persona_id": 1, "viz": "table"},
    4: {"name": "License Audit Trail", "persona_id": 1, "viz": "timeline"},
    5: {"name": "Talent & Music Residuals", "persona_id": 1, "viz": "table"},
    6: {"name": "Duplicate Storage Elimination", "persona_id": 2, "viz": "graph"},
    7: {"name": "Safe Deletion", "persona_id": 2, "viz": "tree"},
    8: {"name": "Master vs Derivative Classification", "persona_id": 2, "viz": "graph"},
    9: {"name": "Archive Re-Conformation", "persona_id": 2, "viz": "table"},
    10: {"name": "Version Control Across the Lifecycle", "persona_id": 2, "viz": "timeline"},
    11: {"name": "Training Data Provenance", "persona_id": 3, "viz": "graph"},
    12: {"name": "Model Contamination Detection", "persona_id": 3, "viz": "table"},
    13: {"name": "Synthetic Content Tracking", "persona_id": 3, "viz": "pie"},
    14: {"name": "Bias Audit", "persona_id": 3, "viz": "graph"},
    15: {"name": "Re-Use Discovery", "persona_id": 4, "viz": "grid"},
    16: {"name": "Clearance Inheritance", "persona_id": 4, "viz": "graph"},
    17: {"name": "Compliance Propagation", "persona_id": 4, "viz": "graph"},
    18: {"name": "Localization Management", "persona_id": 4, "viz": "tree"},
    19: {"name": "Leak Investigation", "persona_id": 5, "viz": "timeline"},
    20: {"name": "Regulatory Compliance (GDPR / AI Act)", "persona_id": 5, "viz": "table"},
    21: {"name": "Chain of Custody for Legal Hold", "persona_id": 1, "viz": "timeline"},
    22: {"name": "Cybersecurity — Ransomware Impact", "persona_id": 5, "viz": "dashboard"},
    23: {"name": "Content Valuation", "persona_id": 6, "viz": "bar"},
    24: {"name": "Syndication Revenue Tracking", "persona_id": 6, "viz": "table"},
    25: {"name": "Insurance & Disaster Recovery Valuation", "persona_id": 6, "viz": "dashboard"},
    26: {"name": "Co-Production Attribution", "persona_id": 1, "viz": "pie"},
}


# ── API Routes ────────────────────────────────────────────────────────────

@app.route("/api/personas")
def get_personas():
    return jsonify(PERSONAS)


@app.route("/api/personas/<int:persona_id>/usecases")
def get_persona_usecases(persona_id):
    persona = next((p for p in PERSONAS if p["id"] == persona_id), None)
    if not persona:
        return jsonify({"error": "Persona not found"}), 404
    cases = []
    for uc_id in persona["use_cases"]:
        uc = USE_CASES.get(uc_id, {})
        cases.append({"id": uc_id, **uc})
    return jsonify({"persona": persona, "use_cases": cases})


@app.route("/api/usecases/<int:uc_id>/data")
def get_usecase_data(uc_id):
    """Query Trino for use-case-specific data."""
    if uc_id not in USE_CASES:
        return jsonify({"error": "Use case not found"}), 404

    query_file = os.path.join(os.path.dirname(__file__), "queries", f"uc{uc_id:02d}.sql")
    if not os.path.isfile(query_file):
        return jsonify({"error": "Query not implemented yet"}), 501

    with open(query_file) as f:
        sql = f.read()

    # Optional query params for filtering
    asset_id = request.args.get("asset_id")
    limit = request.args.get("limit", "100")

    sql = sql.replace("{{SCHEMA}}", _fq)
    sql = sql.replace("{{ASSET_ID}}", asset_id or "")
    sql = sql.replace("{{LIMIT}}", limit)

    try:
        rows, columns = trino.execute(sql)
        return jsonify({
            "use_case": USE_CASES[uc_id],
            "columns": columns,
            "rows": rows,
            "count": len(rows),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/assets/columns")
def get_asset_columns():
    """Return all available columns grouped by category, plus the defaults."""
    return jsonify({
        "groups": ASSET_COLUMNS,
        "defaults": DEFAULT_ASSET_COLUMNS,
    })


@app.route("/api/assets")
def get_assets():
    """List assets. Accepts ?columns=, ?search=, ?limit= query params."""
    limit = request.args.get("limit", "50")
    search = request.args.get("search", "").strip()
    cols_param = request.args.get("columns", "")
    if cols_param:
        requested = [c.strip() for c in cols_param.split(",") if c.strip()]
        # Always include asset_id for row-click navigation
        if "asset_id" not in requested:
            requested.insert(0, "asset_id")
        # Validate — only allow known columns
        selected = [c for c in requested if c in _ALL_COLUMNS]
    else:
        selected = list(DEFAULT_ASSET_COLUMNS)

    col_sql = ", ".join(selected)

    show_subclips = request.args.get("show_subclips", "false").lower() == "true"
    conditions = []
    if not show_subclips:
        conditions.append("(is_subclip IS NULL OR CAST(is_subclip AS VARCHAR) != 'true')")
    if search:
        safe_search = search.replace("'", "''")
        conditions.append(f"LOWER(filename) LIKE LOWER('%{safe_search}%')")
    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    sql = f"""
        SELECT {col_sql}
        FROM vast."{_fq}".assets
        {where_clause}
        ORDER BY ingested_at DESC
        LIMIT {limit}
    """
    try:
        rows, columns = trino.execute(sql)
        rows = _dedup_asset_rows(rows)
        return jsonify({"columns": columns, "rows": rows, "count": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/assets/<asset_id>")
def get_asset_detail(asset_id):
    """Get full detail for a single asset (all columns)."""
    sql = f"""
        SELECT *
        FROM vast."{_fq}".assets
        WHERE asset_id = '{asset_id}'
    """
    try:
        rows, columns = trino.execute(sql)
        rows = _dedup_asset_rows(rows)
        if not rows:
            return jsonify({"error": "Asset not found"}), 404
        return jsonify({"columns": columns, "data": rows[0]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/assets/<asset_id>/relationships")
def get_asset_relationships(asset_id):
    """Get relationship graph edges for an asset."""
    sql = f"""
        SELECT relationship_id, parent_asset_id, child_asset_id,
               relationship_type, confidence, created_at
        FROM vast."{_fq}".relationships
        WHERE parent_asset_id = '{asset_id}' OR child_asset_id = '{asset_id}'
        ORDER BY created_at DESC
    """
    try:
        rows, columns = trino.execute(sql)
        return jsonify({"columns": columns, "rows": rows, "count": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/assets/<asset_id>/detail")
def get_asset_full_detail(asset_id):
    """Get comprehensive dossier for a single asset: primary row + all secondary tables."""
    safe_id = asset_id.replace("'", "''")

    queries = {
        "asset": f'SELECT * FROM vast."{_fq}".assets WHERE asset_id = \'{safe_id}\'',
        "subclips": f"""
            SELECT asset_id, filename, s3_path, file_size_bytes, duration_seconds,
                   video_codec, width, height, subclip_index, subclip_start_seconds,
                   subclip_duration_seconds,
                   content_summary, content_category, content_mood, content_rating,
                   content_safety_rating, content_tags, searchable_keywords,
                   scene_description, ocr_text, transcript, ai_content_assessment,
                   ai_analyzed_at
            FROM vast."{_fq}".assets
            WHERE CAST(subclip_parent_asset_id AS VARCHAR) = '{safe_id}'
            ORDER BY CAST(subclip_index AS INTEGER)
            LIMIT 200
        """,
        "relationships": f"""
            SELECT r.relationship_id, r.parent_asset_id, r.child_asset_id,
                   r.relationship_type, r.confidence, r.created_at,
                   p.filename AS parent_filename, c.filename AS child_filename
            FROM vast."{_fq}".relationships r
            LEFT JOIN (
                SELECT asset_id, MAX(filename) AS filename
                FROM vast."{_fq}".assets
                GROUP BY asset_id
            ) p ON r.parent_asset_id = p.asset_id
            LEFT JOIN (
                SELECT asset_id, MAX(filename) AS filename
                FROM vast."{_fq}".assets
                GROUP BY asset_id
            ) c ON r.child_asset_id = c.asset_id
            WHERE r.parent_asset_id = '{safe_id}' OR r.child_asset_id = '{safe_id}'
            ORDER BY r.created_at DESC
            LIMIT 200
        """,
        "hash_matches": f"""
            SELECT hm.match_id, hm.asset_a_id, hm.asset_b_id,
                   hm.match_type, hm.similarity_score, hm.storage_savings_bytes,
                   a.filename AS asset_a_filename, b.filename AS asset_b_filename
            FROM vast."{_fq}".hash_matches hm
            LEFT JOIN (
                SELECT asset_id, MAX(filename) AS filename
                FROM vast."{_fq}".assets
                GROUP BY asset_id
            ) a ON hm.asset_a_id = a.asset_id
            LEFT JOIN (
                SELECT asset_id, MAX(filename) AS filename
                FROM vast."{_fq}".assets
                GROUP BY asset_id
            ) b ON hm.asset_b_id = b.asset_id
            WHERE hm.asset_a_id = '{safe_id}' OR hm.asset_b_id = '{safe_id}'
            ORDER BY hm.similarity_score DESC
            LIMIT 200
        """,
        "version_history": f"""
            SELECT vh.version_id, vh.asset_id, vh.version_number, vh.previous_version_id,
                   vh.version_label, vh.created_at,
                   p.filename AS previous_version_filename
            FROM vast."{_fq}".version_history vh
            LEFT JOIN vast."{_fq}".assets p ON vh.previous_version_id = p.asset_id
            WHERE vh.asset_id = '{safe_id}'
            ORDER BY vh.version_number
            LIMIT 200
        """,
        "talent_music": f"""
            SELECT detection_id, detection_type, label, confidence,
                   start_time_sec, end_time_sec, detected_at
            FROM vast."{_fq}".talent_music
            WHERE asset_id = '{safe_id}'
            ORDER BY start_time_sec
            LIMIT 200
        """,
        "gdpr_personal_data": f"""
            SELECT detection_id, person_id, data_type, face_detected,
                   frame_timestamps, detected_at
            FROM vast."{_fq}".gdpr_personal_data
            WHERE asset_id = '{safe_id}'
            ORDER BY detected_at
            LIMIT 200
        """,
        "syndication_records": f"""
            SELECT record_id, licensee, territory, delivery_version_id,
                   license_status, tracked_at
            FROM vast."{_fq}".syndication_records
            WHERE asset_id = '{safe_id}'
            ORDER BY tracked_at DESC
            LIMIT 200
        """,
        "production_entities": f"""
            SELECT attribution_id, production_company, crew_origin,
                   ownership_split_pct, contribution_type, attributed_at
            FROM vast."{_fq}".production_entities
            WHERE asset_id = '{safe_id}'
            ORDER BY ownership_split_pct DESC
            LIMIT 200
        """,
    }

    result = {}
    for key, sql in queries.items():
        try:
            rows, columns = trino.execute(sql)
            if key == "asset":
                rows = _dedup_asset_rows(rows)
                result[key] = {"columns": columns, "data": rows[0] if rows else None}
            elif key == "hash_matches":
                # Deduplicate: keep one row per unique other-asset
                seen = set()
                unique = []
                for r in rows:
                    other = r.get("asset_b_id") if r.get("asset_a_id") == safe_id else r.get("asset_a_id")
                    if other not in seen:
                        seen.add(other)
                        unique.append(r)
                result[key] = {"columns": columns, "rows": unique, "count": len(unique)}
            else:
                result[key] = {"columns": columns, "rows": rows, "count": len(rows)}
        except Exception as e:
            if key == "asset":
                result[key] = {"columns": [], "data": None, "error": str(e)}
            else:
                result[key] = {"columns": [], "rows": [], "count": 0, "error": str(e)}

    if not result.get("asset", {}).get("data"):
        return jsonify({"error": "Asset not found"}), 404

    # Override duplicate_count with the actual deduped count
    hm_count = result.get("hash_matches", {}).get("count", 0)
    if result["asset"]["data"]:
        result["asset"]["data"]["duplicate_count"] = hm_count

    return jsonify(result)


@app.route("/api/stats")
def get_stats():
    """Get overall system statistics."""
    queries = {
        "total_assets": f'SELECT COUNT(*) as cnt FROM vast."{_fq}".assets',
        "classified_assets": f'SELECT COUNT(*) as cnt FROM vast."{_fq}".assets WHERE asset_classification IS NOT NULL',
        "total_relationships": f'SELECT COUNT(*) as cnt FROM vast."{_fq}".relationships',
        "conflicts_detected": f'SELECT COUNT(*) as cnt FROM vast."{_fq}".assets WHERE conflict_detected = true',
        "ai_generated": f'SELECT COUNT(*) as cnt FROM vast."{_fq}".assets WHERE ai_probability > 0.7',
        "unique_originals": f'SELECT COUNT(*) as cnt FROM vast."{_fq}".assets WHERE is_unique_original = true',
    }
    stats = {}
    for key, sql in queries.items():
        try:
            rows, _ = trino.execute(sql)
            stats[key] = rows[0]["cnt"] if rows else 0
        except Exception:
            stats[key] = 0
    return jsonify(stats)


# ── Upload endpoint ─────────────────────────────────────────────────────

@app.route("/api/upload", methods=["POST"])
def upload_file():
    """Upload one or more video files directly to the S3 bucket."""
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    files = request.files.getlist("file")
    results = []
    for f in files:
        if not f.filename:
            continue
        key = f.filename
        try:
            s3_client.upload_fileobj(f.stream, S3_UPLOAD_BUCKET, key)
            results.append({"filename": key, "s3_path": f"s3://{S3_UPLOAD_BUCKET}/{key}", "status": "ok"})
        except Exception as e:
            results.append({"filename": key, "status": "error", "error": str(e)})

    if not results:
        return jsonify({"error": "No valid files in request"}), 400
    return jsonify({"uploaded": results})


# ── Video streaming proxy ──────────────────────────────────────────────
#
# The S3 endpoint is on the internal VAST network — browsers on the
# public internet can't fetch presigned URLs directly. Proxy object
# bytes through the webapp and honor HTTP Range so <video> seeking
# works. Only two buckets are allowed; anything else is a 403.

from flask import Response, stream_with_context

STREAMABLE_BUCKETS = {
    "james-media-catalog",
    "james-media-subclips",
    "james-media-clips",          # raw AI-extracted clips (Phase 2)
    "james-media-deliveries",     # C2PA-signed delivery renditions (Phase 3)
    "james-media-inbox",          # rare but useful for debugging
    "james-media-qc-passed",
    "james-media-qc-failed",
}


def _parse_s3_path(s3_path: str) -> tuple[str, str]:
    if not s3_path or not s3_path.startswith("s3://"):
        raise ValueError("path must start with s3://")
    rest = s3_path[len("s3://"):]
    if "/" not in rest:
        raise ValueError("path must be s3://bucket/key")
    bucket, key = rest.split("/", 1)
    return bucket, key


@app.route("/api/video")
def stream_video():
    """GET /api/video?path=s3://bucket/key — range-enabled S3 proxy."""
    s3_path = request.args.get("path") or ""
    try:
        bucket, key = _parse_s3_path(s3_path)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    if bucket not in STREAMABLE_BUCKETS:
        return jsonify({"error": f"bucket '{bucket}' not allowed"}), 403

    range_header = request.headers.get("Range", "")
    s3_kwargs = {"Bucket": bucket, "Key": key}
    if range_header:
        s3_kwargs["Range"] = range_header

    try:
        obj = s3_client.get_object(**s3_kwargs)
    except Exception as e:
        return jsonify({"error": f"S3 fetch failed: {e}"}), 502

    status = 206 if range_header else 200
    headers = {
        "Content-Type": obj.get("ContentType") or "video/mp4",
        "Accept-Ranges": "bytes",
        "Content-Length": str(obj.get("ContentLength", "")),
    }
    if "ContentRange" in obj:
        headers["Content-Range"] = obj["ContentRange"]

    def generate():
        body = obj["Body"]
        try:
            for chunk in body.iter_chunks(chunk_size=64 * 1024):
                yield chunk
        finally:
            body.close()

    return Response(stream_with_context(generate()), status=status, headers=headers)


# ── Semantic search (Qdrant-backed) ─────────────────────────────────────
#
# Writes happen inside the subclip-ai-analyzer DataEngine function: once
# it has transcript + OCR + scene + summary in hand, it embeds that text
# via /v1/embeddings and upserts a Qdrant point keyed by asset_id. This
# endpoint just embeds the user's query with the same model and runs a
# similarity search. No separate indexing worker needed.

INFERENCE_HOST = (
    os.environ.get("INFERENCE_HOST")
    or config.get("inference", {}).get("host")
    or "inference.selab.vastdata.com"
)
INFERENCE_KEY = (
    os.environ.get("INFERENCE_KEY")
    or config.get("inference", {}).get("api_key")
    or ""
)
EMBED_MODEL = (
    os.environ.get("EMBED_MODEL")
    or config.get("inference", {}).get("embed_model")
    or "nvidia/nv-embed-v1"
)

QDRANT_URL = (
    os.environ.get("QDRANT_URL")
    or config.get("qdrant", {}).get("url")
    or "http://qdrant:6333"
)
QDRANT_COLLECTION = (
    os.environ.get("QDRANT_COLLECTION")
    or config.get("qdrant", {}).get("collection")
    or "subclips"
)


# Query wrapper. nv-embed-v1 is a QA retriever that's trained on natural-language
# questions, so single-word/keyword queries ("city", "person talking") produce
# weak embeddings and score poorly against our passage embeddings. Wrapping the
# user's input in an explicit directive turns it into a sentence-form query and
# makes intent unambiguous — matches go up substantially for short inputs.
QUERY_PROMPT_PREFIX = (
    "Please include matches that depict, describe, show, or mention: "
)


def _embed_query(text: str) -> list[float]:
    """Call /v1/embeddings with a directive-wrapped query string.

    The user's raw input is substituted into a fixed prefix so the embedder
    always sees a full sentence — keyword queries score much better this way.
    """
    wrapped = QUERY_PROMPT_PREFIX + text
    body = json.dumps({
        "model": EMBED_MODEL,
        "input": [wrapped],
        "encoding_format": "float",
        "input_type": "query",
    })
    conn = http.client.HTTPSConnection(INFERENCE_HOST, timeout=30)
    try:
        conn.request("POST", "/v1/embeddings", body=body, headers={
            "Authorization": f"Bearer {INFERENCE_KEY}",
            "Content-Type": "application/json",
        })
        resp = conn.getresponse()
        raw = resp.read().decode("utf-8")
        if resp.status != 200:
            raise RuntimeError(f"/v1/embeddings {resp.status}: {raw[:300]}")
        data = json.loads(raw)
    finally:
        conn.close()
    return data["data"][0]["embedding"]


def _qdrant_search(vector: list[float], limit: int) -> list[dict]:
    """POST to Qdrant REST /collections/{c}/points/search."""
    body = json.dumps({
        "vector": vector,
        "limit": limit,
        "with_payload": True,
    })
    parsed = urlparse(QDRANT_URL)
    host = parsed.hostname
    port = parsed.port or (443 if parsed.scheme == "https" else 6333)
    if parsed.scheme == "https":
        conn = http.client.HTTPSConnection(host, port, timeout=15,
                                           context=ssl.create_default_context())
    else:
        conn = http.client.HTTPConnection(host, port, timeout=15)
    try:
        conn.request(
            "POST",
            f"/collections/{QDRANT_COLLECTION}/points/search",
            body=body,
            headers={"Content-Type": "application/json"},
        )
        resp = conn.getresponse()
        raw = resp.read().decode("utf-8")
        if resp.status != 200:
            raise RuntimeError(f"Qdrant search {resp.status}: {raw[:300]}")
        return json.loads(raw).get("result", [])
    finally:
        conn.close()


@app.route("/api/semantic-search")
def semantic_search():
    """Semantic search over subclip summary/OCR/transcript embeddings.

    Query params:
      q     — search string (required)
      limit — max results (default 10, cap 50)
    """
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify({"error": "missing ?q=<query>"}), 400
    try:
        limit = min(int(request.args.get("limit", "10")), 50)
    except ValueError:
        limit = 10

    try:
        vec = _embed_query(q)
    except Exception as e:
        return jsonify({"error": f"embedding failed: {e}"}), 502

    try:
        hits = _qdrant_search(vec, limit)
    except Exception as e:
        return jsonify({"error": f"qdrant search failed: {e}"}), 502

    results = []
    for h in hits:
        p = h.get("payload") or {}
        results.append({
            "asset_id": p.get("asset_id") or str(h.get("id")),
            "score": float(h.get("score", 0)),
            "s3_path": p.get("s3_path"),
            "filename": p.get("filename"),
            "subclip_index": p.get("subclip_index"),
            "content_summary": p.get("content_summary"),
            "content_category": p.get("content_category"),
            "content_mood": p.get("content_mood"),
            "content_rating": p.get("content_rating"),
            "searchable_keywords": p.get("searchable_keywords"),
            "embedded_text": p.get("embedded_text"),
        })

    return jsonify({
        "query": q,
        "limit": limit,
        "count": len(results),
        "results": results,
    })


# ── Function configs (runtime-editable knobs) ──────────────────────────
#
# Reads from the function_configs VAST DB table and exposes the settings
# as a simple REST API for the /settings GUI. A row has:
#   scope, key, value, value_type, default_value, description, min/max,
#   ui_group, ui_order, updated_at, updated_by
# The table is seeded by scripts/seed_function_configs.py.

# Lazy vastdb import so the webapp still works if vastdb is absent.
_vast_session_cache = {"session": None, "at": 0}

def _vastdb_session():
    import time
    now = time.time()
    # Recreate every 5 min — vastdb sessions are cheap but let's not leak
    if _vast_session_cache["session"] is None or (now - _vast_session_cache["at"]) > 300:
        import vastdb
        _vast_session_cache["session"] = vastdb.connect(
            endpoint=config["vast"]["endpoint"],
            access=config["vast"]["access_key"],
            secret=config["vast"]["secret_key"],
        )
        _vast_session_cache["at"] = now
    return _vast_session_cache["session"]


def _row_for_api(r: dict) -> dict:
    """Transform a function_configs row into what the UI wants.

    The DB stores `value`/`default_value`/`min_value`/`max_value` as
    JSON-encoded strings (so any type can round-trip). Decode them here
    so the UI gets a proper number/bool/array.
    """
    def _dec(raw):
        if raw is None or raw == "":
            return None
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return raw
    return {
        "scope":         r.get("scope"),
        "key":           r.get("key"),
        "value":         _dec(r.get("value")),
        "value_type":    r.get("value_type"),
        "default_value": _dec(r.get("default_value")),
        "description":   r.get("description"),
        "min":           _dec(r.get("min_value")),
        "max":           _dec(r.get("max_value")),
        "ui_group":      r.get("ui_group"),
        "ui_order":      r.get("ui_order"),
        "updated_at":    r.get("updated_at"),
        "updated_by":    r.get("updated_by"),
    }


@app.route("/api/configs")
def list_configs():
    """List every scope with counts. Cheap — single table scan."""
    import ibis
    try:
        with _vastdb_session().transaction() as tx:
            tbl = tx.bucket(_bucket).schema(_schema).table("function_configs")
            t = tbl.select().read_all()
    except Exception as e:
        return jsonify({"error": f"config table query failed: {e}"}), 502

    scopes = {}
    for r in t.to_pylist():
        sc = r.get("scope") or "_unscoped"
        scopes.setdefault(sc, 0)
        scopes[sc] += 1
    return jsonify({"scopes": [{"scope": s, "count": c} for s, c in sorted(scopes.items())]})


@app.route("/api/configs/<scope>")
def get_scope(scope):
    """All settings for one scope, grouped/ordered for UI rendering."""
    import ibis
    try:
        with _vastdb_session().transaction() as tx:
            tbl = tx.bucket(_bucket).schema(_schema).table("function_configs")
            t = tbl.select(predicate=ibis._.scope == scope).read_all()
    except Exception as e:
        return jsonify({"error": f"config query failed: {e}"}), 502

    rows = [_row_for_api(r) for r in t.to_pylist()]
    rows.sort(key=lambda r: (r.get("ui_group") or "", r.get("ui_order") or 0, r.get("key") or ""))

    groups = {}
    for r in rows:
        g = r.get("ui_group") or "General"
        groups.setdefault(g, []).append(r)

    return jsonify({
        "scope": scope,
        "count": len(rows),
        "groups": [{"name": g, "settings": items} for g, items in groups.items()],
    })


@app.route("/api/configs/<scope>/<key>", methods=["PUT"])
def update_config(scope, key):
    """Update one setting. Body: {"value": <json-encoded value>, "updated_by": "user@..." (optional)}.

    The `value` in the body can be any JSON-decodable type — we re-encode
    it as a string for storage so types round-trip.
    """
    import ibis
    import pyarrow as pa
    import time as _time

    body = request.get_json(silent=True) or {}
    if "value" not in body:
        return jsonify({"error": "body must include {value: ...}"}), 400

    new_val_json = json.dumps(body["value"])
    updated_by = (body.get("updated_by") or "webapp").strip()[:128]
    ROW_ID = "$row_id"

    try:
        session = _vastdb_session()
        with session.transaction() as tx:
            tbl = tx.bucket(_bucket).schema(_schema).table("function_configs")
            t = tbl.select(
                predicate=(ibis._.scope == scope) & (ibis._.key == key),
                internal_row_id=True,
            ).read_all()
            rows = t.to_pylist()
            if not rows:
                return jsonify({"error": f"no such setting {scope}:{key}"}), 404
            row = rows[0]
            # Build minimal update: value + updated_at + updated_by
            schema = tbl.columns()
            upd_cols = [f for f in schema if f.name in ("value", "updated_at", "updated_by")]
            upd_schema = pa.schema([pa.field(ROW_ID, pa.uint64())] + upd_cols)
            rb = pa.RecordBatch.from_arrays(
                [pa.array([row[ROW_ID]], type=pa.uint64()),
                 pa.array([new_val_json], type=pa.string()),
                 pa.array([_time.time()], type=pa.float64()),
                 pa.array([updated_by], type=pa.string())],
                schema=upd_schema,
            )
            tbl.update(rb)
    except Exception as e:
        return jsonify({"error": f"update failed: {e}"}), 502

    return jsonify({"ok": True, "scope": scope, "key": key,
                    "value": body["value"], "updated_by": updated_by})


@app.route("/api/configs/<scope>", methods=["PUT"])
def bulk_update_scope(scope):
    """Bulk-update many settings in one scope in a single transaction.

    Body shape:
      {
        "updates": [
          {"key": "black_frame_max_ratio_fail", "value": 0.45},
          {"key": "loudness_enabled",           "value": false},
          ...
        ],
        "updated_by": "webapp"   (optional)
      }

    Unknown keys are ignored and listed back under `skipped`.
    """
    import ibis, pyarrow as pa, time as _time
    body = request.get_json(silent=True) or {}
    updates = body.get("updates") or []
    if not isinstance(updates, list) or not updates:
        return jsonify({"error": "body must include non-empty updates: [...]"}), 400
    updated_by = (body.get("updated_by") or "webapp").strip()[:128]

    # Normalize: {key -> value}. Reject rows without a key.
    wanted = {}
    for u in updates:
        k = u.get("key")
        if not k:
            return jsonify({"error": f"update missing key: {u}"}), 400
        wanted[k] = u.get("value")

    ROW_ID = "$row_id"
    applied, skipped = [], []
    try:
        session = _vastdb_session()
        with session.transaction() as tx:
            tbl = tx.bucket(_bucket).schema(_schema).table("function_configs")
            t = tbl.select(
                predicate=ibis._.scope == scope,
                internal_row_id=True,
            ).read_all().to_pylist()

            rows_by_key = {r.get("key"): r for r in t}
            cols = tbl.columns()
            upd_cols = [f for f in cols if f.name in ("value", "updated_at", "updated_by")]
            upd_schema = pa.schema([pa.field(ROW_ID, pa.uint64())] + upd_cols)

            now = _time.time()
            for k, v in wanted.items():
                row = rows_by_key.get(k)
                if not row:
                    skipped.append({"key": k, "reason": "not found"})
                    continue
                rb = pa.RecordBatch.from_arrays(
                    [pa.array([row[ROW_ID]], type=pa.uint64()),
                     pa.array([json.dumps(v)], type=pa.string()),
                     pa.array([now], type=pa.float64()),
                     pa.array([updated_by], type=pa.string())],
                    schema=upd_schema,
                )
                tbl.update(rb)
                applied.append(k)
    except Exception as e:
        return jsonify({"error": f"bulk update failed: {e}"}), 502

    return jsonify({"ok": True, "scope": scope, "applied": applied, "skipped": skipped})


@app.route("/api/configs/<scope>/reset", methods=["POST"])
def reset_scope(scope):
    """Reset every setting in a scope back to its default_value.

    Useful for "Restore defaults" in the UI. Fire-and-forget on the whole
    scope — no body required.
    """
    import ibis, pyarrow as pa, time as _time
    ROW_ID = "$row_id"
    reset_keys = []
    try:
        session = _vastdb_session()
        with session.transaction() as tx:
            tbl = tx.bucket(_bucket).schema(_schema).table("function_configs")
            t = tbl.select(
                predicate=ibis._.scope == scope,
                internal_row_id=True,
            ).read_all().to_pylist()

            if not t:
                return jsonify({"ok": True, "scope": scope, "reset": []})

            cols = tbl.columns()
            upd_cols = [f for f in cols if f.name in ("value", "updated_at", "updated_by")]
            upd_schema = pa.schema([pa.field(ROW_ID, pa.uint64())] + upd_cols)

            now = _time.time()
            for row in t:
                default = row.get("default_value") or ""
                rb = pa.RecordBatch.from_arrays(
                    [pa.array([row[ROW_ID]], type=pa.uint64()),
                     pa.array([default], type=pa.string()),
                     pa.array([now], type=pa.float64()),
                     pa.array(["webapp(reset_all)"], type=pa.string())],
                    schema=upd_schema,
                )
                tbl.update(rb)
                reset_keys.append(row.get("key"))
    except Exception as e:
        return jsonify({"error": f"scope reset failed: {e}"}), 502

    return jsonify({"ok": True, "scope": scope, "reset": reset_keys})


@app.route("/api/configs/<scope>/<key>/reset", methods=["POST"])
def reset_config(scope, key):
    """Set value back to default_value."""
    import ibis, pyarrow as pa, time as _time
    ROW_ID = "$row_id"
    try:
        session = _vastdb_session()
        with session.transaction() as tx:
            tbl = tx.bucket(_bucket).schema(_schema).table("function_configs")
            t = tbl.select(
                predicate=(ibis._.scope == scope) & (ibis._.key == key),
                internal_row_id=True,
            ).read_all()
            rows = t.to_pylist()
            if not rows:
                return jsonify({"error": f"no such setting {scope}:{key}"}), 404
            row = rows[0]
            default = row.get("default_value")
            schema = tbl.columns()
            upd_cols = [f for f in schema if f.name in ("value", "updated_at", "updated_by")]
            upd_schema = pa.schema([pa.field(ROW_ID, pa.uint64())] + upd_cols)
            rb = pa.RecordBatch.from_arrays(
                [pa.array([row[ROW_ID]], type=pa.uint64()),
                 pa.array([default], type=pa.string()),
                 pa.array([_time.time()], type=pa.float64()),
                 pa.array(["webapp(reset)"], type=pa.string())],
                schema=upd_schema,
            )
            tbl.update(rb)
    except Exception as e:
        return jsonify({"error": f"reset failed: {e}"}), 502
    return jsonify({"ok": True, "scope": scope, "key": key})


# ── Phase 3: delivery packages + C2PA verification ──────────────────────
#
# Data is in three VAST DB tables (all in the `media-catalog` schema):
#   - delivery_packages     (one row per package)
#   - package_renditions    (many per package; one per rendition)
#   - extracted_clips       (many per source; one per AI-selected clip)
#
# The sidecar manifest lives at s3://<deliveries_bucket>/<package_id>/manifest.json
# Each rendition MP4 carries an embedded C2PA manifest we re-read with
# c2patool on demand — proves the signature survives in the file itself.

C2PATOOL_BINARY = os.environ.get("C2PATOOL_BINARY", "/usr/local/bin/c2patool")


def _json_safe(v):
    """Round-trip a cell from the DB to something Flask can jsonify.

    vastdb sometimes hands back NaN for nullable numeric columns; those
    break the JSON serializer. Coerce them to None.
    """
    import math
    if isinstance(v, float) and math.isnan(v):
        return None
    return v


def _row_to_dict(row: dict) -> dict:
    return {k: _json_safe(v) for k, v in (row or {}).items()}


def _parse_json_or_none(s):
    if not s:
        return None
    try:
        return json.loads(s)
    except (json.JSONDecodeError, TypeError):
        return None


@app.route("/api/packages")
def list_packages():
    """List delivery packages with source-filename + summary counts.

    Response shape:
      {"packages": [{package_id, source_id, source_filename, clip_count,
                     rendition_count, c2pa_signed_count, status,
                     created_at, updated_at}, ...]}
    """
    import ibis
    try:
        with _vastdb_session().transaction() as tx:
            pkg_tbl = tx.bucket(_bucket).schema(_schema).table("delivery_packages")
            pkgs = pkg_tbl.select().read_all().to_pylist()
            src_tbl = tx.bucket(_bucket).schema(_schema).table("source_videos")
            srcs = src_tbl.select().read_all().to_pylist()
    except Exception as e:
        return jsonify({"error": f"packages query failed: {e}"}), 502

    src_by_id = {s.get("source_id"): s for s in srcs}
    out = []
    for p in pkgs:
        src = src_by_id.get(p.get("source_id")) or {}
        # Fall back through any s3 path on the source row when filename
        # is missing (orphan rows from direct-to-qc-passed test uploads).
        fname = src.get("filename")
        if not fname:
            for k in ("current_s3_path", "s3_inbox_path"):
                v = src.get(k)
                if v:
                    fname = v.rsplit("/", 1)[-1]
                    break
        out.append({
            "package_id":        p.get("package_id"),
            "source_id":         p.get("source_id"),
            "source_filename":   fname,
            "clip_count":        _json_safe(p.get("clip_count")),
            "rendition_count":   _json_safe(p.get("rendition_count")),
            "total_size_bytes":  _json_safe(p.get("total_size_bytes")),
            "c2pa_enabled":      bool(p.get("c2pa_enabled")),
            "c2pa_signed_count": _json_safe(p.get("c2pa_signed_count")) or 0,
            "c2pa_signer":       p.get("c2pa_signer"),
            "status":            p.get("status"),
            "package_root_s3_path": p.get("package_root_s3_path"),
            "created_at":        _json_safe(p.get("created_at")),
            "updated_at":        _json_safe(p.get("updated_at")),
        })
    # Sort newest-first
    out.sort(key=lambda r: r.get("created_at") or 0, reverse=True)
    return jsonify({"packages": out, "count": len(out)})


@app.route("/api/packages/<package_id>")
def get_package(package_id):
    """Full detail for one package: source row + all clips + all renditions.

    Clips are ordered by clip_index. Renditions are grouped under each clip
    for easier UI rendering. Licensing fields are decoded from JSON strings.
    """
    import ibis
    try:
        with _vastdb_session().transaction() as tx:
            pkg_tbl = tx.bucket(_bucket).schema(_schema).table("delivery_packages")
            rows = pkg_tbl.select(
                predicate=ibis._.package_id == package_id,
            ).read_all().to_pylist()
            if not rows:
                return jsonify({"error": f"no package {package_id}"}), 404
            pkg = rows[0]

            src_tbl = tx.bucket(_bucket).schema(_schema).table("source_videos")
            src_rows = src_tbl.select(
                predicate=ibis._.source_id == pkg.get("source_id"),
            ).read_all().to_pylist()
            source_row = src_rows[0] if src_rows else {}

            clips_tbl = tx.bucket(_bucket).schema(_schema).table("extracted_clips")
            clip_rows = clips_tbl.select(
                predicate=ibis._.source_id == pkg.get("source_id"),
            ).read_all().to_pylist()

            rend_tbl = tx.bucket(_bucket).schema(_schema).table("package_renditions")
            rend_rows = rend_tbl.select(
                predicate=ibis._.package_id == package_id,
            ).read_all().to_pylist()
    except Exception as e:
        return jsonify({"error": f"package query failed: {e}"}), 502

    # Decode licensing JSON blobs for easier consumption
    package = _row_to_dict(pkg)
    package["rights_cleared_for"] = _parse_json_or_none(pkg.get("rights_cleared_for_json")) or []
    package["restrictions"]       = _parse_json_or_none(pkg.get("restrictions_json")) or []
    package["c2pa_errors"]        = _parse_json_or_none(pkg.get("c2pa_errors_json")) or []

    # Group renditions under their clip_id
    rends_by_clip: dict = {}
    for r in rend_rows:
        rends_by_clip.setdefault(r.get("clip_id"), []).append(_row_to_dict(r))
    # Stable order within each clip: preset name
    for cid, lst in rends_by_clip.items():
        lst.sort(key=lambda r: (r.get("rendition_name") or ""))

    clips = [_row_to_dict(c) for c in clip_rows]
    clips.sort(key=lambda c: c.get("clip_index") or 0)
    # Parse frame scores json for UI
    for c in clips:
        c["frame_scores"] = _parse_json_or_none(c.get("frame_scores_json")) or []
        c["renditions"]   = rends_by_clip.get(c.get("clip_id"), [])

    return jsonify({
        "package":  package,
        "source":   _row_to_dict(source_row),
        "clips":    clips,
    })


@app.route("/api/packages/<package_id>/manifest")
def get_package_manifest(package_id):
    """Fetch the sidecar manifest.json for a package. Passthrough from S3."""
    import ibis
    try:
        with _vastdb_session().transaction() as tx:
            tbl = tx.bucket(_bucket).schema(_schema).table("delivery_packages")
            rows = tbl.select(
                predicate=ibis._.package_id == package_id,
            ).read_all().to_pylist()
            if not rows:
                return jsonify({"error": f"no package {package_id}"}), 404
            manifest_s3 = rows[0].get("manifest_s3_path")
            if not manifest_s3:
                return jsonify({"error": "no sidecar manifest"}), 404
    except Exception as e:
        return jsonify({"error": f"package query failed: {e}"}), 502

    try:
        bucket, key = _parse_s3_path(manifest_s3)
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        return Response(body, status=200,
                        headers={"Content-Type": "application/json"})
    except Exception as e:
        return jsonify({"error": f"manifest fetch failed: {e}"}), 502


@app.route("/api/packages/<package_id>/renditions/<rendition_id>/c2pa")
def get_rendition_c2pa(package_id, rendition_id):
    """Live C2PA verify: download the rendition, run c2patool, return its JSON.

    This proves the signature travels in the MP4 file — the UI can show
    assertions, signature info, certs, tamper-evidence hashes, etc.
    """
    import ibis
    import subprocess
    import tempfile

    try:
        with _vastdb_session().transaction() as tx:
            tbl = tx.bucket(_bucket).schema(_schema).table("package_renditions")
            rows = tbl.select(
                predicate=(ibis._.package_id == package_id) & (ibis._.rendition_id == rendition_id),
            ).read_all().to_pylist()
            if not rows:
                return jsonify({"error": "no such rendition"}), 404
            rend = rows[0]
    except Exception as e:
        return jsonify({"error": f"rendition query failed: {e}"}), 502

    s3_path = rend.get("rendition_s3_path")
    if not s3_path:
        return jsonify({"error": "no rendition_s3_path"}), 404

    # Download to temp, run c2patool, return the parsed output.
    tmp = None
    try:
        bucket, key = _parse_s3_path(s3_path)
        tmp_fd, tmp = tempfile.mkstemp(suffix=".mp4")
        os.close(tmp_fd)
        s3_client.download_file(bucket, key, tmp)

        r = subprocess.run(
            [C2PATOOL_BINARY, tmp],
            capture_output=True, text=True, timeout=30,
            stdin=subprocess.DEVNULL,
        )
        if r.returncode != 0:
            # "No claim found" is a normal response for unsigned files.
            if "No claim" in (r.stderr or "") or "No claim" in (r.stdout or ""):
                return jsonify({"signed": False, "manifests": {}, "active_manifest": None})
            return jsonify({"error": f"c2patool rc={r.returncode}: {r.stderr[:500]}"}), 502

        try:
            report = json.loads(r.stdout)
        except json.JSONDecodeError:
            return jsonify({"error": "c2patool emitted non-JSON", "raw": r.stdout[:500]}), 502

        # Normalize for the UI: expose the active manifest at top level
        manifests = report.get("manifests") or {}
        active_label = report.get("active_manifest")
        active = manifests.get(active_label) if active_label else (
            next(iter(manifests.values()), None)
        )

        # Extract the AI-disclosure assertion if present for quick UI access
        ai_disclosure = None
        for a in (active or {}).get("assertions", []) or []:
            if a.get("label") == "com.vast.ai_clip_selection":
                ai_disclosure = a.get("data")
                break

        return jsonify({
            "signed":          bool(manifests),
            "rendition_id":    rendition_id,
            "rendition_s3_path": s3_path,
            "active_manifest": active_label,
            "manifests":       manifests,
            "active":          active,
            "ai_disclosure":   ai_disclosure,
        })
    except Exception as e:
        return jsonify({"error": f"verify failed: {e}"}), 502
    finally:
        if tmp:
            try: os.unlink(tmp)
            except OSError: pass


# ── Sources + AI Clipper view (Phase 1+2 read API) ─────────────────────
#
# Powers the /ai-clipper page: every raw upload (source_videos row) and
# every clip the AI extracted from it (extracted_clips rows). This is
# read-only — the data is produced by the qc-inspector + ai-clipper
# pipelines.

@app.route("/api/sources")
def list_sources():
    """List all source_videos rows with summary info for the index view.

    Sorted newest-first. Includes clip counts and the prompt that was
    used so the listing tells the story at a glance.
    """
    try:
        with _vastdb_session().transaction() as tx:
            tbl = tx.bucket(_bucket).schema(_schema).table("source_videos")
            rows = tbl.select().read_all().to_pylist()
    except Exception as e:
        return jsonify({"error": f"source_videos query failed: {e}"}), 502

    out = []
    for r in rows:
        out.append({
            "source_id":               r.get("source_id"),
            "filename":                r.get("filename"),
            "current_s3_path":         r.get("current_s3_path") or r.get("s3_inbox_path"),
            "duration_seconds":        _json_safe(r.get("duration_seconds")),
            "width":                   _json_safe(r.get("width")),
            "height":                  _json_safe(r.get("height")),
            "qc_status":               r.get("qc_status"),
            "qc_verdict_reason":       r.get("qc_verdict_reason"),
            "clip_extraction_status":  r.get("clip_extraction_status"),
            "clip_count":              _json_safe(r.get("clip_count")),
            "clip_prompt":             r.get("clip_prompt"),
            "clip_prompt_source":      r.get("clip_prompt_source"),
            "clip_extracted_at":       _json_safe(r.get("clip_extracted_at")),
            "package_id":              r.get("package_id"),
            "packaging_status":        r.get("packaging_status"),
            "uploaded_at":             _json_safe(r.get("uploaded_at")),
            "created_at":              _json_safe(r.get("created_at")),
            "updated_at":              _json_safe(r.get("updated_at")),
        })
    out.sort(key=lambda r: (r.get("uploaded_at") or r.get("created_at") or 0), reverse=True)
    return jsonify({"sources": out, "count": len(out)})


@app.route("/api/sources/<source_id>")
def get_source(source_id):
    """Full detail: the source_videos row + every extracted_clip for it,
    ordered by clip_index. Used by the /ai-clipper detail view to show
    the full video alongside its per-clip mini-players.
    """
    import ibis
    try:
        with _vastdb_session().transaction() as tx:
            src_tbl = tx.bucket(_bucket).schema(_schema).table("source_videos")
            src_rows = src_tbl.select(
                predicate=ibis._.source_id == source_id,
            ).read_all().to_pylist()
            if not src_rows:
                return jsonify({"error": f"no source {source_id}"}), 404
            source = src_rows[0]

            clips_tbl = tx.bucket(_bucket).schema(_schema).table("extracted_clips")
            clip_rows = clips_tbl.select(
                predicate=ibis._.source_id == source_id,
            ).read_all().to_pylist()
    except Exception as e:
        return jsonify({"error": f"source query failed: {e}"}), 502

    clips = [_row_to_dict(c) for c in clip_rows]
    clips.sort(key=lambda c: c.get("clip_index") or 0)
    for c in clips:
        c["frame_scores"] = _parse_json_or_none(c.get("frame_scores_json")) or []

    return jsonify({
        "source": _row_to_dict(source),
        "clips":  clips,
    })


# ── Serve React frontend ─────────────────────────────────────────────────

@app.route("/")
def serve_index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/<path:path>")
def serve_static(path):
    file_path = os.path.join(app.static_folder, path)
    if os.path.isfile(file_path):
        return send_from_directory(app.static_folder, path)
    return send_from_directory(app.static_folder, "index.html")


# SPA fallback: Flask's built-in static handler (static_url_path="/") fires
# first for unknown top-level paths and returns 404 before the catch-all can
# run. Override the 404 handler to serve index.html for any non-API path so
# React Router can handle client-side routes like /architecture.
@app.errorhandler(404)
def spa_fallback(e):
    if request.path.startswith("/api/"):
        return jsonify({"error": "Not found"}), 404
    return send_from_directory(app.static_folder, "index.html")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port, debug=False)
