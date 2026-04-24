"""Analysis: All hash comparisons in ONE pass.

DB-only — no video download. Reads this asset's sha256 + perceptual_hash from
the assets table, reads ALL other assets' hashes in a single table scan, and
performs every comparison type in one loop:

  - SHA-256 exact match → exact_duplicate
  - pHash Hamming ≤ 2  → exact_copy / unauthorized
  - pHash Hamming ≤ 18 → near_match
  - pHash Hamming ≤ 25 → derivative
  - pHash frame-by-frame 5-30 → reconformation candidate
  - pHash Hamming ≤ 20 + no relationships → orphan resolution

Writes: relationships, hash_matches, assets columns.
"""

import hashlib
import json
import os
import subprocess
import time
import traceback
import uuid

from config_loader import load_config
from s3_client import S3Client
from vast_client import VastDBClient
from schemas import ASSETS_SCHEMA, RELATIONSHIPS_SCHEMA, HASH_MATCHES_SCHEMA
from hash_utils import compare_video_phashes, compare_frame_sequences

# ── Thresholds ──────────────────────────────────────────────────────────────
PHASH_EXACT_COPY = 2        # unauthorized exact copy
PHASH_NEAR_MATCH = 18       # near duplicate
PHASH_ORPHAN_MATCH = 20     # orphan resolution
PHASH_DERIVATIVE = 25       # derivative relationship
PHASH_RECONF_MIN = 5        # reconformation lower bound
PHASH_RECONF_MAX = 30       # reconformation upper bound
MAX_DIST = 256.0            # max possible Hamming distance for similarity calc


def init(ctx):
    """One-time initialization: load config, create clients.

    Validates connectivity but catches failures so the container starts up
    even if VAST DB is temporarily unreachable.  Table setup is deferred to
    first handler call.
    """
    config = load_config()

    s3 = S3Client(config)
    vast = VastDBClient(config)

    ctx.user_data = {
        "config": config,
        "s3": s3,
        "vast": vast,
        "_tables_ready": False,
    }

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

    ctx.logger.info("hash-comparator initialized")


def handler(ctx, event):
    """Compare this asset's hashes against all existing assets."""
    log = ctx.logger.info

    try:
        return _handle(ctx, event)
    except Exception as e:
        log(f"HANDLER ERROR: {type(e).__name__}: {e}")
        log(f"TRACEBACK: {traceback.format_exc()}")
        raise


def _handle(ctx, event):
    """Inner handler with full processing logic."""
    log = ctx.logger.info

    vast = ctx.user_data["vast"]

    # ── Lazy table setup on first handler call ──
    if not ctx.user_data["_tables_ready"]:
        log("Setting up VAST DB tables (first handler call)...")
        vast.setup_tables([
            ("assets", ASSETS_SCHEMA),
            ("relationships", RELATIONSHIPS_SCHEMA),
            ("hash_matches", HASH_MATCHES_SCHEMA),
        ], logger=ctx.logger)
        ctx.user_data["_tables_ready"] = True
        log("Table setup complete")

    # ── Step 1: Parse event ──
    log(f"[1/4] Event received — type: {type(event).__name__}")

    if hasattr(event, "object_key") and hasattr(event, "bucket"):
        bucket_name = str(event.bucket)
        object_key = str(event.object_key)
        s3_path = f"s3://{bucket_name}/{object_key}"
        log(f"       bucket={bucket_name}  key={object_key}")
    elif hasattr(event, "body"):
        s3_path = event.body.decode("utf-8") if isinstance(event.body, bytes) else str(event.body)
        s3_path = s3_path.strip()
    else:
        log(f"       Event attrs: {[a for a in dir(event) if not a.startswith('_')]}")
        raise RuntimeError(f"Cannot extract s3_path from {type(event).__name__}")
    log(f"       s3_path={s3_path}")

    asset_id = hashlib.md5(s3_path.encode()).hexdigest()
    log(f"       asset_id={asset_id}")

    # ── Step 2: Load tables (single read each) ──
    log("[2/4] Loading assets and relationships tables...")
    assets_table = vast.load_table_safe("assets", ctx.logger)
    rel_table = vast.load_table_safe("relationships", ctx.logger)

    if assets_table is None or assets_table.num_rows == 0:
        log("       No assets table. Classifying as root.")
        _write_default_root(vast, asset_id, s3_path)
        return json.dumps({"asset_id": asset_id, "status": "ok"})

    # ── Index asset data ──
    all_ids = assets_table.column("asset_id").to_pylist()

    sha_col = _safe_column(assets_table, "sha256")
    phash_col = _safe_column(assets_table, "perceptual_hash")
    size_col = _safe_column(assets_table, "file_size_bytes")
    width_col = _safe_column(assets_table, "width")
    bitrate_col = _safe_column(assets_table, "bitrate")

    # Find this asset's data — scan ALL duplicate rows to find hashes,
    # retry up to 60s waiting for hash-generator to write them.
    MAX_RETRIES = 6
    RETRY_DELAY = 10  # seconds
    target_idx = None
    target_sha = None
    target_phash = None

    def _find_best_row(ids, sha, phash):
        """Scan all rows for asset_id, return index of best row (one with hashes)."""
        best_idx = None
        for i, aid in enumerate(ids):
            if aid != asset_id:
                continue
            if best_idx is None:
                best_idx = i
            # Prefer row with SHA hash
            row_sha = sha[i] if sha else None
            row_phash = phash[i] if phash else None
            if row_sha or row_phash:
                return i  # found a row with hashes
        return best_idx  # return any matching row, or None

    for attempt in range(MAX_RETRIES):
        target_idx = _find_best_row(all_ids, sha_col, phash_col)

        if target_idx is not None:
            target_sha = sha_col[target_idx] if sha_col else None
            target_phash = phash_col[target_idx] if phash_col else None

        if target_sha or target_phash:
            break  # hashes are ready

        if attempt < MAX_RETRIES - 1:
            wait = RETRY_DELAY
            log(f"       Waiting {wait}s for hash-generator to finish (attempt {attempt + 1}/{MAX_RETRIES})...")
            time.sleep(wait)
            # Reload the table to pick up newly-written hashes
            assets_table = vast.load_table_safe("assets", ctx.logger)
            if assets_table is None or assets_table.num_rows == 0:
                continue
            all_ids = assets_table.column("asset_id").to_pylist()
            sha_col = _safe_column(assets_table, "sha256")
            phash_col = _safe_column(assets_table, "perceptual_hash")
            size_col = _safe_column(assets_table, "file_size_bytes")
            width_col = _safe_column(assets_table, "width")
            bitrate_col = _safe_column(assets_table, "bitrate")

    if target_idx is None:
        log(f"       Asset {asset_id} not in table after {MAX_RETRIES} retries. Classifying as root.")
        _write_default_root(vast, asset_id, s3_path)
        return json.dumps({"asset_id": asset_id, "status": "ok"})

    target_sha = sha_col[target_idx] if sha_col else None
    target_phash = phash_col[target_idx] if phash_col else None
    target_size = (size_col[target_idx] or 0) if size_col else 0
    target_quality = _quality_score(width_col, bitrate_col, target_idx)

    if not target_sha and not target_phash:
        log(f"       No hashes found after {MAX_RETRIES} retries. Classifying as root.")
        _write_default_root(vast, asset_id, s3_path)
        return json.dumps({"asset_id": asset_id, "status": "ok"})

    # Check if asset has any existing relationships (for orphan detection)
    has_existing_rels = _has_relationships(rel_table, asset_id)

    # ── Step 3: Single pass over all other assets ──
    now = time.time()
    relationship_rows = []
    hash_match_rows = []
    is_root = True
    best_orphan_match = None
    best_orphan_dist = float("inf")
    highest_similarity = 0.0

    log(f"[3/4] Comparing against {len(all_ids) - 1} existing assets...")

    for i, existing_id in enumerate(all_ids):
        if existing_id == asset_id:
            continue

        existing_sha = sha_col[i] if sha_col else None
        existing_phash = phash_col[i] if phash_col else None
        existing_size = (size_col[i] or 0) if size_col else 0
        existing_quality = _quality_score(width_col, bitrate_col, i)

        # ── SHA-256 exact match ──
        if target_sha and existing_sha and target_sha == existing_sha:
            hash_match_rows.append({
                "match_id": uuid.uuid4().hex,
                "asset_a_id": asset_id,
                "asset_b_id": existing_id,
                "match_type": "exact-sha256-duplicate",
                "similarity_score": 1.0,
                "storage_savings_bytes": target_size,
                "reconformation_viable": False,
                "detected_at": now,
            })

            parent, child = (asset_id, existing_id) if target_quality >= existing_quality else (existing_id, asset_id)
            if child == asset_id:
                is_root = False
            relationship_rows.append({
                "relationship_id": uuid.uuid4().hex,
                "parent_asset_id": parent,
                "child_asset_id": child,
                "relationship_type": "duplicate",
                "confidence": 1.0,
                "created_at": now,
            })
            highest_similarity = 1.0
            continue

        # ── Perceptual hash comparisons ──
        if not target_phash or not existing_phash:
            continue

        avg_dist = compare_video_phashes(target_phash, existing_phash)
        if avg_dist is None:
            continue

        similarity = max(0.0, 1.0 - (avg_dist / MAX_DIST))
        highest_similarity = max(highest_similarity, similarity)

        # Unauthorized use (exact copy or near match)
        if avg_dist <= PHASH_NEAR_MATCH:
            match_type = "unauthorized-exact-copy" if avg_dist <= PHASH_EXACT_COPY else "unauthorized-near-match"
            hash_match_rows.append({
                "match_id": uuid.uuid4().hex,
                "asset_a_id": asset_id,
                "asset_b_id": existing_id,
                "match_type": match_type,
                "similarity_score": round(similarity, 6),
                "storage_savings_bytes": int(min(target_size, existing_size) * similarity * 0.8),
                "reconformation_viable": False,
                "detected_at": now,
            })

        # Derivative relationship (up to threshold 25)
        if avg_dist <= PHASH_DERIVATIVE:
            parent, child = (asset_id, existing_id) if target_quality >= existing_quality else (existing_id, asset_id)
            if child == asset_id:
                is_root = False
            rel_type = "duplicate" if avg_dist <= PHASH_EXACT_COPY else "derivative"
            confidence = 0.95 if avg_dist <= PHASH_EXACT_COPY else max(0.5, 1.0 - (avg_dist / MAX_DIST))
            relationship_rows.append({
                "relationship_id": uuid.uuid4().hex,
                "parent_asset_id": parent,
                "child_asset_id": child,
                "relationship_type": rel_type,
                "confidence": round(confidence, 4),
                "created_at": now,
            })

        # Reconformation candidate (frame-by-frame, range 5-30)
        if PHASH_RECONF_MIN <= avg_dist <= PHASH_RECONF_MAX:
            seq_result = compare_frame_sequences(target_phash, existing_phash)
            if seq_result:
                seq_avg, frame_matches, total_frames = seq_result
                viable = (PHASH_RECONF_MIN <= seq_avg <= PHASH_RECONF_MAX
                          and frame_matches >= 1
                          and frame_matches < total_frames)
                if viable or seq_avg <= PHASH_RECONF_MAX:
                    match_type = "reconformation-partial-match" if viable else "reconformation-weak-match"
                    hash_match_rows.append({
                        "match_id": uuid.uuid4().hex,
                        "asset_a_id": asset_id,
                        "asset_b_id": existing_id,
                        "match_type": match_type,
                        "similarity_score": round(similarity, 6),
                        "storage_savings_bytes": 0,
                        "reconformation_viable": viable,
                        "detected_at": now,
                    })

        # Orphan resolution (best match for unlinked assets)
        if not has_existing_rels and avg_dist <= PHASH_ORPHAN_MATCH and avg_dist < best_orphan_dist:
            best_orphan_dist = avg_dist
            best_orphan_match = existing_id

    # ── Step 4: Write results ──
    log("[4/4] Writing results...")
    if relationship_rows:
        vast.write_rows("relationships", RELATIONSHIPS_SCHEMA, relationship_rows)
        log(f"       Wrote {len(relationship_rows)} relationships")
    if hash_match_rows:
        vast.write_rows("hash_matches", HASH_MATCHES_SCHEMA, hash_match_rows)
        log(f"       Wrote {len(hash_match_rows)} hash matches")

    # ── Count results by type ──
    unauthorized_count = sum(1 for m in hash_match_rows if m["match_type"].startswith("unauthorized"))
    duplicate_count = sum(1 for m in hash_match_rows if "duplicate" in m["match_type"])
    reconf_matches = [m for m in hash_match_rows if m["match_type"].startswith("reconformation")]
    reconf_viable = sum(1 for m in reconf_matches if m.get("reconformation_viable"))
    total_savings = sum(m["storage_savings_bytes"] for m in hash_match_rows)

    # ── Classification ──
    classification = "root" if is_root else "derivative"
    classification_confidence = 1.0 if not relationship_rows else max(
        r["confidence"] for r in relationship_rows
    )

    # ── Orphan resolution ──
    orphan_resolved_from = ""
    orphan_method = ""
    if not has_existing_rels:
        if best_orphan_match:
            orphan_resolved_from = best_orphan_match
            orphan_method = f"phash-match-distance-{best_orphan_dist:.1f}"
        else:
            orphan_method = "unresolved-no-match"

    # ── Single upsert with ALL columns ──
    vast.upsert_asset(asset_id, {
        "s3_path": s3_path,
        # Orphan resolution
        "orphan_resolved_from_asset_id": orphan_resolved_from,
        "orphan_resolution_method": orphan_method,
        "orphan_resolved_at": now if orphan_method else None,
        # Unauthorized use
        "unauthorized_match_count": unauthorized_count,
        "unauthorized_checked_at": now,
        # Duplicate detection
        "duplicate_count": duplicate_count,
        "total_storage_savings_bytes": total_savings,
        "duplicates_checked_at": now,
        # Master/derivative classification
        "asset_classification": classification,
        "classification_confidence": round(classification_confidence, 4),
        "classification_at": now,
        # Reconformation
        "reconformation_match_count": len(reconf_matches),
        "reconformation_viable": reconf_viable > 0,
        "reconformation_checked_at": now,
    })

    log(
        f"Done. rels={len(relationship_rows)} matches={len(hash_match_rows)} "
        f"class={classification} unauthorized={unauthorized_count} "
        f"dupes={duplicate_count} reconf={len(reconf_matches)}"
    )
    return json.dumps({"asset_id": asset_id, "status": "ok"})


# ── Helpers ─────────────────────────────────────────────────────────────────

def _safe_column(table, col_name):
    """Get column as Python list, or None if column doesn't exist."""
    if col_name in table.column_names:
        return table.column(col_name).to_pylist()
    return None


def _quality_score(width_col, bitrate_col, idx):
    """Compute quality score = width × bitrate for master/derivative direction."""
    w = (width_col[idx] or 0) if width_col else 0
    br = (bitrate_col[idx] or 1) if bitrate_col else 1
    return w * br


def _has_relationships(rel_table, asset_id):
    """Check if asset appears in existing relationships table."""
    if rel_table is None or rel_table.num_rows == 0:
        return False
    parents = rel_table.column("parent_asset_id").to_pylist()
    children = rel_table.column("child_asset_id").to_pylist()
    return asset_id in parents or asset_id in children


def _write_default_root(vast, asset_id, s3_path):
    """Write default root classification when no comparisons possible."""
    now = time.time()
    vast.upsert_asset(asset_id, {
        "s3_path": s3_path,
        "asset_classification": "root",
        "classification_confidence": 1.0,
        "classification_at": now,
        "orphan_resolution_method": "unresolved-no-hash",
        "orphan_resolved_at": now,
        "unauthorized_match_count": 0,
        "unauthorized_checked_at": now,
        "duplicate_count": 0,
        "total_storage_savings_bytes": 0,
        "duplicates_checked_at": now,
        "reconformation_match_count": 0,
        "reconformation_viable": False,
        "reconformation_checked_at": now,
    })
