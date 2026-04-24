"""Analysis: All 16 graph-based analyses in ONE pass (Layer 2).

DB-only — no video download. Reads relationships, assets, and hash_matches
tables ONCE, builds adjacency maps ONCE, then runs 16 analysis modules:

  UC01  Rights conflict detection
  UC02  Orphan status (has any relationships?)
  UC04  License audit trail
  UC07  Safe deletion evaluation
  UC10  Version control → writes version_history table
  UC11  Training data provenance
  UC12  Model contamination detection
  UC14  Bias audit
  UC16  Clearance inheritance
  UC17  Compliance propagation
  UC19  Leak investigation
  UC21  Chain of custody
  UC22  Ransomware impact assessment
  UC23  Content valuation
  UC24  Syndication tracking → writes syndication_records table
  UC25  Insurance & disaster recovery
  UC26  Co-production attribution → writes production_entities table

Single upsert_asset() call at the end with ALL columns from all 16 analyses.
"""

import hashlib
import json
import os
import re
import subprocess
import time
import traceback
import uuid

from config_loader import load_config
from vast_client import VastDBClient
from schemas import (
    ASSETS_SCHEMA,
    RELATIONSHIPS_SCHEMA,
    HASH_MATCHES_SCHEMA,
    VERSION_HISTORY_SCHEMA,
    SYNDICATION_RECORDS_SCHEMA,
    PRODUCTION_ENTITIES_SCHEMA,
)
from graph_utils import (
    build_adjacency,
    find_parents,
    find_parents_with_types,
    find_children,
    find_children_with_types,
    find_all_ancestors,
    count_descendants,
    trace_chain_downward,
    has_relationship,
    trace_root,
)
from path_helpers import (
    TERRITORY_PATTERNS,
    LOCATION_MARKERS,
    extract_recipient,
    extract_date,
    extract_territory,
    extract_licensee,
    extract_company,
    extract_crew_origin,
    classify_contribution,
)

# ── Constants ───────────────────────────────────────────────────────────────

AI_RELATIONSHIP_TYPES = {
    "ai_generated", "ai_enhanced", "deepfake", "synthetic",
    "neural_style", "gan_output", "diffusion", "ai_upscale",
}

RATING_ORDER = {
    "nc-17": 0, "18+": 1, "r": 2, "pg-13": 3, "pg": 4, "g": 5, "unrated": 6,
}

DERIVATIVE_REL_TYPES = {"derivative", "transcode", "conform", "edit"}
REUSE_REL_TYPES = {"reuse", "reference", "include"}
DELIVERY_REL_TYPES = {"delivery", "distribution", "syndication"}


def init(ctx):
    """One-time initialization: load config, create client.

    Validates connectivity but catches failures so the container starts up
    even if VAST DB is temporarily unreachable.  Table setup is deferred to
    first handler call.
    """
    config = load_config()

    vast = VastDBClient(config)

    ctx.user_data = {
        "config": config,
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

    ctx.logger.info("graph-analyzer initialized")


def handler(ctx, event):
    """Run all 16 graph-based analyses for a single asset."""
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
            ("version_history", VERSION_HISTORY_SCHEMA),
            ("syndication_records", SYNDICATION_RECORDS_SCHEMA),
            ("production_entities", PRODUCTION_ENTITIES_SCHEMA),
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

    # ── Step 2: Load all tables ONCE ──
    log("[2/4] Loading tables...")
    rel_table = vast.load_table_safe("relationships", ctx.logger)
    assets_table = vast.load_table_safe("assets", ctx.logger)
    hash_matches_table = vast.load_table_safe("hash_matches", ctx.logger)

    # ── Build adjacency maps ONCE ──
    p2c, c2p = build_adjacency(rel_table)

    # ── Build asset lookup dict ──
    asset_lookup = _build_asset_lookup(assets_table)
    this_asset = asset_lookup.get(asset_id, {})

    # ── Step 3: Run all 16 analyses ──
    log("[3/4] Running 16 analysis modules...")
    now = time.time()
    columns = {}
    extra_rows = {"version_history": [], "syndication_records": [], "production_entities": []}

    _uc01_rights_conflict(columns, asset_id, c2p, asset_lookup)
    _uc02_orphan_status(columns, asset_id, c2p, p2c)
    _uc04_license_audit(columns, asset_id, c2p, p2c)
    _uc07_safe_deletion(columns, asset_id, c2p, p2c)
    _uc10_version_control(columns, extra_rows, asset_id, s3_path, c2p, now)
    _uc11_training_provenance(columns, asset_id, c2p)
    _uc12_model_contamination(columns, asset_id, c2p)
    _uc14_bias_audit(columns, asset_id, c2p, asset_lookup)
    _uc16_clearance_inheritance(columns, asset_id, c2p, asset_lookup)
    _uc17_compliance_propagation(columns, asset_id, c2p, asset_lookup)
    _uc19_leak_investigation(columns, asset_id, p2c, this_asset, asset_lookup)
    _uc21_chain_of_custody(columns, asset_id, c2p, p2c, this_asset)
    _uc22_ransomware_assessment(columns, asset_id, c2p, p2c, this_asset, assets_table)
    _uc23_content_valuation(columns, asset_id, p2c)
    _uc24_syndication_tracking(columns, extra_rows, asset_id, s3_path, p2c, asset_lookup, now)
    _uc25_insurance_valuation(columns, asset_id, c2p, this_asset, assets_table)
    _uc26_coproduction_attribution(columns, extra_rows, asset_id, s3_path, c2p, asset_lookup, now)
    log("       All 16 analyses complete")

    # ── Step 4: Write results ──
    log("[4/4] Writing results...")
    if extra_rows["version_history"]:
        vast.write_rows("version_history", VERSION_HISTORY_SCHEMA, extra_rows["version_history"])
        log(f"       Wrote {len(extra_rows['version_history'])} version_history rows")
    if extra_rows["syndication_records"]:
        vast.write_rows("syndication_records", SYNDICATION_RECORDS_SCHEMA, extra_rows["syndication_records"])
        log(f"       Wrote {len(extra_rows['syndication_records'])} syndication_records rows")
    if extra_rows["production_entities"]:
        vast.write_rows("production_entities", PRODUCTION_ENTITIES_SCHEMA, extra_rows["production_entities"])
        log(f"       Wrote {len(extra_rows['production_entities'])} production_entities rows")

    # ── Single upsert with ALL columns ──
    columns["s3_path"] = s3_path
    vast.upsert_asset(asset_id, columns)
    log(f"       Upserted {len(columns)} columns to assets table")

    log(f"Done. 16 analyses complete for {asset_id}")
    return json.dumps({"asset_id": asset_id, "status": "ok"})


# ═══════════════════════════════════════════════════════════════════════════
# ANALYSIS MODULES
# ═══════════════════════════════════════════════════════════════════════════

def _uc01_rights_conflict(cols, asset_id, c2p, asset_lookup):
    """UC01: Check parent restrictions vs child for rights conflicts."""
    ancestors = find_all_ancestors(c2p, asset_id, max_depth=10)

    restrictions = []
    earliest_expiry = None
    conflict_detected = False
    conflict_details = []

    for anc_id, depth, rel_type in ancestors:
        anc = asset_lookup.get(anc_id, {})
        r = anc.get("restrictions")
        if r:
            restrictions.append(r)
            if "no-derivative" in r.lower():
                conflict_detected = True
                conflict_details.append(f"parent {anc_id} restricts derivatives")

        expiry = anc.get("rights_expiry")
        if expiry:
            if earliest_expiry is None or str(expiry) < str(earliest_expiry):
                earliest_expiry = expiry

    cols["license_type"] = "standard"
    cols["territories"] = "worldwide" if not ancestors else "inherited"
    cols["restrictions"] = "; ".join(restrictions) if restrictions else ""
    cols["rights_expiry"] = str(earliest_expiry) if earliest_expiry else ""
    cols["conflict_detected"] = conflict_detected
    cols["conflict_details"] = "; ".join(conflict_details) if conflict_details else ""
    cols["rights_checked_at"] = time.time()


def _uc02_orphan_status(cols, asset_id, c2p, p2c):
    """UC02 orphan columns handled by hash-comparator.
    UC18 dubbed_from_asset_id: check parent relationships for dub/localize types."""
    dubbed_from = ""
    for parent_id, rel_type in find_parents_with_types(c2p, asset_id):
        rt = (rel_type or "").lower()
        if any(kw in rt for kw in ("dub", "localize", "translate", "voice")):
            dubbed_from = parent_id
            break
    cols["dubbed_from_asset_id"] = dubbed_from


def _uc04_license_audit(cols, asset_id, c2p, p2c):
    """UC04: Trace lineage to root, count descendants."""
    ancestors = find_all_ancestors(c2p, asset_id, max_depth=50)
    descendant_count = count_descendants(p2c, asset_id)

    licensor = ancestors[0][0] if ancestors else ""
    usage_type = "derivative" if ancestors else "original"

    cols["licensor"] = licensor
    cols["usage_type"] = usage_type
    cols["audit_derivative_count"] = descendant_count
    cols["license_audit_at"] = time.time()


def _uc07_safe_deletion(cols, asset_id, c2p, p2c):
    """UC07: Count dependents, determine deletion safety."""
    children = find_children(p2c, asset_id)
    parents = find_parents(c2p, asset_id)
    dep_count = count_descendants(p2c, asset_id)

    is_leaf = len(children) == 0
    is_root = len(parents) == 0

    cols["dependent_count"] = dep_count
    cols["is_leaf"] = is_leaf
    cols["is_root"] = is_root
    cols["deletion_safe"] = is_leaf
    cols["deletion_evaluated_at"] = time.time()


def _uc10_version_control(cols, extra_rows, asset_id, s3_path, c2p, now):
    """UC10: Walk parent chain, assign version numbers."""
    chain = trace_root(c2p, asset_id)
    version_number = len(chain)
    prev_version_id = chain[-2] if len(chain) >= 2 else ""

    label = f"v{version_number}"
    if version_number == 1:
        label += " (original)"
    else:
        label += " (derivative)"

    cols["version_number"] = version_number
    cols["previous_version_id"] = prev_version_id
    cols["version_label"] = label
    cols["version_recorded_at"] = now

    extra_rows["version_history"].append({
        "version_id": str(uuid.uuid4()),
        "asset_id": asset_id,
        "s3_path": s3_path,
        "version_number": version_number,
        "previous_version_id": prev_version_id,
        "version_label": label,
        "created_at": now,
    })


def _uc11_training_provenance(cols, asset_id, c2p):
    """UC11: Trace ancestors, check training rights chain."""
    chain = trace_root(c2p, asset_id)
    is_original = len(chain) == 1
    dataset_id = f"dataset-{hashlib.md5('|'.join(chain).encode()).hexdigest()[:8]}"

    # Check if any ancestor has training-related relationship types
    ancestors = find_all_ancestors(c2p, asset_id, max_depth=20)
    rights_cleared = any(
        "training" in rt.lower() or "ai" in rt.lower()
        for _, _, rt in ancestors
    )

    cols["training_dataset_id"] = dataset_id
    cols["is_training_original"] = is_original
    cols["rights_cleared_for_training"] = rights_cleared
    cols["training_processing_chain"] = json.dumps(chain[:20])
    cols["training_logged_at"] = time.time()


def _uc12_model_contamination(cols, asset_id, c2p):
    """UC12: BFS upward for AI relationship types."""
    ancestors = find_all_ancestors(c2p, asset_id, max_depth=50)

    has_ai = False
    max_depth = 0
    for anc_id, depth, rel_type in ancestors:
        if rel_type.lower() in AI_RELATIONSHIP_TYPES:
            has_ai = True
            max_depth = max(max_depth, depth)

    if not has_ai:
        risk = "none"
    elif max_depth <= 1:
        risk = "high"
    elif max_depth <= 3:
        risk = "medium"
    else:
        risk = "low"

    cols["contamination_risk"] = risk
    cols["has_ai_processing_upstream"] = has_ai
    cols["processing_depth"] = max_depth
    cols["contamination_checked_at"] = time.time()


def _uc14_bias_audit(cols, asset_id, c2p, asset_lookup):
    """UC14: Check AI probability, trace model + training data."""
    this_asset = asset_lookup.get(asset_id, {})
    ai_prob = this_asset.get("ai_probability") or 0.0

    ancestors = find_all_ancestors(c2p, asset_id, max_depth=20)

    model_id = ""
    ai_tool = ""
    training_ids = []
    for anc_id, depth, rel_type in ancestors:
        rt = rel_type.lower()
        if rt in ("model", "generated_by"):
            model_id = anc_id
            anc = asset_lookup.get(anc_id, {})
            ai_tool = anc.get("ai_tool_detected") or ""
        if rt in ("training", "trained_on"):
            training_ids.append(anc_id)

    if ai_prob < 0.3:
        audit_result = "likely_authentic"
        risk_level = "low"
    elif model_id and training_ids:
        audit_result = "ai_detected_training_traced"
        risk_level = "medium"
    elif model_id:
        audit_result = "ai_detected_model_identified"
        risk_level = "medium"
    elif ai_prob > 0.0:
        audit_result = "ai_detected_untraced"
        risk_level = "high"
    else:
        audit_result = "no_ai_detection_record"
        risk_level = "unknown"

    cols["bias_model_id"] = model_id
    cols["bias_ai_tool_used"] = ai_tool
    cols["bias_training_data_ids"] = json.dumps(training_ids)
    cols["bias_audit_result"] = audit_result
    cols["bias_risk_level"] = risk_level
    cols["bias_audited_at"] = time.time()


def _uc16_clearance_inheritance(cols, asset_id, c2p, asset_lookup):
    """UC16: Collect ancestor clearances, inherit first found."""
    ancestors = find_all_ancestors(c2p, asset_id, max_depth=20)

    inherited_from = ""
    clearance_type = ""
    clearance_status = "no_parent_clearances"

    for anc_id, depth, rel_type in ancestors:
        anc = asset_lookup.get(anc_id, {})
        ct = anc.get("clearance_type")
        if ct:
            clearance_type = ct
            inherited_from = anc_id
            clearance_status = f"inherited:{ct}"
            break

    cols["clearance_status"] = clearance_status
    cols["clearance_type"] = clearance_type
    cols["clearance_inherited_from"] = inherited_from
    cols["clearance_recorded_at"] = time.time()


def _uc17_compliance_propagation(cols, asset_id, c2p, asset_lookup):
    """UC17: Collect ancestor compliance, select most restrictive."""
    ancestors = find_all_ancestors(c2p, asset_id, max_depth=20)

    most_restrictive = "unrated"
    most_restrictive_from = ""
    all_warnings = set()

    for anc_id, depth, rel_type in ancestors:
        anc = asset_lookup.get(anc_id, {})

        rating = anc.get("compliance_rating")
        if rating and rating.lower() in RATING_ORDER:
            if RATING_ORDER.get(rating.lower(), 99) < RATING_ORDER.get(most_restrictive, 99):
                most_restrictive = rating.lower()
                most_restrictive_from = anc_id

        warnings_raw = anc.get("content_warnings")
        if warnings_raw:
            try:
                w_list = json.loads(warnings_raw) if isinstance(warnings_raw, str) else warnings_raw
                if isinstance(w_list, list):
                    all_warnings.update(str(w) for w in w_list)
                elif isinstance(w_list, str):
                    all_warnings.update(w.strip() for w in w_list.split(","))
            except (json.JSONDecodeError, TypeError):
                if isinstance(warnings_raw, str):
                    all_warnings.update(w.strip() for w in warnings_raw.split(","))

    cols["compliance_rating"] = most_restrictive
    cols["content_warnings"] = json.dumps(sorted(all_warnings))
    cols["compliance_inherited_from"] = most_restrictive_from
    cols["compliance_propagated_at"] = time.time()


def _uc19_leak_investigation(cols, asset_id, p2c, this_asset, asset_lookup):
    """UC19: Trace delivery descendants, extract recipient/date."""
    descendants = trace_chain_downward(p2c, asset_id)

    # Delivery chain (capped at 20)
    chain_ids = [asset_id] + [d["asset_id"] for d in descendants[:19]]

    # Extract recipient from s3_path
    path = this_asset.get("s3_path") or ""
    recipient = extract_recipient(path)
    delivery_date = extract_date(path)

    # Build fingerprint
    sha = this_asset.get("sha256") or ""
    phash = this_asset.get("perceptual_hash") or ""
    fingerprint = f"{sha[:16]}|{phash[:32]}" if sha or phash else ""

    cols["delivery_recipient"] = recipient
    cols["delivery_date"] = delivery_date
    cols["leak_hash_fingerprint"] = fingerprint
    cols["delivery_chain"] = " ".join(chain_ids)
    cols["leak_indexed_at"] = time.time()


def _uc21_chain_of_custody(cols, asset_id, c2p, p2c, this_asset):
    """UC21: Record SHA-256 baseline, count related assets."""
    sha = this_asset.get("sha256") or ""
    ancestors = find_all_ancestors(c2p, asset_id, max_depth=50)
    desc_count = count_descendants(p2c, asset_id)
    related = len(ancestors) + desc_count

    cols["legal_hold_active"] = True
    cols["sha256_at_hold"] = sha
    cols["hold_placed_at"] = time.time()
    cols["integrity_verified"] = bool(sha)
    cols["related_asset_count"] = related
    cols["custody_verified_at"] = time.time()


def _uc22_ransomware_assessment(cols, asset_id, c2p, p2c, this_asset, assets_table):
    """UC22: Check uniqueness, backups, recovery priority."""
    parents = find_parents(c2p, asset_id)
    children = find_children(p2c, asset_id)
    desc_count = count_descendants(p2c, asset_id)
    is_root = len(parents) == 0

    # Check for backup copies (same SHA-256)
    sha = this_asset.get("sha256") or ""
    has_backup = False
    if sha and assets_table is not None:
        sha_col = _safe_column(assets_table, "sha256")
        id_col = assets_table.column("asset_id").to_pylist() if assets_table else []
        if sha_col:
            for i, s in enumerate(sha_col):
                if s == sha and id_col[i] != asset_id:
                    has_backup = True
                    break

    is_unique = is_root and not has_backup

    if is_unique and not has_backup:
        priority = "CRITICAL"
    elif is_root and children and not has_backup:
        priority = "HIGH"
    elif parents and children:
        priority = "MEDIUM"
    else:
        priority = "LOW"

    cols["is_unique_original"] = is_unique
    cols["has_backup"] = has_backup
    cols["surviving_derivatives_count"] = desc_count
    cols["recovery_priority"] = priority
    cols["ransomware_assessed_at"] = time.time()


def _uc23_content_valuation(cols, asset_id, p2c):
    """UC23: Count relationships by type, compute weighted score."""
    descendants = trace_chain_downward(p2c, asset_id)

    deriv_count = 0
    reuse_count = 0
    delivery_count = 0

    for d in descendants:
        rt = (d.get("relationship_type") or "").lower()
        if rt in DERIVATIVE_REL_TYPES:
            deriv_count += 1
        elif rt in REUSE_REL_TYPES:
            reuse_count += 1
        elif rt in DELIVERY_REL_TYPES:
            delivery_count += 1
        else:
            deriv_count += 1  # default to derivative

    score = deriv_count * 1 + reuse_count * 3 + delivery_count * 5

    if score > 100:
        tier = "PREMIUM"
    elif score > 50:
        tier = "HIGH"
    elif score > 10:
        tier = "MEDIUM"
    else:
        tier = "LOW"

    cols["valuation_derivative_count"] = deriv_count
    cols["reuse_count"] = reuse_count
    cols["delivery_count"] = delivery_count
    cols["commercial_value_score"] = float(score)
    cols["value_tier"] = tier
    cols["valued_at"] = time.time()


def _uc24_syndication_tracking(cols, extra_rows, asset_id, s3_path, p2c, asset_lookup, now):
    """UC24: Trace delivery descendants, extract territory/licensee."""
    descendants = trace_chain_downward(p2c, asset_id)

    licensees = set()
    territories = set()

    for d in descendants:
        child_id = d["asset_id"]
        child_data = asset_lookup.get(child_id, {})
        child_path = child_data.get("s3_path") or ""

        licensee = extract_licensee(child_path)
        territory = extract_territory(child_path)

        if licensee:
            licensees.add(licensee)
        if territory:
            territories.add(territory)

        rel_type = (d.get("relationship_type") or "").lower()
        if "expire" in rel_type:
            status = "expired"
        elif rel_type in DELIVERY_REL_TYPES:
            status = "active"
        else:
            status = "pending"

        extra_rows["syndication_records"].append({
            "record_id": hashlib.md5(f"{asset_id}:{child_id}".encode()).hexdigest(),
            "asset_id": asset_id,
            "s3_path": child_path or s3_path,
            "licensee": licensee,
            "territory": territory,
            "delivery_version_id": child_id,
            "license_status": status,
            "tracked_at": now,
        })

    primary_licensee = extract_licensee(s3_path)
    primary_territory = extract_territory(s3_path)

    cols["syndication_licensee_count"] = len(licensees)
    cols["syndication_territory_count"] = len(territories)
    cols["primary_licensee"] = primary_licensee
    cols["primary_territory"] = primary_territory
    cols["syndication_tracked_at"] = now


def _uc25_insurance_valuation(cols, asset_id, c2p, this_asset, assets_table):
    """UC25: Check irreplaceability, copies, read commercial value."""
    parents = find_parents(c2p, asset_id)
    is_master = len(parents) == 0

    sha = this_asset.get("sha256") or ""
    copy_count = 0
    if sha and assets_table is not None:
        sha_col = _safe_column(assets_table, "sha256")
        id_col = assets_table.column("asset_id").to_pylist() if assets_table else []
        if sha_col:
            for i, s in enumerate(sha_col):
                if s == sha and id_col[i] != asset_id:
                    copy_count += 1

    has_copies = copy_count > 0
    is_irreplaceable = is_master and not has_copies
    commercial_score = this_asset.get("commercial_value_score") or 0.0

    if is_irreplaceable and commercial_score > 50:
        tier = "PRICELESS"
    elif is_irreplaceable:
        tier = "HIGH"
    elif commercial_score > 25:
        tier = "MEDIUM"
    else:
        tier = "LOW"

    cols["is_irreplaceable"] = is_irreplaceable
    cols["has_digital_copies"] = has_copies
    cols["digital_copy_count"] = copy_count
    cols["replacement_cost_tier"] = tier
    cols["commercial_history_score"] = commercial_score
    cols["insurance_valued_at"] = time.time()


def _uc26_coproduction_attribution(cols, extra_rows, asset_id, s3_path, c2p, asset_lookup, now):
    """UC26: Trace source contributors, extract company/crew."""
    ancestors = find_all_ancestors(c2p, asset_id, max_depth=20)

    companies = {}  # company -> count
    all_entities = []

    for anc_id, depth, rel_type in ancestors:
        anc = asset_lookup.get(anc_id, {})
        anc_path = anc.get("s3_path") or ""

        company = extract_company(anc_path)
        crew_origin = extract_crew_origin(anc_path)
        contribution = classify_contribution(rel_type)

        companies[company] = companies.get(company, 0) + 1
        all_entities.append({
            "company": company,
            "crew_origin": crew_origin,
            "contribution_type": contribution,
            "anc_id": anc_id,
        })

    # Calculate ownership splits
    total_contributions = sum(companies.values()) or 1
    for entity in all_entities:
        company = entity["company"]
        split_pct = round((companies[company] / total_contributions) * 100.0, 2)

        extra_rows["production_entities"].append({
            "attribution_id": hashlib.md5(f"{asset_id}:{entity['anc_id']}".encode()).hexdigest(),
            "asset_id": asset_id,
            "s3_path": s3_path,
            "production_company": company,
            "crew_origin": entity["crew_origin"],
            "ownership_split_pct": split_pct,
            "contribution_type": entity["contribution_type"],
            "attributed_at": now,
        })

    # Primary company = most contributions
    primary_company = extract_company(s3_path)
    if companies:
        primary_company = max(companies, key=companies.get)

    primary_origin = extract_crew_origin(s3_path)

    if not ancestors:
        contribution_type = "sole_production"
        ownership = 100.0
    else:
        contribution_type = all_entities[0]["contribution_type"] if all_entities else "source_material"
        ownership = round((companies.get(primary_company, 1) / total_contributions) * 100.0, 2)

    cols["primary_production_company"] = primary_company
    cols["crew_origin"] = primary_origin
    cols["ownership_split_pct"] = ownership
    cols["contribution_type"] = contribution_type
    cols["attribution_at"] = now


# ═══════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _build_asset_lookup(assets_table):
    """Build {asset_id: {col: value}} dict from assets table."""
    if assets_table is None or assets_table.num_rows == 0:
        return {}

    ids = assets_table.column("asset_id").to_pylist()
    lookup = {}
    col_names = assets_table.column_names

    for i, aid in enumerate(ids):
        row = {}
        for col in col_names:
            try:
                row[col] = assets_table.column(col)[i].as_py()
            except Exception:
                pass
        lookup[aid] = row

    return lookup


def _safe_column(table, col_name):
    """Get column as Python list, or None if missing."""
    if table is not None and col_name in table.column_names:
        return table.column(col_name).to_pylist()
    return None
