"""Catalog Reconciler: detect asset moves and deletes via VAST Catalog + Audit Log.

Long-running service that runs on a configurable interval (default 30 min):
  1. Queries VAST Big Catalog (via Trino) for all current files
  2. Compares against the assets table to find missing assets
  3. Investigates missing assets via VAST Audit Log (S3, NFS, SMB)
  4. Updates asset records for moves (new path, re-derived columns) and deletes

New paths in the catalog that aren't in the assets table are ignored — the
PUT pipeline trigger handles those.
"""

import hashlib
import logging
import os
import sys
import time
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from shared.config_loader import load_config
from shared.catalog_client import CatalogClient
from shared.vast_client import VastDBClient
from shared.schemas import ASSET_MOVES_SCHEMA, RELATIONSHIPS_SCHEMA
from shared.path_helpers import (
    extract_recipient,
    extract_date,
    extract_territory,
    extract_licensee,
    extract_company,
    extract_crew_origin,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [reconciler] %(levelname)s %(message)s",
)
log = logging.getLogger("catalog-reconciler")


def main():
    config = load_config()
    catalog = CatalogClient(config)
    vast = VastDBClient(config)

    interval = config.get("reconciler", {}).get("interval_seconds", 1800)
    lookback = config.get("reconciler", {}).get("lookback_seconds", 2100)
    search_path = config.get("catalog", {}).get("media_search_path", "")

    log.info(f"Starting catalog reconciler: interval={interval}s lookback={lookback}s")

    while True:
        try:
            _reconcile(catalog, vast, search_path, lookback)
        except Exception:
            log.exception("Reconciliation cycle failed")

        log.info(f"Sleeping {interval}s until next cycle")
        time.sleep(interval)


def _reconcile(catalog: CatalogClient, vast: VastDBClient, search_path: str, lookback: int):
    """Run one reconciliation cycle."""
    cycle_start = time.time()

    # Step 1: Get all current files from VAST Big Catalog
    log.info("Querying VAST Big Catalog...")
    catalog_files = catalog.list_catalog_files(search_path)
    catalog_paths = set(catalog_files.keys())
    log.info(f"Catalog: {len(catalog_paths)} files")

    # Step 2: Get all tracked assets from VAST DB
    log.info("Reading assets table...")
    assets_table = vast.load_table_safe("assets", logger=log)
    if assets_table is None or assets_table.num_rows == 0:
        log.info("No assets in table, nothing to reconcile")
        return

    # Build set of active asset paths
    asset_paths_col = assets_table.column("s3_path").to_pylist()
    status_col = (
        assets_table.column("status").to_pylist()
        if "status" in assets_table.column_names
        else [None] * assets_table.num_rows
    )
    asset_ids_col = assets_table.column("asset_id").to_pylist()

    # Map path -> asset_id for active assets
    active_assets = {}
    for path, status, asset_id in zip(asset_paths_col, status_col, asset_ids_col):
        if status not in ("moved", "deleted"):
            active_assets[path] = asset_id

    active_paths = set(active_assets.keys())
    log.info(f"Assets: {len(active_paths)} active")

    # Step 3: Diff
    missing = active_paths - catalog_paths
    # new = catalog_paths - active_paths  # ignored — PUT pipeline handles these
    present = active_paths & catalog_paths

    log.info(f"Diff: missing={len(missing)} present={len(present)} "
             f"new_in_catalog={len(catalog_paths - active_paths)}")

    if not missing:
        log.info("No missing assets, updating reconciled timestamps")
        _update_reconciled_timestamps(vast, present, active_assets)
        log.info(f"Cycle complete in {time.time() - cycle_start:.1f}s")
        return

    # Step 4: Investigate each missing asset
    for s3_path in missing:
        asset_id = active_assets[s3_path]
        _investigate_missing(catalog, vast, s3_path, asset_id, lookback)

    # Step 5: Update reconciled timestamp for present assets
    _update_reconciled_timestamps(vast, present, active_assets)

    log.info(f"Cycle complete in {time.time() - cycle_start:.1f}s — "
             f"investigated {len(missing)} missing assets")


def _investigate_missing(
    catalog: CatalogClient,
    vast: VastDBClient,
    s3_path: str,
    asset_id: str,
    lookback: int,
):
    """Investigate a missing asset: check audit log for renames then deletes."""
    log.info(f"Investigating missing: {s3_path}")

    # Try to find a rename/move first
    rename_events = catalog.find_rename_events(s3_path, lookback)
    if rename_events:
        event = rename_events[0]  # most recent
        new_path = event["rename_destination"]
        log.info(f"  RENAME detected via {event['protocol']}: {s3_path} -> {new_path}")
        _handle_move(vast, s3_path, asset_id, new_path, event)
        return

    # Try to find a delete
    delete_events = catalog.find_delete_events(s3_path, lookback)
    if delete_events:
        event = delete_events[0]  # most recent
        log.info(f"  DELETE detected via {event['protocol']}: {s3_path}")
        _handle_delete(vast, s3_path, asset_id, event)
        return

    # Nothing found — audit log should have caught it
    log.error(
        f"No audit events found for missing asset {s3_path} (asset_id={asset_id}) — "
        f"audit log integration may be broken. Searched {lookback}s lookback across "
        f"S3, NFS, SMB protocols."
    )


def _handle_move(
    vast: VastDBClient,
    old_path: str,
    old_asset_id: str,
    new_path: str,
    event: dict,
):
    """Process an asset move: create new row, update old row, record event."""
    now = time.time()
    new_asset_id = hashlib.md5(new_path.encode()).hexdigest()
    new_filename = new_path.rsplit("/", 1)[-1] if "/" in new_path else new_path
    performer = event.get("login_name") or str(event.get("uid", ""))

    # Read existing asset data to carry over
    import pyarrow.compute as pc
    try:
        existing_table = vast.query_rows(
            "assets",
            predicate=pc.equal(pc.field("asset_id"), pc.scalar(old_asset_id)),
        )
        if existing_table.num_rows == 0:
            log.warning(f"  Could not read old asset row for {old_asset_id}")
            return
        old_row = {
            col: existing_table.column(col)[0].as_py()
            for col in existing_table.column_names
        }
    except Exception as e:
        log.warning(f"  Failed to read old asset: {e}")
        return

    # Re-derive path-dependent columns from new path
    new_recipient = extract_recipient(new_path)
    new_date = extract_date(new_path)
    new_territory = extract_territory(new_path)
    new_licensee = extract_licensee(new_path)
    new_company = extract_company(new_path)
    new_crew = extract_crew_origin(new_path)

    # Compute move_count
    old_move_count = old_row.get("move_count") or 0
    original_path = old_row.get("original_s3_path") or old_path

    # Create new asset row: carry over all columns, update path-dependent ones
    new_fields = {
        "asset_id": new_asset_id,
        "s3_path": new_path,
        "filename": new_filename,
        "status": "active",
        "original_s3_path": original_path,
        "move_count": old_move_count + 1,
        "last_moved_at": now,
        "last_moved_by": performer,
        "last_reconciled_at": now,
        # Re-derived path-dependent columns
        "delivery_recipient": new_recipient,
        "delivery_date": new_date,
        "primary_territory": new_territory,
        "primary_licensee": new_licensee,
        "primary_production_company": new_company,
        "crew_origin": new_crew,
    }

    # Upsert new row (carries over existing data via merge)
    # First upsert old data to new id, then overwrite with new fields
    carry_over = {k: v for k, v in old_row.items() if k != "asset_id"}
    carry_over.update(new_fields)
    vast.upsert_asset(new_asset_id, carry_over)

    # Mark old row as moved
    vast.upsert_asset(old_asset_id, {
        "status": "moved",
        "last_moved_at": now,
    })

    # Record in asset_moves table
    vast.write_rows("asset_moves", ASSET_MOVES_SCHEMA, [{
        "event_id": uuid.uuid4().hex,
        "asset_id": old_asset_id,
        "new_asset_id": new_asset_id,
        "event_type": "move",
        "old_s3_path": old_path,
        "new_s3_path": new_path,
        "protocol": event.get("protocol", ""),
        "rpc_type": event.get("rpc_type", ""),
        "performed_by": performer,
        "client_ip": event.get("client_ip", ""),
        "detected_at": now,
        "audit_timestamp": _timestamp_to_epoch(event.get("time")),
    }])

    # Duplicate relationship edges from old_asset_id to new_asset_id
    _duplicate_relationships(vast, old_asset_id, new_asset_id)

    log.info(f"  Move processed: {old_asset_id} -> {new_asset_id}")


def _handle_delete(
    vast: VastDBClient,
    s3_path: str,
    asset_id: str,
    event: dict,
):
    """Process an asset deletion: update status, record event."""
    now = time.time()
    performer = event.get("login_name") or str(event.get("uid", ""))

    vast.upsert_asset(asset_id, {
        "status": "deleted",
        "deleted_at": now,
        "deleted_by": performer,
    })

    vast.write_rows("asset_moves", ASSET_MOVES_SCHEMA, [{
        "event_id": uuid.uuid4().hex,
        "asset_id": asset_id,
        "new_asset_id": None,
        "event_type": "delete",
        "old_s3_path": s3_path,
        "new_s3_path": None,
        "protocol": event.get("protocol", ""),
        "rpc_type": event.get("rpc_type", ""),
        "performed_by": performer,
        "client_ip": event.get("client_ip", ""),
        "detected_at": now,
        "audit_timestamp": _timestamp_to_epoch(event.get("time")),
    }])

    log.info(f"  Delete processed: {asset_id}")


def _duplicate_relationships(vast: VastDBClient, old_id: str, new_id: str):
    """Copy relationship edges from old asset_id to new asset_id."""
    import pyarrow.compute as pc

    rels = vast.load_table_safe("relationships")
    if rels is None or rels.num_rows == 0:
        return

    parent_ids = rels.column("parent_asset_id").to_pylist()
    child_ids = rels.column("child_asset_id").to_pylist()
    rel_types = rels.column("relationship_type").to_pylist()
    confidences = rels.column("confidence").to_pylist()

    now = time.time()
    new_rows = []

    for parent, child, rel_type, conf in zip(parent_ids, child_ids, rel_types, confidences):
        if parent == old_id:
            new_rows.append({
                "relationship_id": uuid.uuid4().hex,
                "parent_asset_id": new_id,
                "child_asset_id": child,
                "relationship_type": rel_type,
                "confidence": conf,
                "created_at": now,
            })
        if child == old_id:
            new_rows.append({
                "relationship_id": uuid.uuid4().hex,
                "parent_asset_id": parent,
                "child_asset_id": new_id,
                "relationship_type": rel_type,
                "confidence": conf,
                "created_at": now,
            })

    if new_rows:
        vast.write_rows("relationships", RELATIONSHIPS_SCHEMA, new_rows)
        log.info(f"  Duplicated {len(new_rows)} relationship edges for new asset_id")


def _update_reconciled_timestamps(
    vast: VastDBClient,
    present_paths: set[str],
    active_assets: dict[str, str],
):
    """Update last_reconciled_at for all assets still present in catalog."""
    now = time.time()
    # Batch update: upsert each present asset with timestamp
    # Only update a sample if there are too many (>1000) to avoid timeout
    paths_to_update = list(present_paths)
    if len(paths_to_update) > 1000:
        log.info(f"  Updating reconciled timestamp for first 1000 of {len(paths_to_update)} assets")
        paths_to_update = paths_to_update[:1000]

    for path in paths_to_update:
        asset_id = active_assets.get(path)
        if asset_id:
            vast.upsert_asset(asset_id, {"last_reconciled_at": now})


def _timestamp_to_epoch(ts) -> float:
    """Convert a Trino timestamp to epoch seconds."""
    if ts is None:
        return 0.0
    if isinstance(ts, (int, float)):
        return float(ts)
    # datetime object from trino
    try:
        return ts.timestamp()
    except Exception:
        return 0.0


if __name__ == "__main__":
    main()
