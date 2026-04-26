#!/usr/bin/env python3
"""Seed the function_configs table with every CONFIG_SCHEMA declared in
the shared/ingest library (and any other modules we register below).

Usage:
    python3 scripts/seed_function_configs.py               # insert missing rows only
    python3 scripts/seed_function_configs.py --reset-defaults  # also reset any rows
                                                              # whose value matches the
                                                              # CURRENT default to the
                                                              # *new* default (for
                                                              # corrections to factory
                                                              # defaults; does NOT touch
                                                              # rows the user has
                                                              # edited to something else)
    python3 scripts/seed_function_configs.py --dry-run     # show what would happen

Idempotent: running with no flags never destroys user edits. The seed
creates new rows for keys that don't exist yet, and leaves existing
rows alone.

To add new knobs: just extend the module's CONFIG_SCHEMA list and re-run
this script.
"""

import argparse
import hashlib
import json
import os
import sys
import time

# Allow `from shared.x import ...` — add repo root
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, ".."))

import ibis
import pyarrow as pa
import vastdb

from shared.config import iter_registered_schemas
from shared.schemas import FUNCTION_CONFIGS_SCHEMA


# ── Load every library module so its register_defaults() calls fire ──
# Add imports here when you ship a new module with a CONFIG_SCHEMA.
def _import_all_schemas():
    # Graceful: a missing module (not yet created) just means no rows
    # for that scope. We don't want the seed to fail during Phase-by-
    # phase rollout.
    candidates = [
        "shared.ingest.qc",         # QC inspector thresholds (Phase 1)
        "shared.ingest.scene",      # AI clipper: scene detection (Phase 2)
        "shared.ingest.vision",     # AI clipper: vision inference (Phase 2)
        "shared.ingest.clips",      # AI clipper: clip assembly (Phase 2)
        "shared.ingest.curate",     # AI clipper: LLM curation (Phase 2)
        "shared.ingest.audio",      # AI clipper: audio-aware excitement (Phase 2.6)
        "shared.ingest.transcode",  # Packager: rendition presets (Phase 3)
        "shared.ingest.thumbnail",  # Packager: thumbnail knobs (Phase 3)
        "shared.ingest.manifest",   # Packager: licensing defaults + sidecar toggle (Phase 3)
        "shared.ingest.provenance", # C2PA signing (Phase 3)
        "shared.ingest.subclipper", # generic subclipper defaults (Phase 4)
        "shared.ingest.global_cfg", # cross-function globals (later)
    ]
    for m in candidates:
        try:
            __import__(m)
        except ImportError:
            pass   # module not created yet — fine


# ── Config table plumbing ────────────────────────────────────────────

def _load_config():
    for p in [os.environ.get("CONFIG_PATH"),
              os.path.join(_HERE, "..", "config", "config.json")]:
        if p and os.path.isfile(p):
            with open(p) as f:
                return json.load(f)
    raise SystemExit("Could not find config.json")


def _session():
    cfg = _load_config()
    return vastdb.connect(
        endpoint=cfg["vast"]["endpoint"],
        access=cfg["vast"]["access_key"],
        secret=cfg["vast"]["secret_key"],
    ), cfg["vast"]["bucket"], cfg["vast"].get("schema", "media-catalog")


def _ensure_table(session, bucket_name, schema_name):
    with session.transaction() as tx:
        bucket = tx.bucket(bucket_name)
        sch = bucket.schema(schema_name, fail_if_missing=False)
        if sch is None:
            sch = bucket.create_schema(schema_name)
        try:
            tbl = sch.table("function_configs")
            # Add any missing columns (schema evolution — matches VastDBClient.setup_tables)
            existing = {f.name for f in tbl.columns()}
            missing = pa.schema([f for f in FUNCTION_CONFIGS_SCHEMA
                                 if f.name not in existing])
            if len(missing) > 0:
                tbl.add_column(missing)
                print(f"  added {len(missing)} columns to function_configs")
        except Exception:
            tbl = sch.create_table("function_configs", FUNCTION_CONFIGS_SCHEMA)
            print(f"  created function_configs table")
    return tbl


def _all_existing_rows(session, bucket_name, schema_name):
    with session.transaction() as tx:
        tbl = tx.bucket(bucket_name).schema(schema_name).table("function_configs")
        t = tbl.select(internal_row_id=True).read_all()
    return t.to_pylist()


def _insert_rows(session, bucket_name, schema_name, rows):
    arrays = []
    for field in FUNCTION_CONFIGS_SCHEMA:
        arrays.append(pa.array([r.get(field.name) for r in rows], type=field.type))
    batch = pa.RecordBatch.from_arrays(arrays, schema=FUNCTION_CONFIGS_SCHEMA)
    with session.transaction() as tx:
        tbl = tx.bucket(bucket_name).schema(schema_name).table("function_configs")
        tbl.insert(batch)


def _update_row_values(session, bucket_name, schema_name, updates):
    """updates: list of (row_id, new_value_str, new_default_str, new_type, ...)."""
    ROW_ID = "$row_id"
    with session.transaction() as tx:
        tbl = tx.bucket(bucket_name).schema(schema_name).table("function_configs")
        schema = tbl.columns()
        # We only update value + default_value + updated_at + updated_by; keep
        # everything else untouched. So the update schema is tight.
        upd_cols = [f for f in schema if f.name in ("value", "default_value",
                                                   "value_type", "description",
                                                   "min_value", "max_value",
                                                   "ui_group", "ui_order",
                                                   "updated_at", "updated_by")]
        upd_schema = pa.schema([pa.field(ROW_ID, pa.uint64())] + upd_cols)
        for row_id, values in updates:
            arrays = [pa.array([row_id], type=pa.uint64())]
            for f in upd_cols:
                arrays.append(pa.array([values.get(f.name)], type=f.type))
            rb = pa.RecordBatch.from_arrays(arrays, schema=upd_schema)
            tbl.update(rb)


def _build_row(scope, key, entry, now):
    """Turn a CONFIG_SCHEMA entry into a function_configs row."""
    default_json = json.dumps(entry.get("default"))
    return {
        "config_id":     f"{scope}:{key}",
        "scope":         scope,
        "key":           key,
        "value":         default_json,     # start at factory default
        "value_type":    entry.get("type", "json"),
        "default_value": default_json,
        "description":   entry.get("description", ""),
        "min_value":     json.dumps(entry.get("min")) if entry.get("min") is not None else None,
        "max_value":     json.dumps(entry.get("max")) if entry.get("max") is not None else None,
        "ui_group":      entry.get("group", "General"),
        "ui_order":      entry.get("order", 0),
        "updated_at":    now,
        "updated_by":    "seed",
    }


# ── Main ─────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="show what would change, write nothing")
    ap.add_argument("--reset-defaults", action="store_true",
                    help="if a row's value == its stored default_value, update both "
                         "to the NEW factory default (for correcting bad defaults). "
                         "User-edited values are never touched.")
    args = ap.parse_args()

    _import_all_schemas()

    session, bucket_name, schema_name = _session()
    _ensure_table(session, bucket_name, schema_name)

    existing = _all_existing_rows(session, bucket_name, schema_name)
    existing_by_id = {r["config_id"]: r for r in existing}

    to_insert = []
    to_update = []
    unchanged = 0
    now = time.time()

    total_declared = 0
    for scope, keys in iter_registered_schemas():
        for key, entry in keys.items():
            total_declared += 1
            full_entry = {"key": key, **entry}
            row = _build_row(scope, key, full_entry, now)
            cfg_id = row["config_id"]

            if cfg_id not in existing_by_id:
                to_insert.append(row)
                continue

            # Row already exists. Should we update?
            cur = existing_by_id[cfg_id]
            new_default = row["default_value"]
            cur_default = cur.get("default_value")
            cur_value   = cur.get("value")

            # Metadata-only refresh: description, groups, bounds, type
            metadata_drift = any([
                cur.get("description") != row["description"],
                cur.get("ui_group") != row["ui_group"],
                cur.get("ui_order") != row["ui_order"],
                cur.get("min_value") != row["min_value"],
                cur.get("max_value") != row["max_value"],
                cur.get("value_type") != row["value_type"],
                cur_default != new_default,
            ])
            # Value drift: only if flag set AND the user hasn't customized
            value_drift = (
                args.reset_defaults
                and cur_value == cur_default
                and new_default != cur_default
            )

            if metadata_drift or value_drift:
                upd = {
                    "value":         row["value"] if value_drift else cur_value,
                    "default_value": new_default,
                    "value_type":    row["value_type"],
                    "description":   row["description"],
                    "min_value":     row["min_value"],
                    "max_value":     row["max_value"],
                    "ui_group":      row["ui_group"],
                    "ui_order":      row["ui_order"],
                    "updated_at":    now,
                    "updated_by":    "seed",
                }
                to_update.append((cur["$row_id"], upd, cfg_id, metadata_drift, value_drift))
            else:
                unchanged += 1

    print(f"\nDeclared keys: {total_declared}")
    print(f"  existing:  {len(existing_by_id)} rows already in DB")
    print(f"  to insert: {len(to_insert)} new rows")
    print(f"  to update: {len(to_update)} rows (metadata drift or --reset-defaults)")
    print(f"  unchanged: {unchanged} rows\n")

    if args.dry_run:
        for r in to_insert:
            print(f"  + {r['config_id']} = {r['value']} ({r['value_type']})")
        for row_id, upd, cfg_id, meta, val in to_update:
            marks = (["metadata"] if meta else []) + (["value"] if val else [])
            print(f"  ~ {cfg_id}  [{'+'.join(marks)}]")
        print("\n(dry-run — no changes written)")
        return

    if to_insert:
        _insert_rows(session, bucket_name, schema_name, to_insert)
        print(f"inserted {len(to_insert)} rows")
    if to_update:
        _update_row_values(session, bucket_name, schema_name,
                           [(rid, upd) for rid, upd, _, _, _ in to_update])
        print(f"updated {len(to_update)} rows")

    print("\nDone.")


if __name__ == "__main__":
    main()
