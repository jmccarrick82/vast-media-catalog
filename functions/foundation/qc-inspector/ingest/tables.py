"""Typed row builders + upserts for pre-ingest tables.

Keeps the SQL-ish plumbing out of every function handler. Each helper:

* Handles the merge-semantics quirks of vastdb (where `table.update()`
  rewrites only the columns in the RecordBatch and leaves others intact).
* Returns the final row id of the affected row — handy for chaining.

Use from function handlers::

    from shared.ingest import tables as T
    T.upsert_source_video(session, {"source_id": ..., "s3_inbox_path": ...})
"""

import json
import time
from typing import Optional

import ibis
import pyarrow as pa

# Import SOURCE_VIDEOS_SCHEMA from wherever it lives — in the project it's
# `shared.schemas`; when this file is bundled into a DataEngine function
# the schema file sits at the function root (`schemas`).
try:
    from shared.schemas import SOURCE_VIDEOS_SCHEMA
except ImportError:
    from schemas import SOURCE_VIDEOS_SCHEMA  # type: ignore

_ROW_ID = "$row_id"


# ── Source videos ────────────────────────────────────────────────────

def upsert_source_video(session, bucket: str, schema: str, fields: dict,
                         create_if_missing: bool = True) -> str:
    """Upsert one row in `source_videos` keyed by source_id.

    Fields with value None are ignored (won't overwrite existing values).
    Always stamps updated_at.

    Returns the `source_id` of the affected row.
    """
    source_id = fields["source_id"]
    fields = {k: v for k, v in fields.items() if v is not None}
    fields.setdefault("created_at", time.time())
    fields["updated_at"] = time.time()

    with session.transaction() as tx:
        tbl = tx.bucket(bucket).schema(schema).table("source_videos")
        existing = tbl.select(
            predicate=ibis._.source_id == source_id,
            internal_row_id=True,
        ).read_all()

        if existing.num_rows == 0:
            if not create_if_missing:
                raise ValueError(f"source_videos row for {source_id} not found")
            # Insert: fill every column from SOURCE_VIDEOS_SCHEMA
            arrays = []
            for f in SOURCE_VIDEOS_SCHEMA:
                arrays.append(pa.array([fields.get(f.name)], type=f.type))
            rb = pa.RecordBatch.from_arrays(arrays, schema=SOURCE_VIDEOS_SCHEMA)
            tbl.insert(rb)
            return source_id

        # Update: only the provided columns on the first matching row
        row_id = existing.column(_ROW_ID)[0].as_py()
        set_cols = [f for f in SOURCE_VIDEOS_SCHEMA if f.name in fields]
        if not set_cols:
            return source_id
        update_schema = pa.schema([pa.field(_ROW_ID, pa.uint64())] + set_cols)
        arrays = [pa.array([row_id], type=pa.uint64())]
        for f in set_cols:
            arrays.append(pa.array([fields.get(f.name)], type=f.type))
        rb = pa.RecordBatch.from_arrays(arrays, schema=update_schema)
        tbl.update(rb)
        return source_id


# ── Schema setup (idempotent — also handles column evolution) ────────

def ensure_source_videos_table(session, bucket: str, schema: str):
    """Create the `source_videos` table if missing, or add any columns
    that exist in SOURCE_VIDEOS_SCHEMA but not in the live table."""
    with session.transaction() as tx:
        b = tx.bucket(bucket)
        sch = b.schema(schema, fail_if_missing=False)
        if sch is None:
            sch = b.create_schema(schema)
        try:
            tbl = sch.table("source_videos")
            existing = {f.name for f in tbl.columns()}
            missing = pa.schema([f for f in SOURCE_VIDEOS_SCHEMA
                                 if f.name not in existing])
            if len(missing) > 0:
                tbl.add_column(missing)
        except Exception:
            sch.create_table("source_videos", SOURCE_VIDEOS_SCHEMA)


# ── Convenience: JSON encode runs for storage ────────────────────────

def json_runs(runs) -> str:
    """Serialize [{start,end}, ...] → JSON string for the *_runs_json columns."""
    try:
        return json.dumps(runs or [])
    except (TypeError, ValueError):
        return "[]"
