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
    from shared.schemas import (
        SOURCE_VIDEOS_SCHEMA, EXTRACTED_CLIPS_SCHEMA,
        DELIVERY_PACKAGES_SCHEMA, PACKAGE_RENDITIONS_SCHEMA,
    )
except ImportError:
    from schemas import (  # type: ignore
        SOURCE_VIDEOS_SCHEMA, EXTRACTED_CLIPS_SCHEMA,
        DELIVERY_PACKAGES_SCHEMA, PACKAGE_RENDITIONS_SCHEMA,
    )

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


# ── Extracted clips ──────────────────────────────────────────────────

def upsert_extracted_clip(session, bucket: str, schema: str, fields: dict,
                          create_if_missing: bool = True) -> str:
    """Upsert one row in `extracted_clips` keyed by clip_id.

    Fields with value None are ignored. Stamps updated_at.
    Returns clip_id.
    """
    clip_id = fields["clip_id"]
    fields = {k: v for k, v in fields.items() if v is not None}
    fields.setdefault("created_at", time.time())
    fields["updated_at"] = time.time()

    with session.transaction() as tx:
        tbl = tx.bucket(bucket).schema(schema).table("extracted_clips")
        existing = tbl.select(
            predicate=ibis._.clip_id == clip_id,
            internal_row_id=True,
        ).read_all()

        if existing.num_rows == 0:
            if not create_if_missing:
                raise ValueError(f"extracted_clips row for {clip_id} not found")
            arrays = []
            for f in EXTRACTED_CLIPS_SCHEMA:
                arrays.append(pa.array([fields.get(f.name)], type=f.type))
            rb = pa.RecordBatch.from_arrays(arrays, schema=EXTRACTED_CLIPS_SCHEMA)
            tbl.insert(rb)
            return clip_id

        row_id = existing.column(_ROW_ID)[0].as_py()
        set_cols = [f for f in EXTRACTED_CLIPS_SCHEMA if f.name in fields]
        if not set_cols:
            return clip_id
        update_schema = pa.schema([pa.field(_ROW_ID, pa.uint64())] + set_cols)
        arrays = [pa.array([row_id], type=pa.uint64())]
        for f in set_cols:
            arrays.append(pa.array([fields.get(f.name)], type=f.type))
        rb = pa.RecordBatch.from_arrays(arrays, schema=update_schema)
        tbl.update(rb)
        return clip_id


def ensure_extracted_clips_table(session, bucket: str, schema: str):
    """Create the `extracted_clips` table if missing, or add missing columns."""
    with session.transaction() as tx:
        b = tx.bucket(bucket)
        sch = b.schema(schema, fail_if_missing=False)
        if sch is None:
            sch = b.create_schema(schema)
        try:
            tbl = sch.table("extracted_clips")
            existing = {f.name for f in tbl.columns()}
            missing = pa.schema([f for f in EXTRACTED_CLIPS_SCHEMA
                                 if f.name not in existing])
            if len(missing) > 0:
                tbl.add_column(missing)
        except Exception:
            sch.create_table("extracted_clips", EXTRACTED_CLIPS_SCHEMA)


# ── Delivery packages ────────────────────────────────────────────────

def upsert_delivery_package(session, bucket: str, schema: str, fields: dict,
                             create_if_missing: bool = True) -> str:
    """Upsert one row in `delivery_packages` keyed by package_id."""
    package_id = fields["package_id"]
    fields = {k: v for k, v in fields.items() if v is not None}
    fields.setdefault("created_at", time.time())
    fields["updated_at"] = time.time()

    with session.transaction() as tx:
        tbl = tx.bucket(bucket).schema(schema).table("delivery_packages")
        existing = tbl.select(
            predicate=ibis._.package_id == package_id,
            internal_row_id=True,
        ).read_all()

        if existing.num_rows == 0:
            if not create_if_missing:
                raise ValueError(f"delivery_packages row for {package_id} not found")
            arrays = []
            for f in DELIVERY_PACKAGES_SCHEMA:
                arrays.append(pa.array([fields.get(f.name)], type=f.type))
            rb = pa.RecordBatch.from_arrays(arrays, schema=DELIVERY_PACKAGES_SCHEMA)
            tbl.insert(rb)
            return package_id

        row_id = existing.column(_ROW_ID)[0].as_py()
        set_cols = [f for f in DELIVERY_PACKAGES_SCHEMA if f.name in fields]
        if not set_cols:
            return package_id
        update_schema = pa.schema([pa.field(_ROW_ID, pa.uint64())] + set_cols)
        arrays = [pa.array([row_id], type=pa.uint64())]
        for f in set_cols:
            arrays.append(pa.array([fields.get(f.name)], type=f.type))
        rb = pa.RecordBatch.from_arrays(arrays, schema=update_schema)
        tbl.update(rb)
        return package_id


def ensure_delivery_packages_table(session, bucket: str, schema: str):
    with session.transaction() as tx:
        b = tx.bucket(bucket)
        sch = b.schema(schema, fail_if_missing=False)
        if sch is None:
            sch = b.create_schema(schema)
        try:
            tbl = sch.table("delivery_packages")
            existing = {f.name for f in tbl.columns()}
            missing = pa.schema([f for f in DELIVERY_PACKAGES_SCHEMA
                                 if f.name not in existing])
            if len(missing) > 0:
                tbl.add_column(missing)
        except Exception:
            sch.create_table("delivery_packages", DELIVERY_PACKAGES_SCHEMA)


# ── Package renditions ───────────────────────────────────────────────

def upsert_package_rendition(session, bucket: str, schema: str, fields: dict,
                              create_if_missing: bool = True) -> str:
    """Upsert one row in `package_renditions` keyed by rendition_id."""
    rendition_id = fields["rendition_id"]
    fields = {k: v for k, v in fields.items() if v is not None}
    fields.setdefault("created_at", time.time())
    fields["updated_at"] = time.time()

    with session.transaction() as tx:
        tbl = tx.bucket(bucket).schema(schema).table("package_renditions")
        existing = tbl.select(
            predicate=ibis._.rendition_id == rendition_id,
            internal_row_id=True,
        ).read_all()

        if existing.num_rows == 0:
            if not create_if_missing:
                raise ValueError(f"package_renditions row for {rendition_id} not found")
            arrays = []
            for f in PACKAGE_RENDITIONS_SCHEMA:
                arrays.append(pa.array([fields.get(f.name)], type=f.type))
            rb = pa.RecordBatch.from_arrays(arrays, schema=PACKAGE_RENDITIONS_SCHEMA)
            tbl.insert(rb)
            return rendition_id

        row_id = existing.column(_ROW_ID)[0].as_py()
        set_cols = [f for f in PACKAGE_RENDITIONS_SCHEMA if f.name in fields]
        if not set_cols:
            return rendition_id
        update_schema = pa.schema([pa.field(_ROW_ID, pa.uint64())] + set_cols)
        arrays = [pa.array([row_id], type=pa.uint64())]
        for f in set_cols:
            arrays.append(pa.array([fields.get(f.name)], type=f.type))
        rb = pa.RecordBatch.from_arrays(arrays, schema=update_schema)
        tbl.update(rb)
        return rendition_id


def ensure_package_renditions_table(session, bucket: str, schema: str):
    with session.transaction() as tx:
        b = tx.bucket(bucket)
        sch = b.schema(schema, fail_if_missing=False)
        if sch is None:
            sch = b.create_schema(schema)
        try:
            tbl = sch.table("package_renditions")
            existing = {f.name for f in tbl.columns()}
            missing = pa.schema([f for f in PACKAGE_RENDITIONS_SCHEMA
                                 if f.name not in existing])
            if len(missing) > 0:
                tbl.add_column(missing)
        except Exception:
            sch.create_table("package_renditions", PACKAGE_RENDITIONS_SCHEMA)


# ── Convenience: JSON encode runs for storage ────────────────────────

def json_runs(runs) -> str:
    """Serialize [{start,end}, ...] → JSON string for the *_runs_json columns."""
    try:
        return json.dumps(runs or [])
    except (TypeError, ValueError):
        return "[]"
