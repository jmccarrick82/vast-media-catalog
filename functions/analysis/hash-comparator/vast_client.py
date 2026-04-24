"""VAST DB client for reading and writing provenance data via vastdb + PyArrow."""

import random
import time

import ibis
import pyarrow as pa
import vastdb

from schemas import ASSETS_SCHEMA

# Name of the internal row-id column returned when select(internal_row_id=True)
_ROW_ID_COL = "$row_id"


class VastDBClient:
    """Client for interacting with VAST DB tables."""

    def __init__(self, config: dict):
        self.endpoint = config["vast"]["endpoint"]
        self.access_key = config["vast"]["access_key"]
        self.secret_key = config["vast"]["secret_key"]
        self.bucket = config["vast"]["bucket"]
        self.schema_name = config["vast"].get("schema", "media-catalog")

    def _connect(self):
        """Create a VAST DB session."""
        return vastdb.connect(
            endpoint=self.endpoint,
            access=self.access_key,
            secret=self.secret_key,
        )

    def setup_tables(self, tables: list[tuple[str, pa.Schema]], logger=None):
        """Verify connectivity and ensure all required tables exist.

        Call this from init() to fail fast if VAST is unreachable or the
        bucket is missing, and to pre-create tables with correct schemas.

        Args:
            tables: List of (table_name, schema) tuples to ensure exist.
            logger: Optional logger (ctx.logger) for status messages.

        Raises:
            Exception: If VAST DB is unreachable, bucket doesn't exist, or
                       table creation fails.
        """
        log = logger.info if logger else print

        log(f"Connecting to VAST DB at {self.endpoint}")
        session = self._connect()
        with session.transaction() as tx:
            # Verify bucket exists
            bucket = tx.bucket(self.bucket)
            log(f"Bucket '{self.bucket}' OK")

            # Ensure schema exists
            db_schema = bucket.schema(self.schema_name, fail_if_missing=False)
            if db_schema is None:
                db_schema = bucket.create_schema(self.schema_name)
                log(f"Created schema '{self.schema_name}'")
            else:
                log(f"Schema '{self.schema_name}' OK")

            # Ensure each table exists and has all expected columns
            for table_name, schema in tables:
                try:
                    table = db_schema.table(table_name)
                    # Schema evolution: add any missing columns
                    existing_cols = {f.name for f in table.columns()}
                    missing = pa.schema([
                        f for f in schema if f.name not in existing_cols
                    ])
                    if len(missing) > 0:
                        table.add_column(missing)
                        log(f"Table '{table_name}': added {len(missing)} new columns: "
                            f"{[f.name for f in missing]}")
                    log(f"Table '{table_name}' OK ({len(table.columns())} columns)")
                except Exception:
                    table = db_schema.create_table(table_name, schema)
                    log(f"Created table '{table_name}' ({len(schema)} columns)")

        log("VAST DB setup complete")

    def _get_or_create_table(self, tx, table_name: str, schema: pa.Schema):
        """Get existing table or create it with the given schema."""
        bucket = tx.bucket(self.bucket)
        db_schema = bucket.schema(self.schema_name, fail_if_missing=False)
        if db_schema is None:
            db_schema = bucket.create_schema(self.schema_name)

        try:
            table = db_schema.table(table_name)
        except Exception:
            table = db_schema.create_table(table_name, schema)
        return table

    # ------------------------------------------------------------------
    # Unified assets table: upsert (insert or update) a single asset row
    # ------------------------------------------------------------------

    def upsert_asset(self, asset_id: str, fields: dict, max_retries: int = 5):
        """Insert or update columns on the unified assets table for a given asset_id.

        Only the columns present in `fields` will be set/updated. Missing
        columns remain null (on insert) or unchanged (on update).

        Race handling:
            Eight foundation/analysis functions fan out from the same S3 event
            and all upsert into the same asset_id row. The old implementation
            used a broken read→delete→insert path that (a) couldn't actually
            tell if a row existed because RecordBatchReader has no .num_rows,
            and (b) called table.delete() with a predicate, but vastdb's
            delete() requires a RecordBatch carrying $row_id. Net effect:
            every call always inserted a brand-new row → 8 rows per asset.

            The new implementation:
              1. selects with internal_row_id=True and materializes the reader
                 via read_all() so we can actually count rows.
              2. if 0 rows exist, inserts a fresh row.
              3. if ≥1 rows exist, merges existing values + new fields into
                 row 0 (via table.update on $row_id) and row-id-deletes the
                 rest. This self-heals any previous duplicates created by the
                 race window and the broken old code.
              4. wraps everything in a retry loop so concurrent writers that
                 lose an optimistic-concurrency conflict can re-read and
                 re-merge instead of silently dropping their fields.

        Args:
            asset_id: The asset identifier (MD5 of s3_path)
            fields: Dict of column_name -> value to set. Must not include asset_id.
            max_retries: How many times to retry on transient transaction errors.
        """
        fields = dict(fields)  # don't mutate caller's dict
        fields["asset_id"] = asset_id

        last_err = None
        for attempt in range(max_retries):
            try:
                session = self._connect()
                with session.transaction() as tx:
                    table = self._get_or_create_table(tx, "assets", ASSETS_SCHEMA)

                    # vastdb uses ibis expressions for predicates, not
                    # pyarrow.compute (which fails with AttributeError on
                    # .op() deep inside the serializer).
                    predicate = ibis._.asset_id == asset_id

                    # Materialize existing rows with their $row_id
                    reader = table.select(
                        predicate=predicate,
                        internal_row_id=True,
                    )
                    existing_tbl = reader.read_all()
                    n_existing = existing_tbl.num_rows

                    if n_existing == 0:
                        # No row yet — simple insert
                        self._insert_asset_row(table, fields)
                        return

                    # Merge existing row 0 values with our new fields.
                    # Non-null existing values stay; new fields always win.
                    merged = {}
                    for col in existing_tbl.column_names:
                        if col == _ROW_ID_COL:
                            continue
                        val = existing_tbl.column(col)[0].as_py()
                        if val is not None:
                            merged[col] = val

                    # If multiple duplicates exist, coalesce their values too
                    # (take the first non-null for each column across all
                    # duplicate rows before applying our new fields).
                    if n_existing > 1:
                        for row_idx in range(1, n_existing):
                            for col in existing_tbl.column_names:
                                if col == _ROW_ID_COL:
                                    continue
                                if merged.get(col) is None:
                                    val = existing_tbl.column(col)[row_idx].as_py()
                                    if val is not None:
                                        merged[col] = val

                    merged.update(fields)
                    merged["asset_id"] = asset_id

                    # Update row 0 via its $row_id
                    first_row_id = existing_tbl.column(_ROW_ID_COL)[0].as_py()
                    self._update_asset_row(table, first_row_id, merged)

                    # Delete any duplicate rows created by the old buggy path
                    # or by a parallel insert race
                    if n_existing > 1:
                        extra_ids = [
                            existing_tbl.column(_ROW_ID_COL)[i].as_py()
                            for i in range(1, n_existing)
                        ]
                        self._delete_rows_by_id(table, extra_ids)

                    return
            except Exception as e:
                # Retry with jittered backoff on any transient error
                last_err = e
                if attempt == max_retries - 1:
                    raise
                time.sleep(0.05 * (attempt + 1) + random.uniform(0, 0.05))

        # Unreachable — the loop either returns or raises
        raise RuntimeError(f"upsert_asset failed after {max_retries} attempts: {last_err}")

    def _insert_asset_row(self, table, fields: dict):
        """Insert a single row into the assets table, filling missing columns with null."""
        arrays = []
        for schema_field in ASSETS_SCHEMA:
            value = fields.get(schema_field.name)
            arrays.append(pa.array([value], type=schema_field.type))

        batch = pa.RecordBatch.from_arrays(arrays, schema=ASSETS_SCHEMA)
        table.insert(batch)

    def _update_asset_row(self, table, row_id: int, fields: dict):
        """Update a single row in the assets table identified by $row_id."""
        # Build schema: $row_id first, then all the columns we want to set
        set_fields = [f for f in ASSETS_SCHEMA if f.name in fields]
        update_schema = pa.schema(
            [pa.field(_ROW_ID_COL, pa.uint64())] + set_fields
        )
        arrays = [pa.array([row_id], type=pa.uint64())]
        for f in set_fields:
            arrays.append(pa.array([fields.get(f.name)], type=f.type))
        rb = pa.RecordBatch.from_arrays(arrays, schema=update_schema)
        table.update(rb)

    def _delete_rows_by_id(self, table, row_ids: list):
        """Delete rows from the assets table by their $row_id values."""
        if not row_ids:
            return
        rb = pa.RecordBatch.from_arrays(
            [pa.array(row_ids, type=pa.uint64())],
            schema=pa.schema([pa.field(_ROW_ID_COL, pa.uint64())]),
        )
        table.delete(rb)

    # ------------------------------------------------------------------
    # Generic multi-row table operations (for relationship tables, etc.)
    # ------------------------------------------------------------------

    def write_rows(self, table_name: str, schema: pa.Schema, rows: list[dict]):
        """Write rows to a VAST DB table.

        Args:
            table_name: Target table name
            schema: PyArrow schema for the table
            rows: List of dicts matching the schema fields
        """
        if not rows:
            return

        arrays = []
        for field in schema:
            values = [row.get(field.name) for row in rows]
            arrays.append(pa.array(values, type=field.type))

        record_batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

        session = self._connect()
        with session.transaction() as tx:
            table = self._get_or_create_table(tx, table_name, schema)
            table.insert(record_batch)

    def load_table_safe(self, table_name: str, logger=None) -> pa.Table | None:
        """Read a table, returning None if it doesn't exist yet."""
        try:
            return self.read_table(table_name)
        except Exception:
            if logger:
                logger.info(f"Table '{table_name}' not found or empty, skipping")
            return None

    def read_table(self, table_name: str) -> pa.Table:
        """Read all rows from a VAST DB table."""
        session = self._connect()
        with session.transaction() as tx:
            bucket = tx.bucket(self.bucket)
            db_schema = bucket.schema(self.schema_name)
            table = db_schema.table(table_name)
            return table.select().read_all()

    def query_rows(self, table_name: str, predicate=None) -> pa.Table:
        """Read rows from a VAST DB table with optional predicate filtering."""
        session = self._connect()
        with session.transaction() as tx:
            bucket = tx.bucket(self.bucket)
            db_schema = bucket.schema(self.schema_name)
            table = db_schema.table(table_name)
            if predicate:
                return table.select(predicate=predicate).read_all()
            return table.select().read_all()
