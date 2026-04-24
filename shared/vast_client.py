"""VAST DB client for reading and writing provenance data via vastdb + PyArrow."""

import pyarrow as pa
import vastdb

from shared.schemas import ASSETS_SCHEMA


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
        with self._connect() as session:
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

                # Ensure each table exists
                for table_name, schema in tables:
                    try:
                        table = db_schema.table(table_name)
                        log(f"Table '{table_name}' OK ({len(schema)} columns)")
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

    def upsert_asset(self, asset_id: str, fields: dict):
        """Insert or update columns on the unified assets table for a given asset_id.

        Only the columns present in `fields` will be set/updated.
        Missing columns remain null (on insert) or unchanged (on update).

        Args:
            asset_id: The asset identifier (MD5 of s3_path)
            fields: Dict of column_name -> value to set. Must not include asset_id.
        """
        fields["asset_id"] = asset_id

        with self._connect() as session:
            with session.transaction() as tx:
                table = self._get_or_create_table(tx, "assets", ASSETS_SCHEMA)

                # Try to read existing row for this asset_id
                existing = None
                try:
                    predicate = pa.compute.equal(
                        pa.compute.field("asset_id"), pa.scalar(asset_id)
                    )
                    result = table.select(predicate=predicate)
                    if result.num_rows > 0:
                        existing = {
                            col: result.column(col)[0].as_py()
                            for col in result.column_names
                        }
                except Exception:
                    existing = None

                if existing:
                    # Merge: keep existing values, overwrite with new fields
                    merged = existing.copy()
                    merged.update(fields)

                    # Delete old row and insert merged
                    table.delete(predicate)
                    self._insert_asset_row(table, merged)
                else:
                    # New asset: insert with nulls for unset columns
                    self._insert_asset_row(table, fields)

    def _insert_asset_row(self, table, fields: dict):
        """Insert a single row into the assets table, filling missing columns with null."""
        arrays = []
        for schema_field in ASSETS_SCHEMA:
            value = fields.get(schema_field.name)
            arrays.append(pa.array([value], type=schema_field.type))

        batch = pa.RecordBatch.from_arrays(arrays, schema=ASSETS_SCHEMA)
        table.insert(batch)

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

        with self._connect() as session:
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
        with self._connect() as session:
            with session.transaction() as tx:
                bucket = tx.bucket(self.bucket)
                db_schema = bucket.schema(self.schema_name)
                table = db_schema.table(table_name)
                return table.select()

    def query_rows(self, table_name: str, predicate=None) -> pa.Table:
        """Read rows from a VAST DB table with optional predicate filtering."""
        with self._connect() as session:
            with session.transaction() as tx:
                bucket = tx.bucket(self.bucket)
                db_schema = bucket.schema(self.schema_name)
                table = db_schema.table(table_name)
                if predicate:
                    return table.select(predicate=predicate)
                return table.select()
