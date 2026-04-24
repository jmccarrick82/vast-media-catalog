"""Trino-based client for querying VAST Big Catalog and Audit Log.

Used by the catalog-reconciler to:
  1. List all files currently on the cluster (Big Catalog)
  2. Investigate missing assets via protocol audit log (S3, NFS, SMB)
"""

from trino.dbapi import connect


# Default table locations on VAST
DEFAULT_CATALOG_SCHEMA = "vast-big-catalog-bucket/vast_big_catalog_schema"
DEFAULT_CATALOG_TABLE = "vast_big_catalog_table"
DEFAULT_AUDIT_SCHEMA = "vast-audit-log-bucket/vast_audit_log_schema"
DEFAULT_AUDIT_TABLE = "vast_audit_log_table"


class CatalogClient:
    """Query VAST Big Catalog and Audit Log via Trino."""

    def __init__(self, config: dict):
        trino_cfg = config.get("trino", {})
        self.host = trino_cfg.get("host", "localhost")
        self.port = trino_cfg.get("port", 8080)
        self.trino_catalog = trino_cfg.get("catalog", "vast")
        self.user = trino_cfg.get("user", "trino")

        catalog_cfg = config.get("catalog", {})
        cat_schema = catalog_cfg.get("big_catalog_table", DEFAULT_CATALOG_SCHEMA)
        audit_schema = catalog_cfg.get("audit_log_table", DEFAULT_AUDIT_SCHEMA)

        self._catalog_table = (
            f'{self.trino_catalog}."{cat_schema}".{DEFAULT_CATALOG_TABLE}'
        )
        self._audit_table = (
            f'{self.trino_catalog}."{audit_schema}".{DEFAULT_AUDIT_TABLE}'
        )
        self.media_search_path = catalog_cfg.get("media_search_path", "")

    def _connect(self):
        """Create a Trino connection (no schema — schemas are in the SQL)."""
        return connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.trino_catalog,
        )

    def _execute(self, sql: str) -> list[dict]:
        """Execute SQL, return rows as list of dicts."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            conn.close()

    # ── Big Catalog queries ───────────────────────────────────────────────────

    def list_catalog_files(self, search_path: str = "") -> dict[str, int]:
        """Query VAST Big Catalog for all files under a search path.

        Returns dict mapping full_path -> file_size.
        """
        path = search_path or self.media_search_path
        where_clause = ""
        if path:
            where_clause = f"AND search_path = '{_escape(path)}'"

        sql = f"""
            SELECT concat(parent_path, name) AS full_path, size
            FROM {self._catalog_table}
            WHERE element_type = 'FILE'
            {where_clause}
        """
        rows = self._execute(sql)
        return {row["full_path"]: row["size"] for row in rows}

    # ── Audit Log queries ─────────────────────────────────────────────────────

    def find_rename_events(self, file_path: str, lookback_seconds: int) -> list[dict]:
        """Search audit log for RENAME/move operations on a specific path.

        Checks all 3 protocols: NFS RENAME, SMB RENAME, S3 CopyObject.
        Returns list of dicts with protocol, rpc_type, time, rename_destination, etc.
        """
        dir_path, file_name = _split_path(file_path)

        sql = f"""
            SELECT
                protocol,
                rpc_type,
                time,
                "path"."path" AS source_path,
                "name"."name" AS source_name,
                rename_path,
                rename_name,
                smb_rename_struct,
                s3_source_object,
                login_name,
                uid,
                client_ip,
                transaction_id,
                status
            FROM {self._audit_table}
            WHERE time >= current_timestamp - interval '{int(lookback_seconds)}' second
            AND (
                (rpc_type = 'RENAME' AND "path"."path" = '{_escape(dir_path)}'
                    AND "name"."name" = '{_escape(file_name)}')
                OR (rpc_type = 'CopyObject'
                    AND s3_source_object IS NOT NULL)
            )
            ORDER BY time DESC
        """
        rows = self._execute(sql)

        results = []
        for row in rows:
            dest = _extract_rename_destination(row)
            if not dest:
                continue

            # For S3 CopyObject, check that the source matches our file
            if row["rpc_type"] == "CopyObject":
                source_obj = row.get("s3_source_object")
                if source_obj and not _s3_source_matches(source_obj, file_path):
                    continue

            results.append({
                "protocol": row["protocol"],
                "rpc_type": row["rpc_type"],
                "time": row["time"],
                "rename_destination": dest,
                "login_name": row.get("login_name", ""),
                "uid": row.get("uid"),
                "client_ip": row.get("client_ip", ""),
                "transaction_id": row.get("transaction_id"),
                "status": row.get("status", ""),
            })

        return results

    def find_delete_events(self, file_path: str, lookback_seconds: int) -> list[dict]:
        """Search audit log for DELETE operations on a specific path.

        Checks all 3 protocols: S3 DeleteObject, NFS REMOVE/RMDIR, SMB delete-on-close.
        """
        dir_path, file_name = _split_path(file_path)

        sql = f"""
            SELECT
                protocol,
                rpc_type,
                time,
                "path"."path" AS source_path,
                "name"."name" AS source_name,
                smb_delete_on_close,
                login_name,
                uid,
                client_ip,
                status
            FROM {self._audit_table}
            WHERE time >= current_timestamp - interval '{int(lookback_seconds)}' second
            AND "path"."path" = '{_escape(dir_path)}'
            AND "name"."name" = '{_escape(file_name)}'
            AND (
                rpc_type IN ('DeleteObject', 'REMOVE', 'RMDIR', 'DELETE')
                OR (rpc_type = 'CLOSE' AND smb_delete_on_close = true)
                OR (rpc_type = 'SET_INFO' AND smb_delete_on_close = true)
            )
            ORDER BY time DESC
        """
        rows = self._execute(sql)

        return [
            {
                "protocol": row["protocol"],
                "rpc_type": row["rpc_type"],
                "time": row["time"],
                "login_name": row.get("login_name", ""),
                "uid": row.get("uid"),
                "client_ip": row.get("client_ip", ""),
                "status": row.get("status", ""),
            }
            for row in rows
        ]

    def query_all_events(self, file_path: str, lookback_seconds: int) -> list[dict]:
        """Get ALL audit log events for a path (fallback for debugging)."""
        dir_path, file_name = _split_path(file_path)

        sql = f"""
            SELECT
                protocol,
                rpc_type,
                time,
                "path"."path" AS source_path,
                "name"."name" AS source_name,
                rename_path,
                rename_name,
                smb_delete_on_close,
                login_name,
                uid,
                client_ip,
                transaction_id,
                status,
                num_bytes
            FROM {self._audit_table}
            WHERE time >= current_timestamp - interval '{int(lookback_seconds)}' second
            AND "path"."path" = '{_escape(dir_path)}'
            AND "name"."name" = '{_escape(file_name)}'
            ORDER BY time DESC
        """
        return self._execute(sql)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _escape(value: str) -> str:
    """Escape single quotes for SQL string literals."""
    return value.replace("'", "''")


def _split_path(full_path: str) -> tuple[str, str]:
    """Split '/dir/subdir/file.mp4' into ('/dir/subdir', 'file.mp4')."""
    full_path = full_path.rstrip("/")
    idx = full_path.rfind("/")
    if idx < 0:
        return "", full_path
    return full_path[:idx], full_path[idx + 1:]


def _extract_rename_destination(row: dict) -> str:
    """Extract the rename destination path from an audit log row.

    Checks protocol-specific fields in order:
      1. rename_path + rename_name (NFS/SMB)
      2. smb_rename_struct.path (SMB)
      3. path.path + name.name of a CopyObject row (S3 — the row itself IS the destination)
    """
    # NFS/SMB: rename_path contains the destination directory
    rename_path = row.get("rename_path")
    rename_name = row.get("rename_name")
    if rename_path and rename_name:
        rp = rename_path.rstrip("/")
        return f"{rp}/{rename_name}"
    if rename_path:
        return rename_path

    # SMB: smb_rename_struct is a row type with .path
    smb_struct = row.get("smb_rename_struct")
    if smb_struct and isinstance(smb_struct, (dict, tuple)):
        if isinstance(smb_struct, dict):
            smb_path = smb_struct.get("path", "")
        else:
            # Trino returns row types as tuples: (smb_ads_name, path)
            smb_path = smb_struct[-1] if len(smb_struct) > 1 else ""
        if smb_path:
            return smb_path

    # S3 CopyObject: the row's own path IS the destination
    if row.get("rpc_type") == "CopyObject":
        source_path = row.get("source_path", "")
        source_name = row.get("source_name", "")
        if source_path and source_name:
            return f"{source_path.rstrip('/')}/{source_name}"

    return ""


def _s3_source_matches(s3_source_object, file_path: str) -> bool:
    """Check if an s3_source_object row matches the file we're looking for."""
    if not s3_source_object:
        return False

    if isinstance(s3_source_object, dict):
        bucket = s3_source_object.get("s3_bucket_name", "")
        name_obj = s3_source_object.get("name", {})
        name = name_obj.get("name", "") if isinstance(name_obj, dict) else ""
        source_full = f"/{bucket}/{name}" if bucket else name
    else:
        return False

    # Normalize for comparison
    return file_path.rstrip("/") == source_full.rstrip("/")
