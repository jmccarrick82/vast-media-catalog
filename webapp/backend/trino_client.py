"""Trino query client for the Media Catalog webapp."""

from trino.dbapi import connect


class TrinoClient:
    """Executes SQL queries against Trino and returns results as dicts."""

    def __init__(self, config: dict):
        self.host = config.get("host", "localhost")
        self.port = config.get("port", 8080)
        self.catalog = config.get("catalog", "vast")
        self.schema = config.get("schema", "")
        self.user = config.get("user", "trino")

    def execute(self, sql: str) -> tuple[list[dict], list[str]]:
        """Execute a SQL query and return (rows_as_dicts, column_names)."""
        conn = connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema,
        )
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            raw_rows = cursor.fetchall()
            rows = [dict(zip(columns, row)) for row in raw_rows]
            return rows, columns
        finally:
            conn.close()
