#!/usr/bin/env python3
"""One-time VAST DB setup: create schema and all tables for the media catalog.

Run this when bootstrapping a new/wiped VAST cluster. It is safe to re-run —
existing schemas and tables are left untouched.

Usage:
    python3 scripts/setup_vastdb.py                     # uses config/config.json
    python3 scripts/setup_vastdb.py /path/to/config.json  # explicit config path

Requires: pip install vastdb pyarrow
"""

import json
import os
import sys

# Allow importing shared modules from the project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pyarrow as pa
import vastdb

from shared.schemas import (
    ASSETS_SCHEMA,
    RELATIONSHIPS_SCHEMA,
    HASH_MATCHES_SCHEMA,
    TALENT_MUSIC_SCHEMA,
    SEMANTIC_EMBEDDINGS_SCHEMA,
    GDPR_PERSONAL_DATA_SCHEMA,
    SYNDICATION_RECORDS_SCHEMA,
    PRODUCTION_ENTITIES_SCHEMA,
    VERSION_HISTORY_SCHEMA,
    ASSET_MOVES_SCHEMA,
)

# ── All tables the system requires ──────────────────────────────────────────
ALL_TABLES = [
    ("assets",              ASSETS_SCHEMA),
    ("relationships",       RELATIONSHIPS_SCHEMA),
    ("hash_matches",        HASH_MATCHES_SCHEMA),
    ("talent_music",        TALENT_MUSIC_SCHEMA),
    ("semantic_embeddings", SEMANTIC_EMBEDDINGS_SCHEMA),
    ("gdpr_personal_data",  GDPR_PERSONAL_DATA_SCHEMA),
    ("syndication_records", SYNDICATION_RECORDS_SCHEMA),
    ("production_entities", PRODUCTION_ENTITIES_SCHEMA),
    ("version_history",     VERSION_HISTORY_SCHEMA),
    ("asset_moves",         ASSET_MOVES_SCHEMA),
]


def load_config(path=None):
    """Load config.json from explicit path, env var, or default location."""
    candidates = [
        path,
        os.environ.get("CONFIG_PATH"),
        os.path.join(os.path.dirname(__file__), "..", "config", "config.json"),
    ]
    for p in candidates:
        if p and os.path.isfile(p):
            with open(p) as f:
                return json.load(f)
    print("ERROR: config.json not found", file=sys.stderr)
    sys.exit(1)


def main():
    config_path = sys.argv[1] if len(sys.argv) > 1 else None
    config = load_config(config_path)

    endpoint = config["vast"]["endpoint"]
    access_key = config["vast"]["access_key"]
    secret_key = config["vast"]["secret_key"]
    bucket_name = config["vast"]["bucket"]
    schema_name = config["vast"].get("schema", "media-catalog")

    print(f"Connecting to VAST DB at {endpoint}")
    session = vastdb.connect(endpoint=endpoint, access=access_key, secret=secret_key)
    print("Connected.")

    with session.transaction() as tx:
        # Verify bucket
        bucket = tx.bucket(bucket_name)
        print(f"Bucket '{bucket_name}' OK")

        # Create or get schema
        db_schema = bucket.schema(schema_name, fail_if_missing=False)
        if db_schema is None:
            db_schema = bucket.create_schema(schema_name)
            print(f"Created schema '{schema_name}'")
        else:
            print(f"Schema '{schema_name}' already exists")

        # Create tables
        created = 0
        existing = 0
        for table_name, schema in ALL_TABLES:
            try:
                table = db_schema.table(table_name)
                print(f"  ✓ {table_name} ({len(schema)} columns) — already exists")
                existing += 1
            except Exception:
                table = db_schema.create_table(table_name, schema)
                print(f"  + {table_name} ({len(schema)} columns) — created")
                created += 1

    print()
    print(f"Done. {created} tables created, {existing} already existed.")
    print(f"Database path: {bucket_name}/{schema_name}")
    print(f"Trino path:    vast.\"{bucket_name}/{schema_name}\".<table>")

    # Verify
    print()
    print("Verification:")
    with session.transaction() as tx:
        db_schema = tx.bucket(bucket_name).schema(schema_name)
        tables = db_schema.tables()
        for t in tables:
            print(f"  - {t.name}")
    print(f"\nTotal: {len(tables)} tables in {bucket_name}/{schema_name}")


if __name__ == "__main__":
    main()
