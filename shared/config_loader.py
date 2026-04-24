"""Shared configuration loader for all media catalog containers."""

import json
import os
import sys


def load_config(config_path: str = None) -> dict:
    """Load configuration from JSON file.

    Resolution order:
    1. Explicit path argument
    2. CONFIG_PATH environment variable
    3. /app/config.json (default container mount point)
    4. ./config/config.json (local development)
    """
    paths = [
        config_path,
        os.environ.get("CONFIG_PATH"),
        "/app/config.json",
        os.path.join(os.path.dirname(__file__), "..", "config", "config.json"),
    ]

    for path in paths:
        if path and os.path.isfile(path):
            with open(path, "r") as f:
                config = json.load(f)
            _validate_config(config)
            return config

    print("ERROR: No config file found. Searched:", [p for p in paths if p], file=sys.stderr)
    sys.exit(1)


def _validate_config(config: dict):
    """Validate required config keys exist."""
    required_sections = ["vast", "s3"]
    for section in required_sections:
        if section not in config:
            raise ValueError(f"Missing required config section: '{section}'")

    vast_keys = ["endpoint", "access_key", "secret_key", "bucket"]
    for key in vast_keys:
        if key not in config["vast"]:
            raise ValueError(f"Missing required vast config key: '{key}'")

    s3_keys = ["endpoint", "access_key", "secret_key"]
    for key in s3_keys:
        if key not in config["s3"]:
            raise ValueError(f"Missing required s3 config key: '{key}'")
