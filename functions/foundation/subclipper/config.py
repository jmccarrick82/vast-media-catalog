"""Runtime-editable configuration loader.

Every function (QC inspector, subclipper, packager, …) declares the knobs
it reads in a `CONFIG_SCHEMA` list on its own module. Those entries get
seeded into the `function_configs` VAST DB table. At runtime, a function
calls `load_config(scope="qc-inspector")` and accesses values via
`cfg.get_float("black_frame_max_ratio_fail")` etc.

Design goals:

* **Never block on DB absence** — if a row is missing, fall back to the
  declared default. A newly-deployed function keeps working even if the
  seed script hasn't been re-run.
* **Never block on DB failures** — transient VAST errors surface as a warning
  and use the last good value (or default). The pipeline keeps flowing.
* **Cache per-pod** — each function container reloads at most every
  ``CONFIG_CACHE_TTL_SECONDS`` so individual handlers don't pay a DB round
  trip per knob. Default 60s; override via env var.
* **Strict type coercion** — the same ``value_type`` drives both the GUI
  widget and the runtime coercion here, so UI and code never disagree.
* **Snapshot loggable** — every handler's first line should log
  ``cfg.snapshot()`` so past-incident logs can replay the exact thresholds
  that were in effect at the time.

The loader is dependency-free apart from vastdb/pyarrow (already in every
function image). It intentionally does NOT import VastDBClient so the
config layer sits below the assets-table client and can be used by any
library module without circular imports.
"""

import json
import os
import threading
import time
from typing import Any, Callable, Optional

import ibis
import pyarrow as pa
import vastdb


# ── Env config for the loader itself ──────────────────────────────────

CONFIG_CACHE_TTL_SECONDS = float(os.environ.get("CONFIG_CACHE_TTL_SECONDS", "60"))
CONFIG_TABLE_NAME = os.environ.get("CONFIG_TABLE_NAME", "function_configs")


# ── Public API ────────────────────────────────────────────────────────

def load_config(scope: str, *, vast_connector: Optional[Callable] = None,
                force_reload: bool = False) -> "Config":
    """Return a Config bound to the given scope.

    `vast_connector` is an optional callable returning a connected vastdb
    session — if not provided, the loader reads VAST endpoint/credentials
    from the same config.json every function uses.
    """
    cache_key = scope
    now = time.monotonic()
    with _cache_lock:
        entry = _cache.get(cache_key)
        if entry is not None and not force_reload:
            cached_at, cfg = entry
            if now - cached_at < CONFIG_CACHE_TTL_SECONDS:
                return cfg

        values = _fetch_scope_from_db(scope, vast_connector=vast_connector)
        cfg = Config(scope=scope, values=values)
        _cache[cache_key] = (now, cfg)
        return cfg


def invalidate_cache(scope: Optional[str] = None):
    """Drop cached configs. Useful right after a UI save so the next
    handler invocation picks up the change immediately.
    """
    with _cache_lock:
        if scope is None:
            _cache.clear()
        else:
            _cache.pop(scope, None)


# ── Config object ─────────────────────────────────────────────────────

# Sentinel used instead of None to distinguish "no default passed" from
# "default is None". Defined before Config so it's in scope for default
# args on the accessor methods.
_MISSING = object()


class Config:
    """Value object holding the fully-coerced settings for one scope.

    Values may come from the DB (current rows) or from declared defaults
    in the calling module. Missing keys fall through to the declared
    default (via ``register_defaults`` below). Missing + undeclared keys
    raise on access so typos are loud.
    """

    _module_defaults: dict[str, dict[str, dict]] = {}  # scope → {key: {type, default, …}}

    def __init__(self, scope: str, values: dict[str, dict]):
        # values shape:  {key: {"value": ..., "value_type": "...", ...}}
        self.scope = scope
        self._values = values or {}

    # ── Accessors ───────────────────────────────────────

    def get(self, key: str, default: Any = _MISSING) -> Any:
        """Raw typed-coerced value for a key. Falls back to declared
        default, then to the passed `default`, then raises KeyError."""
        entry = self._values.get(key)
        if entry is not None:
            raw = entry.get("value")
            vtype = entry.get("value_type", "json")
            return _coerce(raw, vtype)

        declared = self._module_defaults.get(self.scope, {}).get(key)
        if declared is not None:
            return declared["default"]

        if default is not _MISSING:
            return default

        raise KeyError(
            f"No config value for {self.scope}:{key} in DB or declared defaults. "
            f"Did you register the key in the module's CONFIG_SCHEMA?"
        )

    def get_int(self, key, default=_MISSING):
        v = self.get(key, default)
        return int(v) if v is not None else None

    def get_float(self, key, default=_MISSING):
        v = self.get(key, default)
        return float(v) if v is not None else None

    def get_bool(self, key, default=_MISSING):
        v = self.get(key, default)
        if isinstance(v, bool): return v
        if isinstance(v, str):  return v.strip().lower() in ("true", "1", "yes", "on")
        return bool(v)

    def get_string(self, key, default=_MISSING) -> str:
        v = self.get(key, default)
        return "" if v is None else str(v)

    def get_list(self, key, default=_MISSING) -> list:
        v = self.get(key, default)
        if v is None: return []
        if isinstance(v, list): return v
        if isinstance(v, str):
            # Accept comma-separated strings too for convenience
            try: return json.loads(v)
            except json.JSONDecodeError: return [s.strip() for s in v.split(",") if s.strip()]
        raise TypeError(f"{self.scope}:{key} expected list, got {type(v).__name__}")

    def get_duration(self, key, default=_MISSING) -> float:
        """Alias for get_float — exists so code reads as `duration seconds`."""
        return self.get_float(key, default)

    def get_percent(self, key, default=_MISSING) -> float:
        """Returns 0.0–1.0. The UI displays this × 100 as 'N%'."""
        return self.get_float(key, default)

    def get_db(self, key, default=_MISSING) -> float:
        """Alias for get_float — decibels (negative = quieter)."""
        return self.get_float(key, default)

    # ── Introspection ───────────────────────────────────

    def snapshot(self) -> dict:
        """Return {key: resolved_value} for every declared key in this scope.

        Use in handler logs: ``log(f"cfg: {cfg.snapshot()}")``.
        Reflects DB-over-defaults merge so logs show what was actually used.
        """
        out = {}
        declared = self._module_defaults.get(self.scope, {})
        # Cover both declared keys and any DB rows (even if no declaration)
        keys = set(declared.keys()) | set(self._values.keys())
        for k in sorted(keys):
            try:
                out[k] = self.get(k)
            except Exception as e:
                out[k] = f"<error: {e}>"
        return out


# ── Module-level default registration ─────────────────────────────────

def register_defaults(scope: str, schema: list[dict]):
    """Call from each library module at import time to tell the loader
    what keys it expects and what defaults to use if the DB row is missing.

    Example::

        # shared/ingest/qc.py
        from shared.config import register_defaults

        CONFIG_SCOPE = "qc-inspector"
        CONFIG_SCHEMA = [
            {"key": "black_frame_min_run_seconds", "type": "duration_seconds",
             "default": 1.0, "description": "...", "group": "Black frames", "order": 10},
            ...
        ]
        register_defaults(CONFIG_SCOPE, CONFIG_SCHEMA)

    The schema list is also what scripts/seed_function_configs.py consumes
    to populate the DB.
    """
    scope_map = Config._module_defaults.setdefault(scope, {})
    for entry in schema:
        key = entry["key"]
        scope_map[key] = {
            "default":     entry.get("default"),
            "type":        entry.get("type", "json"),
            "description": entry.get("description", ""),
            "min":         entry.get("min"),
            "max":         entry.get("max"),
            "group":       entry.get("group", "General"),
            "order":       entry.get("order", 0),
        }


def iter_registered_schemas():
    """Yield (scope, {key: schema_entry}) for every declared scope.

    Used by the seed script and the webapp's /api/configs endpoint to
    enumerate all known knobs without needing to scan every module
    manually.
    """
    for scope, entries in Config._module_defaults.items():
        yield scope, entries


# ── DB fetch ──────────────────────────────────────────────────────────

_cache: dict[str, tuple[float, Config]] = {}
_cache_lock = threading.Lock()


def _fetch_scope_from_db(scope: str, vast_connector=None) -> dict[str, dict]:
    """Read rows for one scope from `function_configs`. Returns
    {key: {"value": raw_json_string, "value_type": ...}}.

    Never raises on DB errors — logs and returns {} so caller falls back
    to declared defaults.
    """
    try:
        session = (vast_connector or _default_vast_connector)()
        with session.transaction() as tx:
            bucket = tx.bucket(_vast_bucket())
            schema_obj = bucket.schema(_vast_schema())
            tbl = schema_obj.table(CONFIG_TABLE_NAME)
            t = tbl.select(predicate=ibis._.scope == scope).read_all()
    except Exception as e:
        print(f"[config] WARN: failed to read scope={scope} from VAST: {e}")
        return {}

    out = {}
    for r in t.to_pylist():
        key = r.get("key")
        if not key: continue
        out[key] = {
            "value":         r.get("value"),
            "value_type":    r.get("value_type") or "json",
            "description":   r.get("description"),
            "ui_group":      r.get("ui_group"),
            "ui_order":      r.get("ui_order"),
            "min_value":     r.get("min_value"),
            "max_value":     r.get("max_value"),
            "default_value": r.get("default_value"),
            "updated_at":    r.get("updated_at"),
            "updated_by":    r.get("updated_by"),
        }
    return out


def _default_vast_connector():
    """Connect using the same config.json every function uses.

    Prefer an existing session passed in; otherwise fall back to reading
    CONFIG_PATH / default locations.
    """
    import os as _os, json as _json
    for p in (
        _os.environ.get("CONFIG_PATH"),
        "/app/config.json",
        _os.path.join(_os.path.dirname(__file__), "..", "config", "config.json"),
    ):
        if p and _os.path.isfile(p):
            with open(p) as f:
                cfg = _json.load(f)
            return vastdb.connect(
                endpoint=cfg["vast"]["endpoint"],
                access=cfg["vast"]["access_key"],
                secret=cfg["vast"]["secret_key"],
            )
    raise RuntimeError("No config.json found for config loader")


def _vast_bucket() -> str:
    import os as _os, json as _json
    for p in (
        _os.environ.get("CONFIG_PATH"),
        "/app/config.json",
        _os.path.join(_os.path.dirname(__file__), "..", "config", "config.json"),
    ):
        if p and _os.path.isfile(p):
            with open(p) as f:
                return _json.load(f)["vast"]["bucket"]
    raise RuntimeError("No config.json found")


def _vast_schema() -> str:
    import os as _os, json as _json
    for p in (
        _os.environ.get("CONFIG_PATH"),
        "/app/config.json",
        _os.path.join(_os.path.dirname(__file__), "..", "config", "config.json"),
    ):
        if p and _os.path.isfile(p):
            with open(p) as f:
                return _json.load(f)["vast"].get("schema", "media-catalog")
    return "media-catalog"


# ── Type coercion ─────────────────────────────────────────────────────

def _coerce(raw: Any, vtype: str) -> Any:
    """Turn the JSON-encoded string from the DB into a typed Python value."""
    if raw is None or raw == "":
        return None

    # Values are stored JSON-encoded so every type survives a DB round-trip.
    # "1.0" → 1.0, '"h264"' → "h264", '["a","b"]' → ["a","b"], "true" → True.
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except json.JSONDecodeError:
        # Row was written non-JSON-encoded; best-effort treat as string.
        parsed = raw

    if vtype in ("int",):
        return int(parsed) if parsed is not None else None
    if vtype in ("float", "duration_seconds", "percent", "db"):
        return float(parsed) if parsed is not None else None
    if vtype == "bool":
        if isinstance(parsed, bool): return parsed
        if isinstance(parsed, str):  return parsed.strip().lower() in ("true", "1", "yes", "on")
        return bool(parsed)
    if vtype == "string":
        return "" if parsed is None else str(parsed)
    # json, list, dict — pass through as parsed value
    return parsed
