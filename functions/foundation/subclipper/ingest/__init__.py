"""Shared-ingest library — reusable primitives for pre-ingest workflows.

Modules here are called by DataEngine functions (qc-inspector, subclipper,
packager, …) and also imported directly by any ad-hoc script or future
workflow. Rules of the road:

- Each module is a small, single-purpose set of pure functions
  (no S3/DB side effects unless explicitly documented).
- Every module that has runtime-editable knobs calls
  `shared.config.register_defaults(SCOPE, CONFIG_SCHEMA)` at import time.
  That makes its knobs discoverable by the seed script and the
  /settings UI without any manual registration.
- No imports from any specific DataEngine function — this is the shared
  layer everyone depends on, not the other way around.
"""
