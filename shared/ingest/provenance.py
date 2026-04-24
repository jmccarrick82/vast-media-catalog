"""C2PA provenance signing via the c2patool CLI.

c2patool is bundled as a binary in each function image (same pattern as
ffmpeg/ffprobe). This module is a thin wrapper that:

  1. Builds the per-rendition claim dict (delegated to manifest.py)
  2. Writes it to a temp JSON
  3. Invokes c2patool to embed the signed manifest into the MP4
  4. Parses the result and returns structured info (manifest label, etc.)

Verification is the mirror: `verify_c2pa(path)` runs `c2patool <path>`
and parses its JSON report.

The signing cert + private key paths are read from `function_configs`
under scope `provenance`, so the operator can point at a different key
without a redeploy.
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import time
from dataclasses import dataclass
from typing import Optional

try:
    from shared.config import register_defaults
except ImportError:
    from config import register_defaults  # type: ignore

from .manifest import build_c2pa_claim_for_rendition


CONFIG_SCOPE = "provenance"


# The function image bundles the signing cert + key under this default
# path; operators can override via the Settings UI.
_DEFAULT_CERT_DIR = "/workspace/c2pa-signing"

PROVENANCE_CONFIG_SCHEMA = [
    {
        "key":         "c2pa_enabled",
        "type":        "bool",
        "default":     True,
        "group":       "C2PA",
        "order":       10,
        "description": "Master switch. Off = unsigned renditions (sidecar manifest only).",
    },
    {
        "key":         "signing_cert_path",
        "type":        "string",
        "default":     f"{_DEFAULT_CERT_DIR}/signing.pub",
        "group":       "C2PA",
        "order":       20,
        "description": "PEM-encoded X.509 public certificate to attach to each signed manifest.",
    },
    {
        "key":         "signing_key_path",
        "type":        "string",
        "default":     f"{_DEFAULT_CERT_DIR}/signing.key",
        "group":       "C2PA",
        "order":       30,
        "description": "PEM-encoded private key matching signing_cert_path.",
    },
    {
        "key":         "signing_algorithm",
        "type":        "string",
        "default":     "es256",
        "group":       "C2PA",
        "order":       40,
        "description": "COSE signing algorithm. Must match the cert key type. Common: es256, es384, ps256.",
    },
    {
        "key":         "claim_generator",
        "type":        "string",
        "default":     "vast-media-catalog",
        "group":       "C2PA",
        "order":       50,
        "description": "Claim generator name embedded in every manifest. Identifies the pipeline.",
    },
    {
        "key":         "claim_generator_version",
        "type":        "string",
        "default":     "0.1.0",
        "group":       "C2PA",
        "order":       60,
        "description": "Claim generator version string.",
    },
    {
        "key":         "timestamp_authority_url",
        "type":        "string",
        "default":     "http://timestamp.digicert.com",
        "group":       "C2PA",
        "order":       70,
        "description": "RFC3161 TSA URL for signing timestamps. Empty string = no timestamp (faster but signatures aren't time-anchored).",
    },
    {
        "key":         "embed_ai_disclosure",
        "type":        "bool",
        "default":     True,
        "group":       "C2PA",
        "order":       80,
        "description": "Embed the com.vast.ai_clip_selection assertion with the vision model + prompt that selected this clip. Core regulatory disclosure — keep on.",
    },
    {
        "key":         "training_and_mining_flag",
        "type":        "string",
        "default":     "notAllowed",
        "group":       "C2PA",
        "order":       90,
        "description": "Value for c2pa.training-mining entries. Use 'notAllowed' to prohibit AI training on these clips, or 'allowed' to permit.",
    },
    {
        "key":         "sign_timeout_seconds",
        "type":        "duration_seconds",
        "default":     60.0,
        "min":         10.0,
        "max":         300.0,
        "group":       "C2PA",
        "order":       100,
        "description": "Per-rendition signing timeout.",
    },
]

register_defaults(CONFIG_SCOPE, PROVENANCE_CONFIG_SCHEMA)


class C2paError(Exception):
    """Signing or verification failed."""


@dataclass
class SignResult:
    manifest_label: str       # urn:uuid:... from c2patool
    signer:         str       # CN of the cert
    alg:            str       # es256, ...
    signed_at:      float     # epoch seconds
    signed_path:    str       # path to the output (== requested out)
    raw_report:     dict      # full c2patool output for the caller


def _c2patool_binary() -> str:
    override = os.environ.get("C2PATOOL_BINARY")
    if override and os.path.isfile(override):
        return override
    return "c2patool"


def sign_rendition(
    *,
    src_path: str,
    out_path: str,
    manifest: dict,
    clip: dict,
    rendition: dict,
    cfg_snapshot: dict,
) -> SignResult:
    """Sign one rendition MP4 in place.

    `cfg_snapshot` is the dict from `load_config("provenance").snapshot()`;
    passed in so the caller's config is the one used (testability).

    Raises C2paError on any failure — caller decides whether to proceed
    with an unsigned copy or fail the whole package.
    """
    cert_path = cfg_snapshot.get("signing_cert_path")
    key_path  = cfg_snapshot.get("signing_key_path")
    if not cert_path or not os.path.isfile(cert_path):
        raise C2paError(f"signing cert not found at {cert_path!r}")
    if not key_path or not os.path.isfile(key_path):
        raise C2paError(f"signing key not found at {key_path!r}")

    ta_url = cfg_snapshot.get("timestamp_authority_url") or None
    if ta_url == "":
        ta_url = None

    claim = build_c2pa_claim_for_rendition(
        manifest=manifest,
        clip=clip,
        rendition=rendition,
        signing_cert_path=cert_path,
        signing_key_path=key_path,
        claim_generator=cfg_snapshot.get("claim_generator") or "vast-media-catalog",
        claim_generator_version=cfg_snapshot.get("claim_generator_version") or "0.1.0",
        include_ai_disclosure=bool(cfg_snapshot.get("embed_ai_disclosure", True)),
        training_and_mining_flag=cfg_snapshot.get("training_and_mining_flag") or "notAllowed",
        ta_url=ta_url,
        alg=cfg_snapshot.get("signing_algorithm") or "es256",
    )

    # Write claim to a tempfile c2patool can read
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(claim, f)
        claim_path = f.name

    try:
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
        cmd = [
            _c2patool_binary(),
            src_path,
            "--manifest", claim_path,
            "--output",   out_path,
            "--force",
        ]
        try:
            r = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=int(cfg_snapshot.get("sign_timeout_seconds", 60.0)),
                stdin=subprocess.DEVNULL,
            )
        except subprocess.TimeoutExpired as e:
            raise C2paError(f"c2patool sign timed out: {e}") from e

        if r.returncode != 0 or not os.path.isfile(out_path):
            raise C2paError(
                f"c2patool sign failed rc={r.returncode}: "
                f"stderr={r.stderr[:500]} stdout={r.stdout[:200]}"
            )

        report = _parse_c2patool_json(r.stdout) or {}
        manifests = report.get("manifests") or {}
        if not manifests:
            # c2patool sometimes writes the signed file successfully but
            # emits a summary rather than the full read-back. Fall back
            # to a verify pass to pull the labels.
            report = verify_c2pa(out_path)
            manifests = report.get("manifests") or {}

        label = next(iter(manifests.keys()), "urn:uuid:unknown")
        signer = ""
        alg = ""
        if manifests:
            first = next(iter(manifests.values()))
            sig = first.get("signature_info") or {}
            signer = sig.get("issuer") or ""
            alg    = sig.get("alg") or ""

        return SignResult(
            manifest_label=label,
            signer=signer,
            alg=alg,
            signed_at=time.time(),
            signed_path=out_path,
            raw_report=report,
        )
    finally:
        try: os.unlink(claim_path)
        except OSError: pass


def verify_c2pa(path: str, timeout: int = 30) -> dict:
    """Run `c2patool <path>` and return its parsed JSON report.

    Raises C2paError if c2patool fails to read. Returns the empty-ish
    {"manifests": {}} if the file has no embedded manifest.
    """
    cmd = [_c2patool_binary(), path]
    try:
        r = subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=timeout, stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired as e:
        raise C2paError(f"c2patool read timed out: {e}") from e

    if r.returncode != 0:
        # No manifest present is a non-zero exit in some c2patool versions
        # — treat "No claim found" as an empty report rather than an error.
        if "No claim" in r.stderr or "No claim" in r.stdout:
            return {"manifests": {}, "active_manifest": None}
        raise C2paError(
            f"c2patool read failed rc={r.returncode}: {r.stderr[:500]}"
        )

    return _parse_c2patool_json(r.stdout) or {"manifests": {}}


def _parse_c2patool_json(s: str) -> Optional[dict]:
    """c2patool prints a JSON object to stdout on success. Parse defensively."""
    if not s or not s.strip():
        return None
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        # Find the first { ... } chunk
        start = s.find("{")
        end   = s.rfind("}")
        if start >= 0 and end > start:
            try:
                return json.loads(s[start:end + 1])
            except json.JSONDecodeError:
                return None
    return None
