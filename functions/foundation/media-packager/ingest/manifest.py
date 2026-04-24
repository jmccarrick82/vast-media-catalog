"""Build the package manifest — the shipping paperwork for a delivery.

Two flavors, both produced from the same structured dict:

  1. Sidecar `manifest.json` — human/machine-readable JSON next to the clips.
     Works for any format, survives re-encoding, but has no tamper-evidence.

  2. C2PA claim — the same data reshaped into c2patool's expected schema
     (assertions array). Signed and embedded into each rendition's MP4 by
     the provenance module. See shared/ingest/provenance.py.

Both are driven by `build_package_manifest(...)` returning the canonical
dict, then `to_sidecar_json()` / `to_c2pa_claim()` serialize it for each
target.
"""

from __future__ import annotations

import json
from typing import Any, Iterable, List, Optional

try:
    from shared.config import register_defaults
except ImportError:
    from config import register_defaults  # type: ignore


CONFIG_SCOPE = "packager"


MANIFEST_CONFIG_SCHEMA = [
    {
        "key":         "sidecar_manifest_enabled",
        "type":        "bool",
        "default":     True,
        "group":       "Manifest",
        "order":       10,
        "description": "Emit manifest.json alongside the renditions (belt+suspenders with C2PA).",
    },
    {
        "key":         "default_rights_cleared_for",
        "type":        "json",
        "default":     ["editorial", "internal-review"],
        "group":       "Licensing",
        "order":       10,
        "description": "Fallback rights_cleared_for list when the uploader doesn't set x-amz-meta-rights-cleared-for.",
    },
    {
        "key":         "default_restrictions",
        "type":        "json",
        "default":     ["no-redistribution", "no-commercial"],
        "group":       "Licensing",
        "order":       20,
        "description": "Fallback restrictions list.",
    },
    {
        "key":         "default_clearance_days",
        "type":        "int",
        "default":     90,
        "min":         1,
        "max":         3650,
        "group":       "Licensing",
        "order":       30,
        "description": "Days until a package's clearance expires, counted from packaging time.",
    },
    {
        "key":         "source_attribution_template",
        "type":        "string",
        "default":     "VAST Media Catalog / source: {filename}",
        "group":       "Licensing",
        "order":       40,
        "description": "Template for source_attribution. Supported placeholders: {filename}, {source_id}.",
    },
    {
        "key":         "default_licensing_notes",
        "type":        "string",
        "default":     "",
        "group":       "Licensing",
        "order":       50,
        "description": "Freeform notes appended to every package's manifest.licensing.notes.",
    },
]

register_defaults(CONFIG_SCOPE, MANIFEST_CONFIG_SCHEMA)


def normalize_licensing(
    source_meta: dict,
    defaults: dict,
    packaged_at_epoch: float,
) -> dict:
    """Merge uploader-supplied licensing (x-amz-meta-* on the source) with
    configured defaults. Uploader always wins — this is how content owners
    override pipeline defaults per-asset.

    Recognized metadata keys (case-insensitive, with x-amz-meta- stripped):
      rights-cleared-for     comma-separated list
      restrictions           comma-separated list
      clearance-days         integer
      licensing-notes        freeform
      source-attribution     freeform (overrides the template)
    """
    def _list(v, fallback):
        if not v:
            return fallback
        if isinstance(v, list):
            return v
        return [p.strip() for p in str(v).split(",") if p.strip()]

    def _int(v, fallback):
        try:
            return int(v)
        except (TypeError, ValueError):
            return fallback

    rights       = _list(source_meta.get("rights-cleared-for"), defaults["rights_cleared_for"])
    restrictions = _list(source_meta.get("restrictions"),       defaults["restrictions"])
    days         = _int(source_meta.get("clearance-days"),      defaults["clearance_days"])
    notes        = source_meta.get("licensing-notes") or defaults.get("notes") or ""
    attribution  = source_meta.get("source-attribution") or defaults["source_attribution"]

    return {
        "rights_cleared_for":   rights,
        "restrictions":         restrictions,
        "clearance_expires_at": packaged_at_epoch + (int(days) * 86400),
        "notes":                notes,
        "source_attribution":   attribution,
    }


def build_package_manifest(
    *,
    package_id: str,
    source_row: dict,
    clip_rows: List[dict],
    renditions_by_clip: dict,
    thumbnails_by_clip: dict,
    licensing: dict,
    created_at_iso: str,
    claim_generator: str,
    c2pa_enabled: bool,
) -> dict:
    """Assemble the canonical manifest dict. Pure function — no I/O.

    `renditions_by_clip[clip_id]` is a list of dicts:
        {name, relative_path, codec, width, height, bitrate, size_bytes}
    `thumbnails_by_clip[clip_id]` is the relative path string (or None).
    `source_row` and `clip_rows` are dicts from source_videos / extracted_clips.
    """
    return {
        "schema_version": "1.0",
        "package_id":     package_id,
        "created_at":     created_at_iso,
        "claim_generator": claim_generator,
        "c2pa_embedded":  bool(c2pa_enabled),

        "source": {
            "source_id":        source_row.get("source_id"),
            "filename":         source_row.get("filename"),
            "duration_seconds": source_row.get("duration_seconds"),
            "sha256":           source_row.get("sha256"),
            "video_codec":      source_row.get("video_codec"),
            "width":            source_row.get("width"),
            "height":           source_row.get("height"),
            "fps":              source_row.get("fps"),
            "qc_status":        source_row.get("qc_status"),
        },

        "licensing": licensing,

        "clips": [
            {
                "clip_id":          c.get("clip_id"),
                "clip_index":       c.get("clip_index"),
                "prompt":           c.get("prompt"),
                "match_confidence": c.get("match_confidence"),
                "match_reason":     c.get("match_reason"),
                "vision_model":     c.get("vision_model"),
                "start_seconds":    c.get("start_seconds"),
                "end_seconds":      c.get("end_seconds"),
                "duration_seconds": c.get("duration_seconds"),
                "shot_count":       c.get("shot_count"),
                "renditions":       renditions_by_clip.get(c.get("clip_id"), []),
                "thumbnail":        thumbnails_by_clip.get(c.get("clip_id")),
            }
            for c in clip_rows
        ],
    }


def to_sidecar_json(manifest: dict) -> str:
    """Pretty-printed JSON, sorted keys for stable diffs."""
    return json.dumps(manifest, indent=2, sort_keys=True, default=str)


# ── C2PA claim shaping ──────────────────────────────────────────────────

def build_c2pa_claim_for_rendition(
    *,
    manifest: dict,
    clip: dict,
    rendition: dict,
    signing_cert_path: str,
    signing_key_path: str,
    claim_generator: str,
    claim_generator_version: str = "0.1.0",
    include_ai_disclosure: bool = True,
    training_and_mining_flag: str = "notAllowed",
    ta_url: Optional[str] = "http://timestamp.digicert.com",
    alg: str = "es256",
) -> dict:
    """Shape a per-rendition C2PA claim that c2patool can consume directly.

    Each rendition gets its own embedded manifest, but they all tell a
    consistent story: created → placed (AI-selected) → edited (transcoded).
    """
    src          = manifest.get("source", {})
    lic          = manifest.get("licensing", {})
    prompt       = clip.get("prompt") or ""
    vision_model = clip.get("vision_model") or ""
    start        = clip.get("start_seconds") or 0.0
    end          = clip.get("end_seconds") or 0.0

    actions = [
        {
            "action": "c2pa.created",
            "parameters": {
                "description": (
                    f"Source captured via VAST Media Catalog pipeline; "
                    f"source_id={src.get('source_id')}"
                ),
            },
        },
        {
            "action": "c2pa.placed",
            "digitalSourceType":
                "http://cv.iptc.org/newscodes/digitalsourcetype/compositeWithTrainedAlgorithmicMedia",
            "parameters": {
                "description":
                    f"Clip span [{start:.2f}s, {end:.2f}s] selected by "
                    f"AI vision classifier against the prompt.",
                "start": start,
                "end":   end,
            },
            "softwareAgent": {
                "name":    "james-ai-clipper",
                "version": "0.2.0",
            },
        },
        {
            "action": "c2pa.edited",
            "parameters": {
                "description":
                    f"Transcoded to rendition {rendition.get('name')} "
                    f"({rendition.get('width')}x{rendition.get('height')})",
            },
            "softwareAgent": {
                "name":    "james-media-packager",
                "version": "0.1.0",
            },
        },
    ]

    assertions: list[dict] = [
        {
            "label": "c2pa.actions.v2",
            "data":  {"actions": actions},
        },
        {
            "label": "c2pa.creative_work",
            "data": {
                "@context": "https://schema.org",
                "@type":    "CreativeWork",
                "name":     src.get("filename"),
                "author":   [{"@type": "Organization", "name": lic.get("source_attribution")}],
            },
        },
        {
            "label": "c2pa.training-mining",
            "data": {
                "entries": {
                    "c2pa.ai_generative_training": {"use": training_and_mining_flag},
                    "c2pa.ai_training":            {"use": training_and_mining_flag},
                    "c2pa.data_mining":            {"use": training_and_mining_flag},
                    "c2pa.ai_inference":           {"use": training_and_mining_flag},
                },
            },
        },
    ]

    if include_ai_disclosure and (vision_model or prompt):
        # Freeform custom assertion for the AI disclosure details — the
        # model name and prompt are highly demo-relevant and the core
        # regulatory signal.
        assertions.append({
            "label": "com.vast.ai_clip_selection",
            "data": {
                "model":  vision_model,
                "prompt": prompt,
                "match_confidence": clip.get("match_confidence"),
                "source_span":      {"start": start, "end": end},
            },
        })

    claim: dict = {
        "alg":          alg,
        "private_key":  signing_key_path,
        "sign_cert":    signing_cert_path,
        "claim_generator_info": [
            {"name": claim_generator, "version": claim_generator_version},
        ],
        "title":      f"{src.get('filename')} — clip {clip.get('clip_index')} — "
                      f"{rendition.get('name')}",
        "assertions": assertions,
    }
    if ta_url:
        claim["ta_url"] = ta_url
    return claim
