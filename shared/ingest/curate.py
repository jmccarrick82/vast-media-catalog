"""LLM-driven clip curation for the AI clipper.

When the per-shot vision pass produces more matching candidates than the
configured target count, we ask a text-only LLM to choose the best N.
Per-shot vision confidence is noisy (a single frame at the shot midpoint),
so when 17 shots all score 0.7+ on "a bunny on screen", picking by
confidence alone gives you 17 nearly-identical bunny shots.

The curation prompt feeds the model:
  * the original user prompt the clips matched
  * each candidate's per-shot reason (the vision model's own description)
  * the candidate's source-time span and confidence

…and asks it to pick the best N considering match quality, content
diversity, and (lightly) chronological spread. The result is parsed back
into the original candidate objects, returned in source order.

If the LLM call fails or its response can't be parsed, the caller gets
the top-K-by-confidence fallback so the pipeline never stalls on
curation problems.
"""

from __future__ import annotations

import http.client
import json
import re
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

try:
    from shared.config import register_defaults
except ImportError:
    from config import register_defaults  # type: ignore

try:
    from shared.ingest.clips import ClipSpan, top_k_by_confidence
except ImportError:
    from .clips import ClipSpan, top_k_by_confidence  # type: ignore


CONFIG_SCOPE = "ai-clipper"

INFERENCE_HOST_DEFAULT = "inference.selab.vastdata.com"


CURATE_CONFIG_SCHEMA = [
    {
        "key":         "curation_enabled",
        "type":        "bool",
        "default":     True,
        "group":       "Curation",
        "order":       10,
        "description": (
            "When more candidates match than max_clips_per_source, ask an "
            "LLM to rank them and keep the best N. Off = just confidence-rank."
        ),
    },
    {
        "key":         "curation_model",
        "type":        "string",
        "default":     "nvidia/llama-3.3-70b-instruct",
        "group":       "Curation",
        "order":       20,
        "description": (
            "Text-only model used for curation. Inputs are per-clip "
            "descriptions and timestamps; no images. Llama-3.3-70B is a "
            "good default — fast enough, smart enough to dedupe + rank."
        ),
    },
    {
        "key":         "curation_timeout_seconds",
        "type":        "duration_seconds",
        "default":     90.0,
        "min":         10.0,
        "max":         300.0,
        "group":       "Curation",
        "order":       30,
        "description": "Per-call timeout for the curation model.",
    },
    {
        "key":         "curation_retries",
        "type":        "int",
        "default":     2,
        "min":         0,
        "max":         5,
        "group":       "Curation",
        "order":       40,
        "description": (
            "Retries on the curation call before falling back to "
            "confidence-based top-K."
        ),
    },
    {
        "key":         "curation_diversity_weight",
        "type":        "string",
        "default":     "high",
        "group":       "Curation",
        "order":       50,
        "description": (
            "How strongly the curator should reward diverse content over "
            "near-duplicates. Values: 'low' / 'medium' / 'high'. With "
            "'high' the curator avoids picking 5 nearly-identical shots; "
            "with 'low' it just picks the strongest matches even if "
            "they're variations of the same scene."
        ),
    },
]

register_defaults(CONFIG_SCOPE, CURATE_CONFIG_SCHEMA)


@dataclass
class CurationResult:
    selected: List[ClipSpan]
    source: str          # "ai-curated" / "fallback-confidence" / "no-curation-needed"
    error: Optional[str] # set when fallback was triggered
    raw_response: Optional[str] = None


def curate_clips(
    candidates: List[ClipSpan],
    *,
    prompt: str,
    target_count: int,
    api_key: str,
    model: str,
    inference_host: str = INFERENCE_HOST_DEFAULT,
    timeout: int = 90,
    retries: int = 2,
    diversity_weight: str = "high",
) -> CurationResult:
    """Pick the best `target_count` clips out of `candidates`.

    No-op when len(candidates) <= target_count. Otherwise calls the LLM,
    parses ranked indices, returns the chosen ClipSpans in source order.
    Falls back to top-K-by-confidence on any failure.
    """
    if len(candidates) <= target_count:
        return CurationResult(
            selected=list(candidates),
            source="no-curation-needed",
            error=None,
        )

    if not api_key:
        return CurationResult(
            selected=top_k_by_confidence(candidates, target_count),
            source="fallback-confidence",
            error="no api_key for curation",
        )

    user_msg = _build_curation_prompt(
        prompt=prompt,
        candidates=candidates,
        target=target_count,
        diversity_weight=diversity_weight,
    )

    last_err = None
    for attempt in range(retries + 1):
        try:
            response = _chat_completion(
                model=model,
                user_msg=user_msg,
                api_key=api_key,
                inference_host=inference_host,
                timeout=timeout,
            )
            indices = _parse_ranked_indices(response, len(candidates))
            if not indices:
                raise ValueError(f"no parseable indices in response: {response[:200]!r}")
            kept_set = set(indices[:target_count])
            selected = [c for i, c in enumerate(candidates, start=1) if i in kept_set]
            if not selected:
                raise ValueError("indices parsed but no candidates matched")
            return CurationResult(
                selected=selected,
                source="ai-curated",
                error=None,
                raw_response=response,
            )
        except Exception as e:  # noqa: BLE001 — fallback on any failure
            last_err = str(e)
            if attempt < retries:
                time.sleep(2 ** attempt)

    return CurationResult(
        selected=top_k_by_confidence(candidates, target_count),
        source="fallback-confidence",
        error=last_err,
    )


# ── Prompt construction ─────────────────────────────────────────────

_DIVERSITY_BLURB = {
    "low":    "Prioritize the strongest matches. It's fine if multiple picks show similar content.",
    "medium": "Balance match strength with some variety. Two near-duplicate clips earn a penalty unless both are notably stronger than the rest.",
    "high":   "Strongly prefer diverse content. Avoid picking near-duplicate shots — if two candidates show essentially the same thing, only keep the better one and use the freed slot for something different.",
}


def _build_curation_prompt(
    prompt: str,
    candidates: List[ClipSpan],
    target: int,
    diversity_weight: str,
) -> str:
    diversity_text = _DIVERSITY_BLURB.get(diversity_weight, _DIVERSITY_BLURB["high"])
    lines: List[str] = []
    for i, c in enumerate(candidates, start=1):
        reason = (c.reason or "").strip().replace("\n", " ")
        if len(reason) > 220:
            reason = reason[:217] + "…"
        lines.append(
            f"{i}. [{c.start:.1f}s–{c.end:.1f}s, conf {c.confidence:.2f}] {reason}"
        )

    body = (
        "You are curating a highlight reel from candidate video clips. "
        "All candidates were flagged by a vision model as matching the same query. "
        f"You must pick exactly {target} of them.\n\n"
        f"Query: \"{prompt}\"\n\n"
        f"Candidates ({len(candidates)} total):\n"
        + "\n".join(lines)
        + "\n\nSelection guidance:\n"
        f"- {diversity_text}\n"
        "- Higher per-shot confidence is a hint but not the only factor — a high-confidence "
        "near-duplicate of an earlier pick is worse than a moderate-confidence unique scene.\n"
        "- Prefer good chronological spread across the source so the highlight reel feels "
        "like it covers the whole video rather than clustering at the start.\n"
        "- If a candidate's description is generic or thin, discount it.\n\n"
        f"Return ONLY a JSON array of the {target} candidate numbers (1-based) you'd keep, "
        f"ordered by quality (best first). Example: [3, 7, 1, 12, 5]\n"
        "Do not return any prose — just the JSON array."
    )
    return body


# ── Inference call (text-only) ──────────────────────────────────────

def _chat_completion(
    model: str,
    user_msg: str,
    api_key: str,
    inference_host: str,
    timeout: int,
) -> str:
    body = json.dumps({
        "model": model,
        "messages": [
            {"role": "system", "content":
             "You are a careful editor that returns concise structured output. "
             "When asked for a JSON array, return ONLY the JSON — no prose, no code fences."},
            {"role": "user", "content": user_msg},
        ],
        "max_tokens": 200,
        "temperature": 0.2,
    }).encode()
    conn = http.client.HTTPSConnection(inference_host, timeout=timeout)
    try:
        conn.request(
            "POST", "/v1/chat/completions",
            body=body,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
        resp = conn.getresponse()
        raw = resp.read().decode()
        if resp.status != 200:
            raise RuntimeError(f"inference {resp.status}: {raw[:300]}")
        return json.loads(raw)["choices"][0]["message"]["content"]
    finally:
        conn.close()


_INDEX_LIST_RE = re.compile(r"\[\s*(?:-?\d+\s*,\s*)*-?\d+\s*\]")


def _parse_ranked_indices(response: str, max_index: int) -> List[int]:
    """Pull the first JSON array of integers out of the model response."""
    if not response:
        return []
    s = response.strip()
    # Strip code fences if present
    if s.startswith("```"):
        s = s.strip("`")
        nl = s.find("\n")
        if nl >= 0:
            s = s[nl + 1:].strip()
        if s.endswith("```"):
            s = s[:-3].strip()
    m = _INDEX_LIST_RE.search(s)
    if not m:
        return []
    try:
        arr = json.loads(m.group(0))
    except json.JSONDecodeError:
        return []
    out: List[int] = []
    seen = set()
    for v in arr:
        try:
            i = int(v)
        except (TypeError, ValueError):
            continue
        if 1 <= i <= max_index and i not in seen:
            out.append(i)
            seen.add(i)
    return out
