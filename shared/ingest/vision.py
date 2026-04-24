"""Vision inference wrapper for the AI clipper.

Wraps the shared inference endpoint (OpenAI-compatible LiteLLM proxy at
`inference.selab.vastdata.com`). Exposes one narrow function:

    classify_frame(jpg_path, prompt, model_tier) -> {match, confidence, reason, model}

The prompt is what the caller wants to find ("people fighting", "a goal
is scored", "a car drives by"). The model responds with a structured
verdict that's converted to a confidence float.

Two-tier model strategy (configurable):
  * Primary:  11B vision — cheaper, faster
  * Escalate: 90B vision — for borderline confidence scores

Both are hit via the same OpenAI-style `/v1/chat/completions` endpoint
with a data-URL-encoded JPEG.
"""

from __future__ import annotations

import base64
import http.client
import json
import os
import re
import time
from typing import Any

try:
    from shared.config import register_defaults
except ImportError:
    from config import register_defaults  # type: ignore


CONFIG_SCOPE = "ai-clipper"

# Inference endpoint — hardcoded by design. Same host used across the
# project; keys rotate via env/secrets management, not code.
INFERENCE_HOST = "inference.selab.vastdata.com"


VISION_CONFIG_SCHEMA = [
    {
        "key": "vision_model_primary",
        "type": "string",
        "default": "nvidia/llama-3.2-11b-vision-instruct",
        "group": "Vision inference",
        "order": 10,
        "description": "Primary (cheaper/faster) vision model used for every frame.",
    },
    {
        "key": "vision_model_escalation",
        "type": "string",
        "default": "nvidia/llama-3.2-90b-vision-instruct",
        "group": "Vision inference",
        "order": 20,
        "description": (
            "Second-pass model used when primary confidence is in the "
            "borderline band. Set to the same as primary to disable."
        ),
    },
    {
        "key": "escalation_confidence_low",
        "type": "float",
        "default": 0.35,
        "min": 0.0,
        "max": 1.0,
        "group": "Vision inference",
        "order": 30,
        "description": (
            "Lower bound of the borderline band. Primary scores below "
            "this are treated as definite non-matches (no escalation)."
        ),
    },
    {
        "key": "escalation_confidence_high",
        "type": "float",
        "default": 0.75,
        "min": 0.0,
        "max": 1.0,
        "group": "Vision inference",
        "order": 40,
        "description": (
            "Upper bound of the borderline band. Primary scores above "
            "this are treated as definite matches (no escalation)."
        ),
    },
    {
        "key": "inference_timeout_seconds",
        "type": "duration_seconds",
        "default": 60.0,
        "min": 10.0,
        "max": 300.0,
        "group": "Vision inference",
        "order": 50,
        "description": "Per-call timeout against the inference endpoint.",
    },
    {
        "key": "inference_retries",
        "type": "int",
        "default": 2,
        "min": 0,
        "max": 5,
        "group": "Vision inference",
        "order": 60,
        "description": "How many times to retry a failed inference call before giving up.",
    },
    {
        "key": "default_clip_prompt",
        "type": "string",
        "default": "A visually interesting moment worth showing a viewer.",
        "group": "Vision inference",
        "order": 70,
        "description": (
            "Fallback prompt when the upload has no x-amz-meta-clip-prompt "
            "tag and no sidecar JSON."
        ),
    },
    {
        "key": "inference_step_delay_seconds",
        "type": "duration_seconds",
        "default": 0.5,
        "min": 0.0,
        "max": 10.0,
        "group": "Vision inference",
        "order": 80,
        "description": (
            "Delay between sequential calls to the inference endpoint. "
            "Keeps the shared endpoint from being hammered when the "
            "function is dispatched in bursts."
        ),
    },
]

register_defaults(CONFIG_SCOPE, VISION_CONFIG_SCHEMA)


class VisionError(Exception):
    """Vision call failed after retries."""


def _image_b64(path: str) -> str:
    """Read a JPEG and return a data URL suitable for image_url content."""
    with open(path, "rb") as f:
        return f"data:image/jpeg;base64,{base64.b64encode(f.read()).decode()}"


def _inference_chat(
    model: str,
    messages: list,
    api_key: str,
    max_tokens: int = 300,
    temperature: float = 0.1,
    timeout: int = 60,
) -> str:
    """Single POST to /v1/chat/completions. No retries — caller does that."""
    body = json.dumps({
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }).encode()

    conn = http.client.HTTPSConnection(INFERENCE_HOST, timeout=timeout)
    try:
        conn.request(
            "POST",
            "/v1/chat/completions",
            body=body,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
        resp = conn.getresponse()
        raw = resp.read().decode()
        if resp.status != 200:
            raise VisionError(f"Inference {resp.status}: {raw[:500]}")
        return json.loads(raw)["choices"][0]["message"]["content"]
    finally:
        conn.close()


# Matches tight JSON the model usually produces. Falls back to loose
# {match: yes/no, confidence: 0.7} parsing if the model rambles.
_JSON_FENCE_RE = re.compile(r"\{[^{}]*\}", re.DOTALL)


def _parse_verdict(raw: str) -> dict:
    """Extract {match: bool, confidence: float, reason: str} from model text."""
    # Preferred: a single JSON object in the response
    m = _JSON_FENCE_RE.search(raw or "")
    if m:
        try:
            obj = json.loads(m.group(0))
            match_val = obj.get("match")
            if isinstance(match_val, str):
                match_bool = match_val.strip().lower() in ("yes", "true", "1", "match")
            else:
                match_bool = bool(match_val)
            conf = float(obj.get("confidence") or 0.0)
            conf = max(0.0, min(1.0, conf))
            reason = str(obj.get("reason") or "").strip()[:500]
            return {"match": match_bool, "confidence": conf, "reason": reason}
        except (ValueError, TypeError):
            pass

    # Fallback: heuristic parse
    text = (raw or "").lower()
    match_bool = (
        "yes" in text[:30]
        or '"match": true' in text
        or text.startswith("match")
    )
    conf_match = re.search(r"confidence[:\s]*([01](?:\.[0-9]+)?)", text)
    conf = float(conf_match.group(1)) if conf_match else (0.7 if match_bool else 0.2)
    return {
        "match": match_bool,
        "confidence": max(0.0, min(1.0, conf)),
        "reason": (raw or "")[:500].strip(),
    }


def classify_frame(
    jpg_path: str,
    prompt: str,
    api_key: str,
    model: str,
    timeout: int = 60,
    retries: int = 2,
) -> dict:
    """Ask a vision model whether `jpg_path` matches `prompt`.

    Returns {match: bool, confidence: float 0-1, reason: str, model: str}.
    Raises VisionError after all retries are exhausted.
    """
    img = _image_b64(jpg_path)
    system = (
        "You are a precise video content classifier. For each image you "
        "will be given a query describing what to look for. Answer ONLY "
        'with a JSON object: {"match": true|false, "confidence": 0.0-1.0, '
        '"reason": "short explanation"}. Do not include any other text.'
    )
    user_content = [
        {"type": "image_url", "image_url": {"url": img}},
        {
            "type": "text",
            "text": (
                f"Query: {prompt}\n\n"
                "Does this image match the query? Respond with JSON only."
            ),
        },
    ]
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": user_content},
    ]

    last_err: Exception | None = None
    for attempt in range(retries + 1):
        try:
            raw = _inference_chat(
                model=model,
                messages=messages,
                api_key=api_key,
                max_tokens=200,
                temperature=0.1,
                timeout=timeout,
            )
            verdict = _parse_verdict(raw)
            verdict["model"] = model
            return verdict
        except Exception as e:  # noqa: BLE001 — retry on anything
            last_err = e
            if attempt < retries:
                time.sleep(2 ** attempt)  # 1s, 2s, 4s
            else:
                raise VisionError(
                    f"classify_frame({model}) failed after {retries+1} attempts: {e}"
                ) from e
    # Unreachable but satisfies type checker
    raise VisionError(str(last_err))


def classify_with_escalation(
    jpg_path: str,
    prompt: str,
    api_key: str,
    primary_model: str,
    escalation_model: str,
    low: float,
    high: float,
    timeout: int = 60,
    retries: int = 2,
) -> dict:
    """Run the two-tier strategy.

    * Primary → if confidence < low OR >= high, return primary result.
    * Borderline (low <= confidence < high) → re-run with escalation model
      and return the escalated verdict (annotated with `escalated=True`).
    """
    primary = classify_frame(
        jpg_path,
        prompt,
        api_key=api_key,
        model=primary_model,
        timeout=timeout,
        retries=retries,
    )
    primary["escalated"] = False

    if primary_model == escalation_model:
        return primary

    conf = primary.get("confidence", 0.0)
    if low <= conf < high:
        try:
            escalated = classify_frame(
                jpg_path,
                prompt,
                api_key=api_key,
                model=escalation_model,
                timeout=timeout,
                retries=retries,
            )
            escalated["escalated"] = True
            escalated["primary_confidence"] = conf
            return escalated
        except VisionError:
            # If escalation fails, fall back to the primary verdict — we
            # already have something usable.
            return primary

    return primary
