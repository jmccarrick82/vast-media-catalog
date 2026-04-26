"""Audio-aware excitement features for the AI clipper (Phase 2.6).

Vision-only clip selection on a sports broadcast picks five visually
similar "swing/at-bat" frames that don't capture excitement — every
pitch looks like a swing. Real excitement is partly *audible*: crowd
roar, an announcer shouting, sudden volume spike on contact.

This module wraps two cheap ffmpeg passes:

  * `ebur128`  — EBU R128 short-term LUFS (3-second window) per sample
  * `astats`   — RMS and peak dB across the whole window

…and an optional opt-in Whisper transcription per candidate.

The downstream curator subtracts a per-source baseline (median short-term
LUFS over the entire source) from each candidate's p95 short-term LUFS
to get an "excitement above baseline" delta in dB. Positive means louder
than typical broadcast audio (likely crowd roar / announcer shouting);
near-zero means the same level as everywhere else in the source.

Pure-function — no S3, no DB. Mirrors `scene.py` / `vision.py` layout.
"""

from __future__ import annotations

import os
import re
import statistics
import subprocess
from dataclasses import dataclass
from typing import List, Optional

try:
    from shared.config import register_defaults
except ImportError:  # bundled flat layout inside a function image
    from config import register_defaults  # type: ignore


CONFIG_SCOPE = "ai-clipper"

AUDIO_CONFIG_SCHEMA = [
    {
        "key":         "audio_analysis_enabled",
        "type":        "bool",
        "default":     True,
        "group":       "Audio cues",
        "order":       10,
        "description": (
            "Run an EBU R128 short-term LUFS pass over each clip candidate "
            "and surface the result to the curator. Adds ~5s ffmpeg per "
            "candidate but is what makes 'crowd roar' beat 'silence'."
        ),
    },
    {
        "key":         "audio_use_whisper",
        "type":        "bool",
        "default":     False,
        "group":       "Audio cues",
        "order":       20,
        "description": (
            "Also send each candidate's audio to the Whisper endpoint for "
            "an excerpt of speech. Adds 5–10s per candidate; off by default. "
            "When on, the announcer's words show up in the curation prompt — "
            "'and that one is OUTTA HERE!' is a giant excitement signal."
        ),
    },
    {
        "key":         "audio_baseline_lufs_window_seconds",
        "type":        "duration_seconds",
        "default":     30.0,
        "min":         5.0,
        "max":         600.0,
        "group":       "Audio cues",
        "order":       30,
        "description": (
            "Minimum source duration we'll trust the baseline LUFS over. "
            "The baseline is the median short-term loudness across the "
            "whole source — for very short sources this is noisy."
        ),
    },
    {
        "key":         "audio_excitement_min_db",
        "type":        "float",
        "default":     6.0,
        "min":         0.0,
        "max":         30.0,
        "group":       "Audio cues",
        "order":       40,
        "description": (
            "Curation threshold: a candidate whose p95 short-term LUFS is "
            "this many dB above the source baseline is highlighted to the "
            "LLM as 'audio excitement.' Lower = more candidates flagged; "
            "higher = only obvious crowd-roar moments."
        ),
    },
]

register_defaults(CONFIG_SCOPE, AUDIO_CONFIG_SCHEMA)


# ── Public types ─────────────────────────────────────────────────────

@dataclass
class AudioFeatures:
    """Per-candidate audio summary. All values may be None when the
    candidate has no audio stream or ffmpeg failed to parse output."""
    peak_lufs:           Optional[float]   # max short-term LUFS within span
    rms_db:              Optional[float]   # astats RMS_level dB across span
    short_term_lufs_p95: Optional[float]   # 95th-percentile short-term LUFS
    speech_rate_wpm:     Optional[float]   # words/min from Whisper transcript
    transcript:          Optional[str]     # Whisper text (when use_whisper=True)
    duration:            float


# ── ffmpeg helpers ───────────────────────────────────────────────────

def _ffmpeg_binary() -> str:
    override = os.environ.get("FFMPEG_BINARY")
    if override and os.path.isfile(override):
        return override
    return "ffmpeg"


class AudioAnalyzeError(Exception):
    """ffmpeg audio analysis failed."""


def _run_ffmpeg_filter(
    args: List[str],
    timeout: int = 300,
) -> str:
    """Run `ffmpeg <args> -f null -` and return stderr (where filter
    diagnostics land). Hardened against the same hang-modes as qc.py:
    -nostdin + closed stdin so a Knative pod's pipe-stdin can't stall it.
    """
    cmd = [
        _ffmpeg_binary(),
        "-nostdin", "-nostats", "-hide_banner",
        *args,
        "-f", "null", "-",
    ]
    try:
        r = subprocess.run(
            cmd,
            capture_output=True, text=True,
            stdin=subprocess.DEVNULL,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as e:
        raw = e.stderr
        if isinstance(raw, bytes):
            return raw.decode("utf-8", errors="replace")
        return raw or ""
    except FileNotFoundError as e:
        raise AudioAnalyzeError(f"ffmpeg binary not found: {e}") from e
    return r.stderr or ""


# ebur128 framelog=verbose lines look like:
#   [Parsed_ebur128_0 @ 0x...] t: 0.4    M: -16.5  S: -23.8  I: -19.2 LUFS  LRA:  0.0 LU
# We capture the S: short-term value (3-second sliding window).
_STM_RE = re.compile(
    r"\bS:\s*(-?\d+(?:\.\d+)?)\s",
    re.MULTILINE,
)

# astats output lines are like:
#   [Parsed_astats_0 @ 0x...] RMS level dB: -18.50
#   [Parsed_astats_0 @ 0x...] Peak level dB: -3.00
_ASTATS_RMS_RE = re.compile(
    r"RMS level dB:\s*(-?\d+(?:\.\d+)?|-inf)",
    re.IGNORECASE,
)


def _short_term_lufs_samples(
    src: str,
    start: Optional[float] = None,
    end: Optional[float] = None,
    timeout: int = 300,
) -> List[float]:
    """Parse ebur128 stderr into a list of short-term LUFS samples.
    Drops -inf / extremely-low values that indicate silence."""
    args: List[str] = []
    if start is not None and start >= 0:
        args += ["-ss", f"{start:.3f}"]
    if end is not None and end > 0:
        args += ["-to", f"{end:.3f}"]
    args += [
        "-i", src,
        "-vn",
        "-af", "ebur128=peak=true:framelog=verbose",
    ]
    out = _run_ffmpeg_filter(args, timeout=timeout)
    samples: List[float] = []
    for m in _STM_RE.finditer(out):
        try:
            v = float(m.group(1))
        except (TypeError, ValueError):
            continue
        # ebur128 emits -120 / very low values for silence; filter the obvious
        # garbage so percentiles aren't dragged into the floor.
        if v > -100.0:
            samples.append(v)
    return samples


def _astats_rms_db(
    src: str,
    start: Optional[float] = None,
    end: Optional[float] = None,
    timeout: int = 120,
) -> Optional[float]:
    """Single-pass overall RMS dB across the requested span."""
    args: List[str] = []
    if start is not None and start >= 0:
        args += ["-ss", f"{start:.3f}"]
    if end is not None and end > 0:
        args += ["-to", f"{end:.3f}"]
    args += [
        "-i", src,
        "-vn",
        "-af", "astats=metadata=0:length=0",
    ]
    out = _run_ffmpeg_filter(args, timeout=timeout)
    m = _ASTATS_RMS_RE.search(out)
    if not m:
        return None
    val = m.group(1)
    if val.lower() == "-inf":
        return None
    try:
        return float(val)
    except ValueError:
        return None


def _percentile(sorted_vals: List[float], p: float) -> float:
    """Nearest-rank percentile. `sorted_vals` must be pre-sorted ascending.
    `p` is 0..1.
    """
    if not sorted_vals:
        return float("-inf")
    p = max(0.0, min(1.0, p))
    idx = int(round(p * (len(sorted_vals) - 1)))
    return sorted_vals[idx]


# ── Public API ───────────────────────────────────────────────────────

def compute_baseline_lufs(
    src: str,
    window: float = 30.0,
    timeout: int = 300,
) -> Optional[float]:
    """Return the median short-term LUFS across the *whole source* —
    i.e. "typical" loudness against which candidates can be compared.

    `window` is treated as a hint for minimum reliable sample count: very
    short sources (< window) may produce noisy baselines but we still
    return whatever the data supports rather than refusing.

    Returns None when the source has no usable audio.
    """
    samples = _short_term_lufs_samples(src, start=None, end=None, timeout=timeout)
    if not samples:
        return None
    # Median is more robust than mean against momentary loudness spikes
    # (which is exactly what we're trying to *find* in candidates — the
    # baseline shouldn't move just because there's one cheering moment).
    try:
        return float(statistics.median(samples))
    except statistics.StatisticsError:
        return None


def extract_audio_features(
    src: str,
    start: Optional[float] = None,
    end: Optional[float] = None,
    use_whisper: bool = False,
    api_key: Optional[str] = None,
    inference_host: Optional[str] = None,
    timeout: int = 120,
) -> AudioFeatures:
    """Compute audio features for a candidate clip span [start, end].

    Returns AudioFeatures even on partial failure — fields default to
    None when the underlying ffmpeg call didn't produce parseable output
    (e.g. no audio stream, or astats missing). The caller should treat
    None as "no signal" rather than zero.
    """
    duration = 0.0
    if start is not None and end is not None and end > start:
        duration = float(end - start)

    # Short-term LUFS samples within the span (3-second sliding window
    # from ebur128). Even a 5s candidate yields ~3-5 samples — enough
    # for a meaningful peak/p95.
    samples = _short_term_lufs_samples(src, start=start, end=end, timeout=timeout)
    if samples:
        samples_sorted = sorted(samples)
        peak_lufs = samples_sorted[-1]
        p95_lufs  = _percentile(samples_sorted, 0.95)
    else:
        peak_lufs = None
        p95_lufs  = None

    rms_db = _astats_rms_db(src, start=start, end=end, timeout=timeout)

    transcript: Optional[str] = None
    speech_rate_wpm: Optional[float] = None
    if use_whisper and api_key and duration > 0.5:
        # Lazy import — keeps audio.py importable without the Whisper
        # helper present (e.g. in unit tests) and avoids paying the
        # http.client import cost when whisper is disabled.
        try:
            from shared.ingest.whisper import transcribe_segment
        except ImportError:
            try:
                from .whisper import transcribe_segment  # type: ignore
            except ImportError:
                from whisper import transcribe_segment  # type: ignore
        try:
            transcript = transcribe_segment(
                src,
                start=start or 0.0,
                end=end or 0.0,
                api_key=api_key,
                inference_host=inference_host,
                timeout=timeout,
            )
            if transcript and duration > 0:
                words = len(transcript.split())
                speech_rate_wpm = (words / duration) * 60.0
        except Exception:
            # Don't let a Whisper failure kill the whole audio analysis;
            # the loudness signal alone is still useful.
            transcript = None
            speech_rate_wpm = None

    return AudioFeatures(
        peak_lufs=peak_lufs,
        rms_db=rms_db,
        short_term_lufs_p95=p95_lufs,
        speech_rate_wpm=speech_rate_wpm,
        transcript=transcript,
        duration=duration,
    )
