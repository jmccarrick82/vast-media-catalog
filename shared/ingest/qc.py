"""Non-AI QC primitives: silence / black / freeze / loudness detection.

These are the leaf operations the QC inspector composes. Each function is
a pure wrapper around an ffmpeg or ffprobe call that parses the output
into a structured, JSON-serializable result.

Thresholds are NOT hardcoded here — they come from the caller (typically
loaded from `function_configs` via `shared.config.load_config("qc-inspector")`).
This module only *declares* the knob schema so the seed script and the
/settings UI know what to show.

Tier 1 primitives (must-have, implemented here):
    - probe_metadata(path)
    - detect_silence(path, min_run, threshold_db)
    - detect_black_frames(path, min_run, pixel_threshold)
    - detect_freeze_frames(path, min_run, noise_threshold)
    - measure_loudness(path)     [optional, gated by config]

Tier 2 primitives (planned, add later):
    - detect_vfr(path)
    - detect_interlaced(path)
"""

# Import the config register_defaults helper from wherever it lives:
# project context → shared.config; function bundle → flat `config` module.
try:
    from shared.config import register_defaults
except ImportError:
    from config import register_defaults  # type: ignore

CONFIG_SCOPE = "qc-inspector"

CONFIG_SCHEMA = [
    # ── Structure / codec checks ────────────────────────────────
    {"key": "min_duration_seconds",
     "type": "duration_seconds", "default": 5.0,
     "min": 0.0, "max": 86400.0,
     "group": "Structure", "order": 10,
     "description": "Fail QC if source is shorter than this."},

    {"key": "max_duration_seconds",
     "type": "duration_seconds", "default": 21600.0,
     "min": 0.0, "max": 86400.0,
     "group": "Structure", "order": 20,
     "description": "Fail QC if source is longer than this (default 6 hours)."},

    {"key": "require_video_stream",
     "type": "bool", "default": True,
     "group": "Structure", "order": 30,
     "description": "Fail QC if the file has no video stream."},

    {"key": "require_audio_stream",
     "type": "bool", "default": False,
     "group": "Structure", "order": 40,
     "description": "Fail (instead of just warn) if no audio stream is present."},

    {"key": "video_codec_allowlist",
     "type": "json", "default": ["h264", "hevc", "vp9", "av1"],
     "group": "Structure", "order": 50,
     "description": "Accepted video codecs. Anything else fails QC."},

    {"key": "audio_codec_allowlist",
     "type": "json", "default": ["aac", "ac3", "opus", "mp3", "flac", "pcm_s16le"],
     "group": "Structure", "order": 60,
     "description": "Accepted audio codecs."},

    # ── Resolution ──────────────────────────────────────────────
    {"key": "min_video_width",
     "type": "int", "default": 640,
     "min": 0, "max": 8192,
     "group": "Resolution", "order": 10,
     "description": "Minimum accepted video width (pixels)."},

    {"key": "min_video_height",
     "type": "int", "default": 360,
     "min": 0, "max": 8192,
     "group": "Resolution", "order": 20,
     "description": "Minimum accepted video height (pixels)."},

    {"key": "warn_below_width",
     "type": "int", "default": 854,
     "min": 0, "max": 8192,
     "group": "Resolution", "order": 30,
     "description": "Warn if width is below this (SD threshold)."},

    {"key": "warn_below_height",
     "type": "int", "default": 480,
     "min": 0, "max": 8192,
     "group": "Resolution", "order": 40,
     "description": "Warn if height is below this."},

    # ── Black frames ────────────────────────────────────────────
    {"key": "black_frame_min_run_seconds",
     "type": "duration_seconds", "default": 1.0,
     "min": 0.1, "max": 60.0,
     "group": "Black frames", "order": 10,
     "description": "Minimum continuous run to count as a black-frame event."},

    {"key": "black_frame_pixel_threshold",
     "type": "float", "default": 0.98,
     "min": 0.5, "max": 1.0,
     "group": "Black frames", "order": 20,
     "description": "Pixel-darkness threshold (0-1). Higher = stricter (fewer matches)."},

    {"key": "black_frame_max_ratio_warn",
     "type": "percent", "default": 0.10,
     "min": 0.0, "max": 1.0,
     "group": "Black frames", "order": 30,
     "description": "Warn if black-frame runs exceed this fraction of total duration."},

    {"key": "black_frame_max_ratio_fail",
     "type": "percent", "default": 0.50,
     "min": 0.0, "max": 1.0,
     "group": "Black frames", "order": 40,
     "description": "Fail (quarantine) if black-frame runs exceed this fraction."},

    # ── Freeze frames ───────────────────────────────────────────
    {"key": "freeze_min_run_seconds",
     "type": "duration_seconds", "default": 1.0,
     "min": 0.1, "max": 60.0,
     "group": "Freeze frames", "order": 10,
     "description": "Minimum continuous run to count as a freeze event."},

    {"key": "freeze_noise_threshold",
     "type": "float", "default": 0.003,
     "min": 0.0, "max": 1.0,
     "group": "Freeze frames", "order": 20,
     "description": "Frame-to-frame motion threshold (lower = stricter)."},

    {"key": "freeze_max_ratio_warn",
     "type": "percent", "default": 0.10,
     "min": 0.0, "max": 1.0,
     "group": "Freeze frames", "order": 30,
     "description": "Warn if freeze runs exceed this fraction of duration."},

    {"key": "freeze_max_ratio_fail",
     "type": "percent", "default": 0.50,
     "min": 0.0, "max": 1.0,
     "group": "Freeze frames", "order": 40,
     "description": "Fail if freeze runs exceed this fraction."},

    # ── Silence ─────────────────────────────────────────────────
    {"key": "silence_min_run_seconds",
     "type": "duration_seconds", "default": 1.0,
     "min": 0.1, "max": 60.0,
     "group": "Silence", "order": 10,
     "description": "Minimum continuous silent run to count as an event."},

    {"key": "silence_threshold_db",
     "type": "db", "default": -50.0,
     "min": -90.0, "max": 0.0,
     "group": "Silence", "order": 20,
     "description": "Audio below this dB level is considered silent."},

    {"key": "silence_max_ratio_warn",
     "type": "percent", "default": 0.25,
     "min": 0.0, "max": 1.0,
     "group": "Silence", "order": 30,
     "description": "Warn if silence covers more than this fraction."},

    {"key": "silence_max_ratio_fail",
     "type": "percent", "default": 0.95,
     "min": 0.0, "max": 1.0,
     "group": "Silence", "order": 40,
     "description": "Fail if silence covers more than this fraction (effectively no audio)."},

    # ── Loudness (optional, Tier 2) ─────────────────────────────
    {"key": "loudness_enabled",
     "type": "bool", "default": True,
     "group": "Loudness", "order": 10,
     "description": "Run EBU R128 loudness measurement. Adds ~2x processing time."},

    {"key": "loudness_min_lufs",
     "type": "float", "default": -30.0,
     "min": -60.0, "max": 0.0,
     "group": "Loudness", "order": 20,
     "description": "Warn if integrated loudness is below this (too quiet)."},

    {"key": "loudness_max_true_peak_dbtp",
     "type": "db", "default": -1.0,
     "min": -30.0, "max": 6.0,
     "group": "Loudness", "order": 30,
     "description": "Warn if true peak exceeds this (clipping risk)."},

    # ── Stream quirks (Tier 2) ──────────────────────────────────
    {"key": "vfr_detection_enabled",
     "type": "bool", "default": True,
     "group": "Stream quirks", "order": 10,
     "description": "Flag variable-frame-rate content."},

    {"key": "interlaced_detection_enabled",
     "type": "bool", "default": True,
     "group": "Stream quirks", "order": 20,
     "description": "Flag interlaced content (may need deinterlacing)."},
]

register_defaults(CONFIG_SCOPE, CONFIG_SCHEMA)


# ── Implementations ──────────────────────────────────────────────────

import os
import re
import subprocess
from typing import List, Optional

# Re-export from ffprobe so callers only need one import. Relative import
# so this works both as shared.ingest.qc (in the project) and as
# ingest.qc (when copied into a DataEngine function bundle).
from .ffprobe import probe_metadata, FFProbeError  # noqa: F401


def _ffmpeg_binary() -> str:
    override = os.environ.get("FFMPEG_BINARY")
    if override and os.path.isfile(override):
        return override
    return "ffmpeg"


def _run_ffmpeg_scan(args: List[str], timeout: int = 120) -> str:
    """Run ffmpeg with an input file + filter, discard output, return stderr
    (where all the filter diagnostics land).

    Guards against two common hang modes:

    1. **stdin reads** — ffmpeg without ``-nostdin`` polls stdin for interactive
       commands (q/?/etc.). In a Knative pod stdin is often a pipe that never
       closes, so ffmpeg can hang forever. ``-nostdin`` + ``stdin=DEVNULL``
       makes sure that never happens.
    2. **Runaway filters** — a pathological input can make a filter spin. The
       per-call timeout is bounded (default 120s; callers may pass higher
       for long sources) so one bad detector can't eat the whole handler.
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
        # Return the partial stderr (if any) so callers can still parse what
        # DID get emitted before the timeout. With text=True, e.stderr is
        # already a str; with text=False it'd be bytes. Handle both.
        raw = e.stderr
        if isinstance(raw, bytes):
            return raw.decode("utf-8", errors="replace")
        return raw or ""
    # ffmpeg often exits non-zero for weird reasons even when filters ran; we
    # rely on the stderr content, not the return code.
    return r.stderr or ""


def _parse_runs(pattern: re.Pattern, text: str) -> List[dict]:
    """Extract a list of {start, end} from lines like `lavfi.black_start=123.45`.

    Pass a compiled pattern with two groups — start and end — or a pattern
    that produces a single-group match; in that case we pair up successive
    start/end events.
    """
    runs = []
    for m in pattern.finditer(text):
        gs = m.groups()
        if len(gs) == 2:
            try:
                runs.append({"start": float(gs[0]), "end": float(gs[1])})
            except (TypeError, ValueError):
                pass
    return runs


# ── Silence detection (-af silencedetect) ─────────────────────────────

_SILENCE_START_RE = re.compile(r"silence_start:\s*(-?\d+(?:\.\d+)?)")
_SILENCE_END_RE   = re.compile(r"silence_end:\s*(-?\d+(?:\.\d+)?)\s*\|\s*silence_duration:\s*(-?\d+(?:\.\d+)?)")

def detect_silence(path: str, min_run: float = 1.0, threshold_db: float = -50.0) -> List[dict]:
    """Return list of {start, end} for each silent run ≥ `min_run` seconds
    where audio level stays below `threshold_db`.

    Uses ffmpeg's silencedetect audio filter.
    """
    out = _run_ffmpeg_scan([
        "-i", path,
        "-vn",
        "-af", f"silencedetect=n={threshold_db}dB:d={min_run}",
    ])
    # silencedetect logs: "silence_start: X" then later "silence_end: Y | silence_duration: Z"
    starts = [float(m.group(1)) for m in _SILENCE_START_RE.finditer(out)]
    ends   = [float(m.group(1)) for m in _SILENCE_END_RE.finditer(out)]
    # Pair up sequentially. If a trailing silence runs to EOF, silencedetect
    # doesn't emit an end — we drop that one (conservative).
    runs = []
    for s, e in zip(starts, ends):
        if e > s:
            runs.append({"start": round(s, 3), "end": round(e, 3)})
    return runs


# ── Black frame detection (-vf blackdetect) ──────────────────────────

_BLACK_RE = re.compile(
    r"black_start:\s*(-?\d+(?:\.\d+)?)\s+black_end:\s*(-?\d+(?:\.\d+)?)"
)

def detect_black_frames(path: str, min_run: float = 1.0,
                        pixel_threshold: float = 0.98) -> List[dict]:
    """Return list of {start, end} for each black-frame run ≥ `min_run` seconds.

    `pixel_threshold` is the fraction of pixels below the darkness
    threshold required to call a frame "black". 0.98 = 98% of pixels.
    Higher = stricter (fewer matches).
    """
    out = _run_ffmpeg_scan([
        "-i", path,
        "-vf", f"blackdetect=d={min_run}:pix_th=0.10:pic_th={pixel_threshold}",
        "-an",
    ])
    return _parse_runs(_BLACK_RE, out)


# ── Freeze frame detection (-vf freezedetect) ────────────────────────

_FREEZE_START_RE = re.compile(r"freeze_start:\s*(-?\d+(?:\.\d+)?)")
_FREEZE_END_RE   = re.compile(r"freeze_end:\s*(-?\d+(?:\.\d+)?)")

def detect_freeze_frames(path: str, min_run: float = 1.0,
                         noise_threshold: float = 0.003) -> List[dict]:
    """Return list of {start, end} for each frozen-frame run ≥ `min_run` seconds
    where frame-to-frame motion stays below `noise_threshold` (lower = stricter).
    """
    out = _run_ffmpeg_scan([
        "-i", path,
        "-vf", f"freezedetect=n={noise_threshold}:d={min_run}",
        "-an",
    ])
    starts = [float(m.group(1)) for m in _FREEZE_START_RE.finditer(out)]
    ends   = [float(m.group(1)) for m in _FREEZE_END_RE.finditer(out)]
    runs = []
    for s, e in zip(starts, ends):
        if e > s:
            runs.append({"start": round(s, 3), "end": round(e, 3)})
    return runs


# ── EBU R128 loudness (-af ebur128) ───────────────────────────────────

_EBUR128_SUMMARY_RE = re.compile(
    r"Integrated loudness:\s*\n\s*I:\s*(-?\d+(?:\.\d+)?)\s*LUFS", re.MULTILINE,
)
_EBUR128_TRUE_PEAK_RE = re.compile(
    r"True peak:\s*\n\s*Peak:\s*(-?\d+(?:\.\d+)?)\s*dBFS", re.MULTILINE,
)

def measure_loudness(path: str) -> dict:
    """Measure integrated loudness (LUFS) and true peak (dBTP) per EBU R128.

    Returns {"integrated_lufs": float|None, "true_peak_dbtp": float|None}.
    """
    out = _run_ffmpeg_scan([
        "-i", path,
        "-vn",
        "-af", "ebur128=peak=true",
    ])
    lufs_match = _EBUR128_SUMMARY_RE.search(out)
    peak_match = _EBUR128_TRUE_PEAK_RE.search(out)
    return {
        "integrated_lufs": float(lufs_match.group(1)) if lufs_match else None,
        "true_peak_dbtp":  float(peak_match.group(1)) if peak_match else None,
    }


# ── VFR / interlaced detection (Tier 2) ──────────────────────────────

def detect_vfr(path: str, frame_sample: int = 500) -> dict:
    """Approximate VFR check: sample up to N frames and measure frame
    interval variance. VFR files have non-trivial interval spread.
    Returns {"is_vfr": bool, "fps_stddev": float|None, "sampled": int}.
    """
    try:
        import statistics
    except ImportError:
        return {"is_vfr": False, "fps_stddev": None, "sampled": 0}
    cmd = [
        "ffprobe" if not os.environ.get("FFPROBE_BINARY") else os.environ["FFPROBE_BINARY"],
        "-v", "quiet",
        "-select_streams", "v:0",
        "-show_entries", "frame=pkt_duration_time",
        "-of", "csv=p=0",
        "-read_intervals", f"%+#{frame_sample}",
        path,
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return {"is_vfr": False, "fps_stddev": None, "sampled": 0}

    durations = []
    for line in r.stdout.strip().splitlines():
        try:
            v = float(line.strip())
            if v > 0:
                durations.append(v)
        except ValueError:
            pass

    if len(durations) < 10:
        return {"is_vfr": False, "fps_stddev": None, "sampled": len(durations)}
    stddev = statistics.stdev(durations)
    mean = statistics.mean(durations)
    # If stddev > 10% of the mean interval, call it VFR.
    is_vfr = (stddev / mean) > 0.1 if mean > 0 else False
    return {"is_vfr": is_vfr, "fps_stddev": round(stddev, 6), "sampled": len(durations)}


_IDET_SUMMARY_RE = re.compile(
    r"Multi frame detection:\s*TFF:\s*(\d+)\s*BFF:\s*(\d+)\s*Progressive:\s*(\d+)\s*Undetermined:\s*(\d+)"
)

def detect_interlaced(path: str, sample_frames: int = 300) -> dict:
    """Approximate interlacing check using the idet filter over the
    first N frames. Returns {"is_interlaced": bool, "progressive": int,
    "interlaced": int, "sampled": int}.
    """
    out = _run_ffmpeg_scan([
        "-i", path,
        "-vf", f"idet",
        "-frames:v", str(sample_frames),
        "-an",
    ], timeout=120)
    m = _IDET_SUMMARY_RE.search(out)
    if not m:
        return {"is_interlaced": False, "progressive": 0, "interlaced": 0, "sampled": 0}
    tff, bff, prog, und = (int(x) for x in m.groups())
    interlaced = tff + bff
    sampled = interlaced + prog + und
    is_interlaced = interlaced > prog * 0.2 if sampled > 0 else False
    return {
        "is_interlaced": is_interlaced,
        "progressive":   prog,
        "interlaced":    interlaced,
        "sampled":       sampled,
    }


# ── Convenience: total duration covered by a list of runs ────────────

def total_run_duration(runs: List[dict]) -> float:
    """Sum of (end - start) across a runs list."""
    return sum(max(0.0, r.get("end", 0) - r.get("start", 0)) for r in runs)


def run_ratio(runs: List[dict], total_seconds: Optional[float]) -> float:
    """Fraction of total_seconds covered by runs. 0 if total missing."""
    if not total_seconds or total_seconds <= 0:
        return 0.0
    return min(1.0, total_run_duration(runs) / total_seconds)
