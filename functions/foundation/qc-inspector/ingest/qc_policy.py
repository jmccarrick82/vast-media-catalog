"""QC verdict policy.

Pure function: given a probe result, detector outputs, and a Config
object, return the verdict (`passed`/`warn`/`failed`) + human-readable
reason(s).

Kept separate from the detectors so unit tests can cover corner cases
without running ffmpeg.
"""

from typing import Optional, List


def evaluate_qc(
    probe: dict,
    black_runs: List[dict],
    freeze_runs: List[dict],
    silence_runs: List[dict],
    loudness: Optional[dict],
    vfr: Optional[dict],
    interlaced: Optional[dict],
    cfg,
) -> dict:
    """Apply the config thresholds. Returns a dict:

        {
          "status":  "passed" | "warn" | "failed",
          "reason":  "<first severe issue or 'all checks clean'>",
          "issues":  [<human-readable issue strings, warn + fail>],
          "ratios":  {"black": 0.0, "freeze": 0.0, "silence": 0.0},
        }
    """
    from .qc import run_ratio

    issues: List[str] = []
    severity = "passed"   # upgrade to warn/failed as we find things

    def _fail(msg: str):
        nonlocal severity
        issues.append(f"FAIL: {msg}")
        severity = "failed"

    def _warn(msg: str):
        nonlocal severity
        issues.append(f"WARN: {msg}")
        if severity != "failed":
            severity = "warn"

    duration = probe.get("duration_seconds") or 0.0
    video = probe.get("video") or {}
    audio = probe.get("audio") or {}

    # ── Structure ──────────────────────────────────────────────
    min_dur = cfg.get_duration("min_duration_seconds")
    max_dur = cfg.get_duration("max_duration_seconds")
    if duration <= 0:
        _fail("unknown or zero duration (corrupt file?)")
    elif duration < min_dur:
        _fail(f"duration {duration:.1f}s < minimum {min_dur:.1f}s")
    elif duration > max_dur:
        _fail(f"duration {duration:.1f}s > maximum {max_dur:.1f}s")

    has_video = probe.get("video") is not None
    has_audio = probe.get("audio") is not None
    if not has_video and cfg.get_bool("require_video_stream"):
        _fail("no video stream present")
    if not has_audio:
        if cfg.get_bool("require_audio_stream"):
            _fail("no audio stream present")
        else:
            _warn("no audio stream present")

    if has_video:
        vcodec = video.get("codec") or ""
        allow = cfg.get_list("video_codec_allowlist")
        if vcodec and allow and vcodec not in allow:
            _fail(f"video codec '{vcodec}' not in allowlist {allow}")
    if has_audio:
        acodec = audio.get("codec") or ""
        allow = cfg.get_list("audio_codec_allowlist")
        if acodec and allow and acodec not in allow:
            _warn(f"audio codec '{acodec}' not in allowlist {allow}")

    # ── Resolution ────────────────────────────────────────────
    if has_video:
        w = video.get("width") or 0
        h = video.get("height") or 0
        if w < cfg.get_int("min_video_width") or h < cfg.get_int("min_video_height"):
            _fail(f"video resolution {w}x{h} below minimum "
                  f"{cfg.get_int('min_video_width')}x{cfg.get_int('min_video_height')}")
        elif (w < cfg.get_int("warn_below_width") or
              h < cfg.get_int("warn_below_height")):
            _warn(f"video resolution {w}x{h} below SD threshold "
                  f"{cfg.get_int('warn_below_width')}x{cfg.get_int('warn_below_height')}")

    # ── Black / freeze / silence ratios ───────────────────────
    black_ratio   = run_ratio(black_runs, duration)
    freeze_ratio  = run_ratio(freeze_runs, duration)
    silence_ratio = run_ratio(silence_runs, duration)

    if black_ratio >= cfg.get_percent("black_frame_max_ratio_fail"):
        _fail(f"black frames cover {_pct(black_ratio)} of duration "
              f"(>= {_pct(cfg.get_percent('black_frame_max_ratio_fail'))})")
    elif black_ratio >= cfg.get_percent("black_frame_max_ratio_warn"):
        _warn(f"black frames cover {_pct(black_ratio)} of duration "
              f"(>= {_pct(cfg.get_percent('black_frame_max_ratio_warn'))})")

    if freeze_ratio >= cfg.get_percent("freeze_max_ratio_fail"):
        _fail(f"freeze frames cover {_pct(freeze_ratio)} of duration")
    elif freeze_ratio >= cfg.get_percent("freeze_max_ratio_warn"):
        _warn(f"freeze frames cover {_pct(freeze_ratio)} of duration")

    if silence_ratio >= cfg.get_percent("silence_max_ratio_fail"):
        _fail(f"silence covers {_pct(silence_ratio)} of duration")
    elif silence_ratio >= cfg.get_percent("silence_max_ratio_warn"):
        _warn(f"silence covers {_pct(silence_ratio)} of duration")

    # ── Loudness (optional) ──────────────────────────────────
    if loudness is not None:
        lufs = loudness.get("integrated_lufs")
        peak = loudness.get("true_peak_dbtp")
        lufs_min = cfg.get_float("loudness_min_lufs")
        peak_max = cfg.get_db("loudness_max_true_peak_dbtp")
        if lufs is not None and lufs < lufs_min:
            _warn(f"integrated loudness {lufs:.1f} LUFS < {lufs_min:.1f} (too quiet)")
        if peak is not None and peak > peak_max:
            _warn(f"true peak {peak:.1f} dBTP > {peak_max:.1f} (clipping risk)")

    # ── Stream quirks (optional) ─────────────────────────────
    if vfr and vfr.get("is_vfr"):
        _warn(f"variable frame rate detected (fps stddev {vfr.get('fps_stddev')})")
    if interlaced and interlaced.get("is_interlaced"):
        _warn(f"interlaced content detected ({interlaced.get('interlaced')}/"
              f"{interlaced.get('sampled')} frames)")

    reason = issues[0] if issues else "all checks clean"
    return {
        "status": severity,
        "reason": reason,
        "issues": issues,
        "ratios": {
            "black":   round(black_ratio, 4),
            "freeze":  round(freeze_ratio, 4),
            "silence": round(silence_ratio, 4),
        },
    }


def _pct(f: float) -> str:
    return f"{round(f * 100)}%"
