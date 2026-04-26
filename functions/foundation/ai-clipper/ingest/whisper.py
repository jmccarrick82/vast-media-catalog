"""Whisper transcription helper — POSTs an audio span to the
inference endpoint's `/v1/audio/transcriptions` route and returns the
text.

Lifted out of subclip-ai-analyzer so audio.py can call it without
duplicating the multipart-encoding boilerplate. Two entry points:

    transcribe_file(audio_path, ...)       — already-extracted audio file
    transcribe_segment(src, start, end, ...) — extracts a span via ffmpeg
                                                 then transcribes it

`transcribe_segment` writes to a temp WAV next to the source — short
spans (5–30s of broadcast audio) are tiny so this is fine on a Knative
pod's ephemeral disk.
"""

from __future__ import annotations

import http.client
import json
import os
import subprocess
import tempfile
from typing import Optional


INFERENCE_HOST_DEFAULT = "inference.selab.vastdata.com"
WHISPER_MODEL_DEFAULT = "local-mlx/whisper-turbo"


class WhisperError(Exception):
    """Whisper call failed."""


def _ffmpeg_binary() -> str:
    override = os.environ.get("FFMPEG_BINARY")
    if override and os.path.isfile(override):
        return override
    return "ffmpeg"


def _extract_wav(
    src: str,
    start: float,
    end: float,
    out_wav: str,
    timeout: int = 60,
) -> str:
    """Pull [start, end] of `src` to a 16kHz mono WAV at `out_wav`."""
    cmd = [
        _ffmpeg_binary(),
        "-nostdin", "-hide_banner",
        "-loglevel", "error",
        "-y",
        "-ss", f"{max(0.0, start):.3f}",
        "-to", f"{max(start + 0.1, end):.3f}",
        "-i", src,
        "-vn",
        "-ac", "1",
        "-ar", "16000",
        "-c:a", "pcm_s16le",
        out_wav,
    ]
    try:
        r = subprocess.run(
            cmd,
            capture_output=True, text=True,
            stdin=subprocess.DEVNULL,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as e:
        raise WhisperError(
            f"ffmpeg wav extract timed out ({timeout}s) for {start:.2f}..{end:.2f}"
        ) from e
    if r.returncode != 0 or not os.path.isfile(out_wav) or os.path.getsize(out_wav) == 0:
        raise WhisperError(
            f"ffmpeg wav extract failed ({start:.2f}..{end:.2f}): {r.stderr[:300]}"
        )
    return out_wav


def transcribe_file(
    audio_path: str,
    api_key: str,
    *,
    model: str = WHISPER_MODEL_DEFAULT,
    inference_host: Optional[str] = None,
    timeout: int = 120,
) -> str:
    """POST `audio_path` to the Whisper endpoint, return the transcript text.
    Returns "" on empty/no-speech responses; raises WhisperError on HTTP error.
    """
    host = inference_host or INFERENCE_HOST_DEFAULT
    boundary = "----WhisperBoundary"
    with open(audio_path, "rb") as f:
        audio_data = f.read()

    body = b""
    body += f"--{boundary}\r\n".encode()
    body += b"Content-Disposition: form-data; name=\"model\"\r\n\r\n"
    body += model.encode() + b"\r\n"
    body += f"--{boundary}\r\n".encode()
    body += b"Content-Disposition: form-data; name=\"file\"; filename=\"audio.wav\"\r\n"
    body += b"Content-Type: audio/wav\r\n\r\n"
    body += audio_data
    body += b"\r\n"
    body += f"--{boundary}--\r\n".encode()

    conn = http.client.HTTPSConnection(host, timeout=timeout)
    try:
        conn.request(
            "POST",
            "/v1/audio/transcriptions",
            body=body,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type":  f"multipart/form-data; boundary={boundary}",
            },
        )
        resp = conn.getresponse()
        raw = resp.read().decode()
        if resp.status != 200:
            raise WhisperError(f"Whisper {resp.status}: {raw[:300]}")
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise WhisperError(f"Whisper non-JSON response: {raw[:300]}") from e
        return (data.get("text") or "").strip()
    finally:
        conn.close()


def transcribe_segment(
    src: str,
    start: float,
    end: float,
    api_key: str,
    *,
    model: str = WHISPER_MODEL_DEFAULT,
    inference_host: Optional[str] = None,
    timeout: int = 120,
) -> str:
    """Extract [start, end] of `src` to a temp WAV, transcribe, clean up.
    Returns the transcript text (may be empty)."""
    if end <= start:
        return ""
    fd, wav_path = tempfile.mkstemp(prefix="whisper-", suffix=".wav")
    os.close(fd)
    try:
        _extract_wav(src, start, end, wav_path, timeout=min(timeout, 60))
        return transcribe_file(
            wav_path,
            api_key=api_key,
            model=model,
            inference_host=inference_host,
            timeout=timeout,
        )
    finally:
        try:
            os.unlink(wav_path)
        except OSError:
            pass
