"""Video analysis utilities: ffprobe metadata, perceptual hashing, frame extraction."""

import hashlib
import json
import os
import subprocess
import tempfile


def extract_metadata(video_path: str) -> dict:
    """Extract video metadata using ffprobe.

    Returns dict with: duration, codec, resolution, fps, bitrate, audio_codec,
    audio_channels, creation_time, format_name.
    """
    cmd = [
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        video_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        raise RuntimeError(f"ffprobe failed: {result.stderr}")

    data = json.loads(result.stdout)
    fmt = data.get("format", {})
    streams = data.get("streams", [])

    video_stream = next((s for s in streams if s.get("codec_type") == "video"), {})
    audio_stream = next((s for s in streams if s.get("codec_type") == "audio"), {})

    # Parse FPS from r_frame_rate (e.g., "30000/1001")
    fps = 0.0
    r_frame_rate = video_stream.get("r_frame_rate", "0/1")
    if "/" in r_frame_rate:
        num, den = r_frame_rate.split("/")
        if int(den) > 0:
            fps = round(int(num) / int(den), 3)

    tags = fmt.get("tags", {})

    # creation_time: prefer explicit creation_time tag, fall back to date tag
    creation_time = tags.get("creation_time", "") or tags.get("date", "")

    return {
        "duration_seconds": float(fmt.get("duration", 0)),
        "file_size_bytes": int(fmt.get("size", 0)),
        "format_name": fmt.get("format_name", ""),
        "bitrate": int(fmt.get("bit_rate", 0)),
        "video_codec": video_stream.get("codec_name", ""),
        "width": int(video_stream.get("width", 0)),
        "height": int(video_stream.get("height", 0)),
        "fps": fps,
        "pixel_format": video_stream.get("pix_fmt", ""),
        "audio_codec": audio_stream.get("codec_name", ""),
        "audio_channels": int(audio_stream.get("channels", 0)),
        "audio_sample_rate": int(audio_stream.get("sample_rate", 0)),
        "creation_time": creation_time,
        "title": tags.get("title", ""),
        "encoder": tags.get("encoder", ""),
    }


def compute_sha256(file_path: str) -> str:
    """Compute SHA-256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def extract_keyframes(video_path: str, max_frames: int = 10) -> list[str]:
    """Extract key frames from video as temporary JPEG files.

    Returns list of temp file paths. Caller responsible for cleanup.
    """
    tmpdir = tempfile.mkdtemp(prefix="frames_")
    output_pattern = os.path.join(tmpdir, "frame_%04d.jpg")

    cmd = [
        "ffmpeg",
        "-v", "quiet",
        "-i", video_path,
        "-vf", f"select=eq(pict_type\\,I),scale=320:-1",
        "-vsync", "vfr",
        "-frames:v", str(max_frames),
        "-q:v", "2",
        output_pattern,
    ]
    subprocess.run(cmd, capture_output=True, timeout=120)

    frames = sorted([
        os.path.join(tmpdir, f) for f in os.listdir(tmpdir)
        if f.endswith(".jpg")
    ])
    return frames


def compute_perceptual_hash(image_path: str) -> str:
    """Compute perceptual hash (pHash) of an image using imagehash."""
    import imagehash
    from PIL import Image

    img = Image.open(image_path)
    phash = imagehash.phash(img, hash_size=16)
    return str(phash)


def compute_video_perceptual_hash(video_path: str) -> str:
    """Compute a perceptual hash for a video by hashing its key frames.

    Extracts key frames, computes pHash for each, concatenates them
    into a composite video fingerprint.
    """
    frames = extract_keyframes(video_path, max_frames=5)
    if not frames:
        return ""

    hashes = []
    for frame_path in frames:
        try:
            h = compute_perceptual_hash(frame_path)
            hashes.append(h)
        except Exception:
            pass

    # Cleanup temp frames
    for f in frames:
        try:
            os.unlink(f)
        except OSError:
            pass
    tmpdir = os.path.dirname(frames[0]) if frames else None
    if tmpdir:
        try:
            os.rmdir(tmpdir)
        except OSError:
            pass

    return "|".join(hashes) if hashes else ""


def create_subclip(video_path: str, start_sec: float, duration_sec: float,
                   output_path: str = None, metadata_tags: dict = None) -> str:
    """Create a subclip from a video file using ffmpeg stream copy (no re-encoding).

    Args:
        video_path: Path to the source video file.
        start_sec: Start time in seconds.
        duration_sec: Duration of the subclip in seconds.
        output_path: Optional explicit output path. If None, a temp file is created.
        metadata_tags: Optional dict of key-value pairs to embed in the output
                       container via ffmpeg -metadata (e.g., {"parent_asset_id": "abc123"}).

    Returns:
        Path to the output subclip file. Caller responsible for cleanup if temp.
    """
    if output_path is None:
        ext = os.path.splitext(video_path)[1] or ".mp4"
        tmp = tempfile.NamedTemporaryFile(suffix=ext, delete=False)
        output_path = tmp.name
        tmp.close()

    cmd = [
        "ffmpeg",
        "-v", "quiet",
        "-i", video_path,
        "-ss", str(start_sec),
        "-t", str(duration_sec),
        "-c", "copy",
    ]

    if metadata_tags:
        for key, value in metadata_tags.items():
            cmd.extend(["-metadata", f"{key}={value}"])

    cmd.extend(["-y", output_path])

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg subclip failed: {result.stderr}")

    return output_path


def extract_audio_segment(video_path: str, duration_seconds: int = 30) -> str:
    """Extract audio from the first N seconds of video as a temp WAV file.

    Returns temp file path. Caller responsible for cleanup.
    """
    tmp = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
    tmp_path = tmp.name
    tmp.close()

    cmd = [
        "ffmpeg",
        "-v", "quiet",
        "-i", video_path,
        "-t", str(duration_seconds),
        "-vn",
        "-acodec", "pcm_s16le",
        "-ar", "16000",
        "-ac", "1",
        tmp_path,
    ]
    subprocess.run(cmd, capture_output=True, timeout=120)
    return tmp_path
