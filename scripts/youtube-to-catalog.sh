#!/bin/bash
# Download YouTube videos at proxy resolution and upload to the media catalog S3 bucket.
#
# Usage:
#   ./youtube-to-catalog.sh URL [URL2 URL3 ...]
#   ./youtube-to-catalog.sh playlist_url          # downloads all videos in playlist
#
# Proxy settings: 480p, H.264, low bitrate (~500kbps video + 64kbps audio)
# Approx size: ~4MB per minute of video

set -euo pipefail

# Override via env: REMOTE=user@host REMOTE_PASS=... ./youtube-to-catalog.sh <url>
S3CFG="${S3CFG:-/home/vastdata/james/james.s3cfg}"
BUCKET="${BUCKET:-s3://james-media-catalog}"
REMOTE="${REMOTE:-vastdata@YOUR_HOST}"
REMOTE_PASS="${REMOTE_PASS:-YOUR_PASSWORD}"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# Proxy encoding settings
WIDTH=854        # 480p width
HEIGHT=480       # 480p height
VIDEO_BITRATE="500k"
AUDIO_BITRATE="64k"

if [ $# -eq 0 ]; then
    echo "Usage: $0 <youtube-url> [url2 url3 ...]"
    echo "       $0 <playlist-url>"
    exit 1
fi

echo "=== YouTube to Media Catalog ==="
echo "Downloading to proxy resolution (${WIDTH}x${HEIGHT}, ${VIDEO_BITRATE} video)"
echo ""

for url in "$@"; do
    echo "--- Processing: $url ---"

    # Download best available (yt-dlp handles format selection)
    # Use --no-playlist for single videos, playlists will auto-expand
    yt-dlp \
        -o "$TMPDIR/%(title)s.%(ext)s" \
        --restrict-filenames \
        --format "bestvideo[height<=720]+bestaudio/best[height<=720]/best" \
        --merge-output-format mp4 \
        "$url" 2>&1 | grep -E "Downloading|Destination|Merging|Already"

    echo ""
done

# Now transcode everything to proxy resolution
echo "=== Transcoding to proxy resolution ==="
PROXY_DIR="$TMPDIR/proxy"
mkdir -p "$PROXY_DIR"

for f in "$TMPDIR"/*.mp4 "$TMPDIR"/*.mkv "$TMPDIR"/*.webm; do
    [ -f "$f" ] || continue
    basename=$(basename "$f")
    name="${basename%.*}"
    outfile="$PROXY_DIR/${name}.mp4"

    echo "Transcoding: $basename"
    ffmpeg -i "$f" \
        -vf "scale=${WIDTH}:${HEIGHT}:force_original_aspect_ratio=decrease,pad=${WIDTH}:${HEIGHT}:(ow-iw)/2:(oh-ih)/2" \
        -c:v libx264 -preset fast -b:v "$VIDEO_BITRATE" \
        -c:a aac -b:a "$AUDIO_BITRATE" -ac 2 \
        -movflags +faststart \
        -y "$outfile" 2>&1 | grep -E "^$|Duration|frame=.*speed=" | tail -1

    size=$(du -sh "$outfile" | cut -f1)
    echo "  -> $outfile ($size)"
    echo ""
done

# Upload to remote S3
echo "=== Uploading to $BUCKET ==="
for f in "$PROXY_DIR"/*.mp4; do
    [ -f "$f" ] || continue
    basename=$(basename "$f")
    size=$(du -sh "$f" | cut -f1)
    echo "Uploading: $basename ($size)"

    # SCP to remote, then s3cmd from there
    sshpass -p "$REMOTE_PASS" scp -o StrictHostKeyChecking=no "$f" "$REMOTE:/tmp/$basename"
    sshpass -p "$REMOTE_PASS" ssh -o StrictHostKeyChecking=no "$REMOTE" \
        "s3cmd -c $S3CFG put /tmp/$basename $BUCKET/$basename && rm /tmp/$basename"

    echo "  -> $BUCKET/$basename"
done

echo ""
echo "=== Done! ==="
echo "Check the dashboard: http://YOUR_HOST:3001/"
