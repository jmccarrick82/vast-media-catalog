#!/bin/bash
#
# Seed the catalog with a diverse 20-video demo corpus.
#
# Each clip: 90s, 480p, 400kbps — small + fast to process.
# Downloads in parallel (5 at a time), transcodes serially,
# uploads via .91 over s3cmd.
#
# Tolerates per-query failures — just skips and keeps going.
#
# Usage:
#   scripts/seed-demo-corpus.sh

set -euo pipefail

# Override via env: REMOTE=user@host REMOTE_PASS=... scripts/seed-demo-corpus.sh
REMOTE="${REMOTE:-vastdata@YOUR_HOST}"
REMOTE_PASS="${REMOTE_PASS:-YOUR_PASSWORD}"
S3CFG="${S3CFG:-/home/vastdata/james/james.s3cfg}"
BUCKET="${BUCKET:-s3://james-media-catalog}"

WIDTH=854
HEIGHT=480
VBITRATE="400k"
ABITRATE="64k"
DURATION=90   # seconds

# Persistent working dir so we can resume if upload fails — user can
# clean it up later. Keeps transcoded files around so Phase 3 can rerun.
TMPDIR="${SEED_CORPUS_DIR:-/tmp/seed-corpus}"
RAW="$TMPDIR/raw"
PROXY="$TMPDIR/proxy"
mkdir -p "$RAW" "$PROXY"
echo "Working dir: $TMPDIR (persisted — delete manually when done)"

# slug -> search query (20 varied topics)
declare -a SLUGS=(
  "cooking-pasta"
  "wildlife-lion"
  "spacex-launch"
  "piano-classical"
  "soccer-goal"
  "chemistry-demo"
  "ancient-egypt"
  "cherry-blossom"
  "ferrari-review"
  "tornado-storm"
  "guitar-solo"
  "coral-reef"
  "ballet-dance"
  "airplane-landing"
  "volcano-eruption"
  "robot-demo"
  "paris-travel"
  "basketball-dunk"
  "aurora-lights"
  "oil-painting"
)
declare -a QUERIES=(
  "cooking pasta tutorial short"
  "lion wildlife hunting documentary"
  "spacex falcon rocket launch"
  "piano classical performance short"
  "soccer football goal highlight"
  "chemistry demonstration experiment"
  "ancient egypt pyramids documentary"
  "japanese cherry blossom spring timelapse"
  "ferrari car review walkaround"
  "tornado storm chaser footage"
  "electric guitar solo rock"
  "coral reef scuba diving fish"
  "ballet dance performance short"
  "airplane cockpit landing view"
  "volcano eruption lava flow"
  "boston dynamics robot demo"
  "paris eiffel tower travel walk"
  "nba basketball dunk highlight"
  "northern lights aurora timelapse"
  "oil painting art technique demo"
)

N=${#SLUGS[@]}

# ── Phase 1: download (parallel, 5 at a time) ──
# Skip anything already downloaded so rerun is cheap.
echo "=== Phase 1: downloading $N clips via yt-dlp (parallel x5) ==="
for i in "${!SLUGS[@]}"; do
  slug="${SLUGS[$i]}"
  query="${QUERIES[$i]}"
  existing=$(ls "$RAW/${slug}".* 2>/dev/null | head -1 || true)
  if [ -n "$existing" ]; then
    echo "  [${i}] ${slug}: already have $(basename "$existing") — skip"
    continue
  fi
  echo "  [${i}] ${slug}: ${query}"
  (
    yt-dlp -o "$RAW/${slug}.%(ext)s" \
      --no-warnings \
      --restrict-filenames \
      --format "bestvideo[height<=480][ext=mp4]+bestaudio/best[height<=480][ext=mp4]/best[height<=480]" \
      --merge-output-format mp4 \
      --download-sections "*0-${DURATION}" \
      --match-filter "duration > 20" \
      --no-playlist \
      "ytsearch1:${query}" >/dev/null 2>&1 || true
  ) &
  if (( (i + 1) % 5 == 0 )); then wait; fi
done
wait
echo "  Downloaded: $(ls -1 $RAW/*.mp4 $RAW/*.mkv $RAW/*.webm 2>/dev/null | wc -l | tr -d ' ') of $N"

# ── Phase 2: transcode (serial, cheap) — skip if already done ──
echo "=== Phase 2: transcoding to ${WIDTH}x${HEIGHT} @ ${VBITRATE} ==="
for f in "$RAW"/*.mp4 "$RAW"/*.mkv "$RAW"/*.webm; do
  [ -f "$f" ] || continue
  slug=$(basename "$f")
  slug="${slug%.*}"
  out="$PROXY/${slug}.mp4"
  if [ -f "$out" ]; then
    echo "  = $(basename "$out") ($(du -h "$out" | cut -f1)) — already transcoded"
    continue
  fi
  ffmpeg -y -i "$f" -t $DURATION \
    -vf "scale=${WIDTH}:${HEIGHT}:force_original_aspect_ratio=decrease,pad=${WIDTH}:${HEIGHT}:(ow-iw)/2:(oh-ih)/2" \
    -c:v libx264 -preset fast -b:v "$VBITRATE" \
    -c:a aac -b:a "$ABITRATE" -ac 2 \
    -movflags +faststart \
    "$out" </dev/null >/dev/null 2>&1 && \
    echo "  ✓ $(basename "$out") ($(du -h "$out" | cut -f1))"
done
echo "  Transcoded: $(ls -1 $PROXY/*.mp4 2>/dev/null | wc -l | tr -d ' ') clips"

# ── Phase 3: upload via .91, skip already-uploaded, tolerate per-file failures ──
echo "=== Phase 3: uploading to $BUCKET via $REMOTE ==="

# List whatever's already in S3 so we skip it (saves a ton on rerun)
EXISTING=$(sshpass -p "$REMOTE_PASS" ssh -q -o StrictHostKeyChecking=no "$REMOTE" \
  "s3cmd -c $S3CFG ls $BUCKET/ 2>/dev/null | awk '{print \$NF}' | xargs -n1 basename 2>/dev/null" || true)

for f in "$PROXY"/*.mp4; do
  [ -f "$f" ] || continue
  base=$(basename "$f")
  if echo "$EXISTING" | grep -qx "$base"; then
    echo "  = $BUCKET/$base — already there"
    continue
  fi
  if sshpass -p "$REMOTE_PASS" scp -q -o StrictHostKeyChecking=no -o ConnectTimeout=15 "$f" "$REMOTE:/tmp/$base" && \
     sshpass -p "$REMOTE_PASS" ssh -q -o StrictHostKeyChecking=no -o ConnectTimeout=15 "$REMOTE" \
       "s3cmd -c $S3CFG put /tmp/$base $BUCKET/$base >/dev/null && rm /tmp/$base"; then
    echo "  ✓ $BUCKET/$base"
  else
    echo "  ✗ $base — upload failed (will retry on next script run)"
  fi
done

echo ""
echo "=== Done. Check http://YOUR_HOST:3001/search once pipeline completes ==="
echo "    Working dir preserved at $TMPDIR — rm -rf it when you're satisfied"
