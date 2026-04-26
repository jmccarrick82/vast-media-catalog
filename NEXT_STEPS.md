# NEXT_STEPS — Current state + what's queued

_Last refreshed: 2026-04-25 evening_

## Top of queue: audio-aware excitement signal (Phase 2.6)

**Why:** vision-only clipping picks 5 visually-similar "swing/at-bat"
clips on baseball that don't capture actual moments of excitement —
it has no way to tell a routine pitch from a walk-off home run.

**Plan + step-by-step build sequence:** see
[`docs/TODO_AUDIO_EXCITEMENT.md`](docs/TODO_AUDIO_EXCITEMENT.md).

Headline approach: new `shared/ingest/audio.py` module wrapping ffmpeg
`ebur128` + `astats` (cheap; loudness peak above source baseline) plus
optional Whisper transcript (expensive; opt-in). Per-candidate audio
features get folded into the LLM curator's prompt so it can prefer
"swing into a crowd roar" over "swing into silence".

4 new config knobs under `ai-clipper`:
`audio_analysis_enabled`, `audio_use_whisper`,
`audio_baseline_lufs_window_seconds`, `audio_excitement_min_db`.

3 new columns on `extracted_clips`:
`audio_peak_lufs`, `audio_excitement_db`, `audio_transcript_excerpt`.

Acceptance test: same 10-min CWS source, same prompt, but the 5
selected clips should shift compared to today's vision-only picks
(`8.6s / 102s / 232s / 326s / 543s`) — at least 2 should land on
moments where audio is ≥8 dB above baseline.

## Where things stand

### Pre-ingest pipeline (Phases 1–3) — ✅ deployed and proven end-to-end

- **Phase 1 — qc-inspector** (revision 6 live): ffprobe + 9 non-AI detectors, policy-gated, routes to `qc-passed` or `qc-failed`, preserves uploader S3 metadata through the move, writes full result row to `source_videos`.
- **Phase 2 — ai-clipper** (revision 3 live): ffmpeg scene detect → per-shot vision classify (11B with 90B escalation) → merge + constrain → cut to `james-media-clips/<source_id>/` → emit `_ready.json` marker.
- **Phase 3 — media-packager** (revision 4 live): transcode each clip into every preset → sign each rendition with C2PA via `c2patool` → build JSON sidecar manifest → upload bundle to `james-media-deliveries/<package_id>/`. All 5 C2PA assertions embed cleanly (actions, creative_work, training-mining, com.vast.ai_clip_selection, bmff hash). Verified with `c2patool` locally against a real signed rendition.

Pipeline is `james-media-unified` on DataEngine cluster `var201-k8s`. Status: `Ready`.

### Runtime config

- `function_configs` table seeded with **72 knobs** across 5 scopes (`qc-inspector` 27, `ai-clipper` 16, `packager` 12, `provenance` 10, `subclipper` 7).
- Every function reads with a 60s per-pod cache — edits via `/settings` UI take effect on the next handler invocation, no redeploys needed.

### Code-complete but not yet deployed

- **Webapp `/packages` UI** — `PackagesPage.jsx` (grid), `PackageDetailPage.jsx` (player + live C2PA panel via `c2patool` server-side), updated `ArchitecturePage.jsx`, bulk config editor + "Restore defaults" on `/settings`. `c2patool` is bundled into the webapp Dockerfile.
- **Phase 4 — `james-subclipper` function** (`functions/foundation/subclipper/`): direct-invoke handler that takes `{src, clips: [{start, end, …}]}` and loops `clips.cut_clip()` with per-clip override precedence. Library primitive (`shared/ingest/clips.py::cut_clip`) already in use by Phases 2 and 3.

## Immediate next steps (when you're ready)

### A. Ship the webapp

```bash
# From /Users/james/projects/media/catalog/
TAG=packages-$(date +%s)
docker buildx build --platform linux/amd64 \
  -f webapp/Dockerfile \
  -t docker.selab.vastdata.com:5000/james/media-catalog-webapp:$TAG \
  --load ./webapp
docker push docker.selab.vastdata.com:5000/james/media-catalog-webapp:$TAG
# Then redeploy the webapp on .91 with the new tag
```

Smoke test once deployed:
- Open `http://10.143.11.91:3001/packages` — grid should list every `delivery_packages` row.
- Click one → should play a rendition, the C2PA panel should show signer = "VAST Data Media Demo", action chain, AI disclosure with model + prompt.

### B. Deploy `james-subclipper` (Phase 4)

```bash
# Build + publish the function image
cd /Users/james/projects/media/catalog/functions/foundation/subclipper
vast functions build james-subclipper -T v1
docker tag james-subclipper:v1 docker.selab.vastdata.com:5000/james/james-subclipper:v1
docker push docker.selab.vastdata.com:5000/james/james-subclipper:v1

# Register function
vast functions create \
  --name james-subclipper \
  --description "Phase 4: Direct-invoke generic subclipper. Accepts {src, clips[]}." \
  --container-registry selab-docker \
  --artifact-type image \
  --artifact-source "james/james-subclipper" \
  --image-tag v1 \
  --publish \
  --revision-description "v1: initial"

# Seed the 7 new `subclipper` config rows (idempotent)
sshpass -p vastdata scp shared/ingest/subclipper.py vastdata@10.143.11.91:/tmp/seed-cfg/shared/ingest/
sshpass -p vastdata scp scripts/seed_function_configs.py vastdata@10.143.11.91:/tmp/seed-cfg/scripts/
sshpass -p vastdata ssh vastdata@10.143.11.91 "cd /tmp/seed-cfg && python3 scripts/seed_function_configs.py"
```

Then invoke-test:
```bash
vast functions invoke james-subclipper --body '{
  "src": "s3://james-media-catalog/basketball-backyard.mp4",
  "out_bucket": "james-media-subclips",
  "out_prefix": "invoke-test/",
  "clips": [
    {"start": 0.0, "end": 3.0, "name": "first-dunk"},
    {"start": 10.0, "end": 12.5, "name": "highlight-2", "width": 1280, "height": 720, "stream_copy": false, "crf": 20}
  ]
}'
```

### C. Phase 3 hand-off to catalog pipeline (optional, not yet built)

Right now, signed renditions sit at `s3://james-media-deliveries/<package_id>/` and
never make it into the main 8-function catalog fan-out. To close the loop:

1. Add 3 columns to the `assets` schema: `source_video_id`, `clip_id`, `package_id`.
2. Patch `metadata-extractor` to read those 3 tags off each incoming object and stamp them on the assets row.
3. Have media-packager copy each rendition into `s3://james-media-catalog/` with `x-amz-meta-source-id/clip-id/package-id` set.
4. Existing trigger `james-media-catalog` fires the 8-function provenance pipeline as it does today; rows are now linked back to their pre-ingest lineage via the 3 new columns.

## Operational reminders

- All configs still point at `172.200.202.1` / bucket `james-db` / schema `media-catalog`.
- `.204` migration was considered and dropped — `.204` DB not available. No changes were made; current deployment is intact.
- Git push to a personal GitHub repo is queued (separate track, see `docs/INGEST_PIPELINE.md` current state).
- Cert for C2PA is self-signed — fine for demo. For production you'd chain to a C2PA-recognized CA so Adobe Content Credentials shows a verified signer badge.

## Known quirks

- **Knative revision caching**: `vast functions update --image-tag vN --publish` needs a brand new tag every time, else Knative reuses the last revision. Use incrementing tags (`v1`, `v2`, …) or date suffixes.
- **Knative `timeoutSeconds` max is 600** — not 900. Any higher and the pipeline deploy rejects with `validation failed: expected 0 <= 900 <= 600`.
- **Cert perms**: Paketo buildpacks ship cert files as `0600` owned by root. The non-root `cnb` runtime user can't read them. `media-packager/main.py::_fix_cert_perms()` chmods `c2pa-signing/*` to `0644` at init time — fine because the key is a self-signed dev secret.
- **x-amz-meta tag stripping**: `copy_object` with `MetadataDirective=REPLACE` wipes source metadata. `shared/ingest/s3_helpers.py::move_object` now merges existing tags with new ones (preserving `clip-prompt`, `rights-cleared-for`, etc. through the qc-inspector move).
