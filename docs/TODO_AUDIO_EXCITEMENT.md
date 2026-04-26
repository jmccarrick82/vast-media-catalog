# Audio-aware Excitement Signal — Build TODO

_Phase 2.6 of the AI clipper. Planned 2026-04-25._

## The problem

Current ai-clipper picks clips on visual content alone. On a baseball
broadcast with the prompt "an exciting baseball play," the LLM curator
correctly picks 5 clips with chronological spread — but every clip is
basically "guy in batting stance," because the vision model can't tell
a routine pitch from a walk-off home run. Both look like a swing.

Real excitement is partly **audible**:
- crowd noise (a roaring stadium vs. polite background chatter)
- announcer pitch + speech rate (calm cadence vs. shouting)
- silence followed by sudden volume spike (the moment of contact)

We need to fold an audio signal into clip selection so "swing into a
crowd roar" beats "swing into silence."

## Acceptance test

Re-run the same 10-min CWS Tennessee vs UNC source already in the
catalog (`source_id=ed82e3d2f4fde6a39a96a4036d30ef5f`). Same prompt,
same `max_clips_per_source=5`. With audio analysis enabled, the 5
selected clips should differ from today's selection — biased toward
moments where the broadcast audio is louder or the announcer is excited.
At least 2 of the 5 should fall on a moment with `audio_excitement_db`
≥ 8 dB above the source baseline.

## Build sequence

### 1. `shared/ingest/audio.py` (new module)

Pure-function library, no S3 / DB side effects. Mirrors the layout of
`scene.py` / `vision.py`.

```python
def extract_audio_features(
    src: str,
    start: float | None,
    end: float | None,
    use_whisper: bool = False,
    api_key: str | None = None,
    inference_host: str | None = None,
) -> AudioFeatures:
    """Run ffmpeg ebur128 + astats over [start, end] and return:
       AudioFeatures(
           peak_lufs: float,           # EBU R128 short-term peak
           rms_db: float,              # ffmpeg astats RMS_level
           short_term_lufs_p95: float, # 95th percentile short-term LUFS
           speech_rate_wpm: float | None,  # Whisper transcript / duration
           transcript: str | None,
           duration: float,
       )
    """
```

Implementation notes:
- For loudness: `ffmpeg -ss <start> -to <end> -i src -af ebur128=peak=true:framelog=verbose -f null -` and parse stderr for `M:` short-term LUFS lines.
- For RMS: ffmpeg `astats` filter, parse `RMS level dB`.
- For Whisper: same multipart POST that subclip-ai-analyzer's `_call_whisper` uses — lift that into a shared helper in `shared/ingest/whisper.py` (similar pattern to vision.py's `_inference_chat`).

Register config defaults at module import (same `register_defaults` pattern as the rest of `shared/ingest/`):

```python
CONFIG_SCOPE = "ai-clipper"
AUDIO_CONFIG_SCHEMA = [
    {"key": "audio_analysis_enabled", "type": "bool", "default": True,
     "group": "Audio cues", "order": 10,
     "description": "..."},
    {"key": "audio_use_whisper", "type": "bool", "default": False,
     "group": "Audio cues", "order": 20, "description": "..."},
    {"key": "audio_baseline_lufs_window_seconds",
     "type": "duration_seconds", "default": 30.0,
     "min": 5.0, "max": 600.0, "group": "Audio cues", "order": 30,
     "description": "..."},
    {"key": "audio_excitement_min_db", "type": "float", "default": 6.0,
     "min": 0.0, "max": 30.0, "group": "Audio cues", "order": 40,
     "description": "..."},
]
register_defaults(CONFIG_SCOPE, AUDIO_CONFIG_SCHEMA)
```

### 2. Schema additions in `shared/schemas.py`

Add 3 columns to `EXTRACTED_CLIPS_SCHEMA`:

```python
pa.field("audio_peak_lufs",         pa.float64()),
pa.field("audio_excitement_db",     pa.float64()),
pa.field("audio_transcript_excerpt", pa.string()),
```

`tables.upsert_extracted_clip` requires no change (it already handles arbitrary fields). `ensure_extracted_clips_table` will pick up the new columns via its existing `add_column` evolution path.

### 3. `clips.py::MatchedShot` extension

Add optional fields:

```python
@dataclass
class MatchedShot:
    start: float
    end: float
    confidence: float
    reason: str
    model: str
    audio_peak_lufs: float | None = None
    audio_excitement_db: float | None = None
    audio_transcript: str | None = None
```

`merge_matching_shots` should keep the **max** `audio_excitement_db` and the **highest-confidence** transcript across merged shots.

### 4. ai-clipper handler — call audio analysis

Between `merge_matching_shots` and `constrain_clips` (or right after constrain):

```python
if cfg.get_bool("audio_analysis_enabled"):
    # Compute baseline once over the whole source
    baseline = audio.compute_baseline_lufs(local, window=cfg.get_duration("audio_baseline_lufs_window_seconds"))
    log(f"       audio baseline: {baseline:.1f} LUFS")
    for span in candidates:
        feats = audio.extract_audio_features(
            local, start=span.start, end=span.end,
            use_whisper=cfg.get_bool("audio_use_whisper"),
            api_key=api_key,
            inference_host=...,
        )
        span.audio_peak_lufs = feats.peak_lufs
        span.audio_excitement_db = feats.short_term_lufs_p95 - baseline
        span.audio_transcript = feats.transcript
```

Then in `extracted_clips` upsert, include the 3 new fields.

### 5. `curate.py` — fold audio into the prompt

In `_build_curation_prompt`, when audio data is present, add a section per candidate:

```
Candidates (N total):
1. [span 0.0–3.5s, conf 0.90, audio +12.3 dB ↑] "swing of bat"
   transcript: "and that one is OUTTA HERE!"
2. [span 12.0–14.5s, conf 0.85, audio +1.2 dB] "..."
```

And update the guidance:

```
- Audio cues matter. A clip with audio_excitement_db >= 8 (crowd roar
  or announcer shouting) typically marks a more interesting moment
  than one with audio near baseline.
```

### 6. Re-seed configs

```bash
# After scp'ing shared/ingest/audio.py + updated seed_function_configs.py to .91:
sshpass -p vastdata ssh vastdata@10.143.11.91 "cd /tmp/seed-cfg && python3 scripts/seed_function_configs.py"
```

Should add 4 new rows under scope `ai-clipper`. Total knob count goes from 79 → 83.

### 7. Sync into ai-clipper bundle + build v8

```bash
cp shared/ingest/audio.py functions/foundation/ai-clipper/ingest/
cp shared/schemas.py functions/foundation/ai-clipper/schemas.py
cp shared/ingest/clips.py functions/foundation/ai-clipper/ingest/  # MatchedShot fields
cp shared/ingest/curate.py functions/foundation/ai-clipper/ingest/  # prompt updates

cd functions/foundation/ai-clipper
vast functions build james-ai-clipper -T v8
docker tag james-ai-clipper:v8 docker.selab.vastdata.com:5000/james/james-ai-clipper:v8
docker push docker.selab.vastdata.com:5000/james/james-ai-clipper:v8
vast functions update james-ai-clipper --image-tag v8 --publish \
  --revision-description "v8: audio-aware excitement signal in curation"
```

Update `functions/pipeline-unified.yaml` ai-clipper revision 7 → 8, deploy.

### 8. UI surfacing (optional in this phase)

Add an "audio excitement" badge to `AiClipperPage.jsx` and `PackageDetailPage.jsx` clip rows:

```jsx
{c.audio_excitement_db != null && c.audio_excitement_db >= 6 && (
  <span title={`+${c.audio_excitement_db.toFixed(1)} dB above source baseline`}>
    🔊 +{c.audio_excitement_db.toFixed(0)} dB
  </span>
)}
```

Backend `/api/sources/<id>` already returns the full `extracted_clips` row, so no API changes needed.

### 9. Smoke test + commit

- Wipe pre-ingest state.
- Re-upload the same `baseball10.mp4` from `/tmp/baseball10.mp4` with the same prompt.
- Verify the 5 selected clips differ from the today's set (`8.6s, 102s, 232s, 326s, 543s`) — bias should shift toward whichever of the 11 candidates had crowd noise.
- Confirm `audio_excitement_db` is populated on each clip's `extracted_clips` row.
- Commit + push.

## Risks / unknowns

- **ffmpeg ebur128 perf on Knative pod**: 10-min source × per-candidate scan ≈ 11 × ~5s ffmpeg passes ≈ 1 minute extra. Should fit in the existing 600s timeout. If it doesn't, parallelize with concurrent.futures + a limit of 4.
- **Whisper cost**: each candidate is ~5–30s of audio; 11 candidates × Whisper API call ≈ 30–60s extra. Not free but tolerable. Default to `audio_use_whisper=false` and let the operator opt in.
- **LLM weighting**: the curator might over-index on audio and pick all crowd-roar clips at the expense of visual variety. Mitigate by keeping the existing `curation_diversity_weight` knob and explicitly mentioning "balance audio + visual" in the prompt.

## Pickup order (if this gets split)

1 → 2 → 3 → 4 → 6 → 5 → 7 → 9 (UI step 8 is optional)

Steps 1+3+4 are the meat. 5 is the demo-defining piece (curator actually weighing audio). 7+9 close the loop and prove it.
