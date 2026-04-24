import React from "react";
import {
  Database, Cpu, Image as ImageIcon, Film, Mic, Fingerprint, Shield,
  GitBranch, Network, Sparkles, ArrowRight, Cloud, Layers, Server,
  Workflow, Eye, FileText, Scissors, Bot, Search, Boxes, Zap, Play,
  CheckCircle, XCircle, Package, ShieldCheck, Inbox, Award, Settings,
  AlertTriangle, Sliders,
} from "lucide-react";

/* ─────────────────────────────────────────────────────────────
   ArchitecturePage
   End-to-end walkthrough of the VAST Content Provenance pipeline.
   No backend calls — pure static explainer used for live demos.
───────────────────────────────────────────────────────────── */

const FOUNDATION_FUNCS = [
  {
    name: "metadata-extractor",
    icon: FileText,
    what: "Runs ffprobe on the source video to pull codec, resolution, duration, bitrate, frame rate, and any embedded custom tags (including parent_asset_id for subclips).",
    writes: "assets row: filename, duration_seconds, video_codec, width, height, fps, bitrate, format, content_type, subclip_parent_asset_id",
  },
  {
    name: "hash-generator",
    icon: Fingerprint,
    what: "Computes both SHA-256 (cryptographic identity) and perceptual hashes (for near-duplicate detection) over the video stream.",
    writes: "sha256, perceptual_hash, hash_computed_at",
  },
  {
    name: "keyframe-extractor",
    icon: ImageIcon,
    what: "Pulls up to 10 I-frames from the video via ffmpeg and uploads them as JPEGs to the key-frames bucket for downstream vision tasks.",
    writes: "keyframe_count, keyframe_s3_prefix, keyframes_extracted_at",
  },
  {
    name: "video-subclip",
    icon: Scissors,
    what: "Chops long videos into ~30-second subclips, re-embeds parent_asset_id as a custom metadata tag, and uploads each subclip to the subclips bucket. The write into the subclips bucket is what fires the second trigger.",
    writes: "subclip_count, subclip_parent_asset_id, relationships rows (PARENT_OF)",
  },
  {
    name: "audio-analyzer",
    icon: Mic,
    what: "Extracts a 30-second audio segment once, then runs language detection and music fingerprinting against it.",
    writes: "music_detected, audio_fingerprint, talent_music_scanned_at",
  },
];

const ANALYSIS_FUNCS = [
  {
    name: "synthetic-detector",
    icon: Bot,
    what: "Deep metadata scan for AI-generation tells (tool signatures, C2PA manifests, model fingerprints in container tags).",
    writes: "ai_probability, ai_tool_detected, ai_model_version, ai_detection_method",
  },
  {
    name: "hash-comparator",
    icon: GitBranch,
    what: "Compares the asset's cryptographic and perceptual hashes against every other asset in the catalog to find exact and near-duplicates. Writes matches into the relationships and hash_matches tables.",
    writes: "relationships (DUPLICATE_OF, NEAR_DUPLICATE_OF), hash_matches rows",
  },
  {
    name: "graph-analyzer",
    icon: Network,
    what: "Runs all 16 graph-based analyses in one pass — version tracking, syndication chains, production entity clustering, GDPR personal-data flags, and more.",
    writes: "version_history, syndication_records, production_entities, gdpr_personal_data, asset_moves",
  },
];

const SUBCLIP_STEPS = [
  { n: 1, title: "Fetch subclip",     detail: "Download the MP4 from the subclips bucket to local scratch." },
  { n: 2, title: "Parent lookup",     detail: "Query the assets table for the parent's metadata so downstream context is available." },
  { n: 3, title: "Extract media",     detail: "Pull one keyframe JPEG and a 30-second WAV audio segment via ffmpeg." },
  { n: 4, title: "Whisper transcript", detail: "Post the WAV to Whisper-large-v3 for speech-to-text." },
  { n: 5, title: "Vision 90B — OCR",   detail: "Llama-3.2-90B-Vision reads any on-screen text in the keyframe." },
  { n: 6, title: "Vision 90B — scene", detail: "Second 90B-Vision call: rich scene description (setting, subjects, mood)." },
  { n: 7, title: "Vision 11B — AI",    detail: "Smaller 11B-Vision pass screens the keyframe for synthetic/AI-generated tells." },
  { n: 8, title: "Llama-Guard",        detail: "Content-safety model rates the combined transcript + OCR + scene text." },
  { n: 9, title: "Llama-3.3-70B",      detail: "Final composite pass: category, mood, rating, summary, tags, keywords. Swapped down from 405B — a 70B model does this job just as well and runs 3–5× faster." },
  { n: 10, title: "Embed + Qdrant",    detail: "Concatenate summary + scene + OCR + transcript + keywords into a single passage, POST to /v1/embeddings (nvidia/nv-embed-v1, 4096-dim — NVIDIA's flagship retriever), upsert as a point in the `subclips` collection keyed by asset_id. Powers /search in the webapp." },
];

const SEARCH_STEPS = [
  { n: 1, title: "User types a query", detail: "Natural-language phrase typed into /search — e.g. \"narrator speaking about history\"." },
  { n: 2, title: "Webapp → /v1/embeddings", detail: "Flask backend forwards the query to the same inference endpoint used at ingest (same model — nvidia/nv-embed-v1 — with input_type=query for asymmetric retrieval)." },
  { n: 3, title: "Qdrant cosine search", detail: "4096-dim query vector runs against the `subclips` collection — top-K by cosine similarity." },
  { n: 4, title: "Hit payload", detail: "Each point carries asset_id, s3_path, summary/category/mood/rating/keywords, and the exact passage that was embedded — no extra DB round-trip needed." },
  { n: 5, title: "Inline playback", detail: "React renders each hit with an embedded <video> tag pointing at /api/video?path=…, a Range-enabled S3 proxy so seeking works over the internal VAST endpoint." },
];

const BUCKETS = [
  { name: "james-media-inbox",       purpose: "Raw uploads land here — triggers QC inspector." },
  { name: "james-media-qc-passed",   purpose: "QC-approved source videos — triggers AI clipper." },
  { name: "james-media-qc-failed",   purpose: "QC-rejected uploads (quarantine)." },
  { name: "james-media-clips",       purpose: "AI-extracted raw clips + _ready.json markers — triggers packager." },
  { name: "james-media-deliveries",  purpose: "C2PA-signed delivery bundles: renditions + thumbnails + manifest.json." },
  { name: "james-media-catalog",     purpose: "Legacy/direct-upload source bucket — triggers the 8-function provenance fan-out." },
  { name: "james-media-subclips",    purpose: "Generated ~30s subclips — triggers the subclip AI analyzer." },
  { name: "james-key-frames",        purpose: "Extracted JPEG I-frames used by vision models." },
  { name: "james-db",                purpose: "VAST DB — holds every structured table below." },
  { name: "qdrant (container)",      purpose: "Vector DB — subclip text embeddings for /search." },
];

const TABLES = [
  // Pre-ingest (Phase 1/2/3)
  "source_videos",          // one row per raw upload, updated through every stage
  "extracted_clips",        // AI-selected clip timestamps + vision verdicts
  "delivery_packages",      // one row per packaged delivery bundle
  "package_renditions",     // per-rendition rows with C2PA manifest labels
  "function_configs",       // runtime-editable knobs shared across all functions
  // Main catalog (8-function fan-out)
  "assets", "relationships", "hash_matches", "talent_music",
  "semantic_embeddings", "gdpr_personal_data", "syndication_records",
  "production_entities", "version_history", "asset_moves",
];

const QC_CHECKS = [
  { name: "ffprobe",       detail: "Duration, codec, resolution, fps, pixel format, audio channels/sample rate — every QC decision needs the probed metadata." },
  { name: "Black frames",  detail: "ffmpeg blackdetect — flags prolonged black sequences; warn > 10%, fail > 50% by default." },
  { name: "Freeze frames", detail: "ffmpeg freezedetect — same idea for frozen/static video." },
  { name: "Silence",       detail: "ffmpeg silencedetect — configurable dB threshold + min-run; warn > 25%, fail > 95%." },
  { name: "Loudness",      detail: "ffmpeg ebur128 — integrated LUFS + true-peak dBTP. Fails if peak > -1 dBTP (clipping) or LUFS < -30 (too quiet)." },
  { name: "VFR detection", detail: "Samples 500 frame intervals via ffprobe — flags variable frame rate (breaks many downstream tools)." },
  { name: "Interlaced",    detail: "ffmpeg idet — flags sources that need deinterlacing before further processing." },
  { name: "Resolution",    detail: "Policy gate: source must be ≥ min_video_width × min_video_height (default 640×360)." },
  { name: "Codec allow",   detail: "Policy gate: video codec ∈ {h264, hevc, vp9, av1}, audio codec in configured set." },
];

const PACKAGER_STEPS = [
  { n: 1, title: "Read _ready.json",        detail: "Marker emitted by ai-clipper lists the source_id + clip_ids. Packager skips any non-marker PUT in the clips bucket." },
  { n: 2, title: "Load source + clip rows", detail: "Fetches source_videos + extracted_clips from VAST DB for context. Uploader S3 metadata (rights-cleared-for, restrictions, clearance-days) overrides config defaults." },
  { n: 3, title: "Transcode renditions",    detail: "For each clip: ffmpeg produces every preset from function_configs (e.g. h264-1080p, h264-720p, proxy-360p, hevc-4k-if-source-supports)." },
  { n: 4, title: "Extract thumbnail",       detail: "One JPEG per clip via ffmpeg — mid-frame, scaled to configured max width." },
  { n: 5, title: "C2PA sign each rendition", detail: "c2patool embeds a signed manifest into every rendition MP4 — actions chain (created/placed/edited), ingredients link back to source, AI disclosure with model+prompt, training-mining flags, BMFF hash for tamper-evidence." },
  { n: 6, title: "Build sidecar manifest",  detail: "manifest.json aggregates the whole bundle (clips, renditions, thumbnails, licensing) — belt-and-suspenders with C2PA and covers formats that can't embed." },
  { n: 7, title: "Upload + write DB rows",  detail: "delivery_packages + package_renditions rows capture every c2pa_manifest_label, signer, and file checksum. Package goes to s3://james-media-deliveries/<package_id>/." },
];

const C2PA_ASSERTIONS = [
  { label: "c2pa.actions.v2",             what: "The lineage chain: created (source captured) → placed (AI-selected clip span) → edited (transcoded to rendition)." },
  { label: "c2pa.creative_work",          what: "Source attribution + author metadata (schema.org CreativeWork)." },
  { label: "c2pa.training-mining",        what: "Whether this content is allowed for AI training, generative training, data mining, and inference. Default: all notAllowed." },
  { label: "com.vast.ai_clip_selection",  what: "VAST-custom: vision model name, exact prompt, match confidence, source timespan. The key regulatory AI-disclosure signal." },
  { label: "c2pa.hash.bmff.v2",           what: "Auto-generated: cryptographic hash over the MP4 container structure. Any byte-level edit invalidates the signature." },
];

/* ─────────────────────────────────────────────────────────── */

function Section({ icon: Icon, title, subtitle, children }) {
  return (
    <section style={{ marginTop: 40 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 6 }}>
        {Icon && (
          <div style={{
            width: 36, height: 36, borderRadius: 10,
            background: "var(--vast-blue-dim)",
            display: "flex", alignItems: "center", justifyContent: "center",
            color: "var(--vast-blue)",
          }}>
            <Icon size={18} />
          </div>
        )}
        <h2 style={{ fontSize: 20, fontWeight: 700, letterSpacing: "-0.3px" }}>{title}</h2>
      </div>
      {subtitle && (
        <p style={{ color: "var(--text-dim)", fontSize: 14, marginLeft: 48, marginBottom: 18 }}>
          {subtitle}
        </p>
      )}
      {children}
    </section>
  );
}

function FlowArrow() {
  return (
    <div style={{
      display: "flex", alignItems: "center", justifyContent: "center",
      color: "var(--vast-blue)", padding: "4px 0",
    }}>
      <ArrowRight size={18} />
    </div>
  );
}

function Pipe({ children }) {
  return (
    <div style={{
      position: "relative",
      borderLeft: "2px dashed var(--vast-blue-dim)",
      marginLeft: 18, paddingLeft: 22, paddingBottom: 4,
    }}>
      {children}
    </div>
  );
}

function FuncCard({ fn }) {
  const Icon = fn.icon;
  return (
    <div style={{
      background: "var(--surface)",
      border: "1px solid var(--border)",
      borderRadius: 14,
      padding: 18,
      display: "flex",
      gap: 14,
    }}>
      <div style={{
        flexShrink: 0,
        width: 40, height: 40, borderRadius: 10,
        background: "var(--vast-blue-dim)",
        display: "flex", alignItems: "center", justifyContent: "center",
        color: "var(--vast-blue)",
      }}>
        <Icon size={20} />
      </div>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{
          fontFamily: "SF Mono, Menlo, monospace",
          fontSize: 13, fontWeight: 700,
          color: "var(--vast-blue)", marginBottom: 6,
        }}>
          {fn.name}
        </div>
        <div style={{ fontSize: 13, color: "var(--text)", lineHeight: 1.5 }}>
          {fn.what}
        </div>
        <div style={{
          fontSize: 11, color: "var(--text-dim)", marginTop: 8,
          fontFamily: "SF Mono, Menlo, monospace",
        }}>
          writes → {fn.writes}
        </div>
      </div>
    </div>
  );
}

function ScopeRow({ scope, count, what }) {
  return (
    <div style={{
      display: "grid", gridTemplateColumns: "140px 40px 1fr", gap: 8,
      padding: "6px 10px", borderRadius: 6,
      background: "var(--vast-dark)",
      border: "1px solid var(--border)",
      alignItems: "center",
    }}>
      <code style={{ color: "var(--vast-blue)", fontFamily: "SF Mono, Menlo, monospace", fontSize: 12 }}>
        {scope}
      </code>
      <span style={{ textAlign: "right", color: "var(--text-dim)", fontSize: 11 }}>
        {count}
      </span>
      <span style={{ color: "var(--text-dim)", fontSize: 11, lineHeight: 1.4 }}>
        {what}
      </span>
    </div>
  );
}

function StageBox({ icon: Icon, label, sublabel, tone = "default" }) {
  const bg = tone === "blue" ? "var(--vast-blue-dim)" : "var(--surface)";
  const bc = tone === "blue" ? "var(--vast-blue)" : "var(--border)";
  return (
    <div style={{
      background: bg,
      border: `1px solid ${bc}`,
      borderRadius: 12,
      padding: "14px 18px",
      display: "flex", alignItems: "center", gap: 12,
      minWidth: 220,
    }}>
      {Icon && <Icon size={18} color="var(--vast-blue)" />}
      <div>
        <div style={{ fontSize: 13, fontWeight: 600, color: "var(--text)" }}>{label}</div>
        {sublabel && (
          <div style={{ fontSize: 11, color: "var(--text-dim)", marginTop: 2 }}>{sublabel}</div>
        )}
      </div>
    </div>
  );
}

export default function ArchitecturePage() {
  return (
    <>
      <div className="page-header">
        <h1>Architecture &amp; Workflow</h1>
        <p>
          How an uploaded video travels through the VAST DataEngine pipeline and lands
          as a fully-enriched, queryable record in the catalog.
        </p>
      </div>

      {/* High-level flow */}
      <Section icon={Workflow} title="End-to-end flow">
        <div style={{
          background: "var(--surface)",
          border: "1px solid var(--border)",
          borderRadius: 16,
          padding: 24,
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          gap: 8,
        }}>
          <StageBox icon={Inbox} label="Upload" sublabel="s3://james-media-inbox/ + optional x-amz-meta-* tags" tone="blue" />
          <FlowArrow />
          <StageBox
            icon={CheckCircle}
            label="Phase 1 — QC Inspector"
            sublabel="ffprobe + 9 non-AI detectors → passed / warn / failed"
          />
          <FlowArrow />
          <StageBox
            icon={Scissors}
            label="Phase 2 — AI Clipper"
            sublabel="ffmpeg scene detection + 11B/90B vision classifier → extracted_clips"
          />
          <FlowArrow />
          <StageBox
            icon={Package}
            label="Phase 3 — Media Packager"
            sublabel="transcode → C2PA sign every rendition → JSON manifest"
            tone="blue"
          />
          <FlowArrow />
          <StageBox
            icon={Sparkles}
            label="Hand-off → catalog fan-out"
            sublabel="james-media-catalog event fires the existing 8-function provenance pipeline"
          />
          <FlowArrow />
          <StageBox
            icon={Cpu}
            label="5 foundation + 3 analysis + subclip AI"
            sublabel="metadata, hashes, keyframes, subclips, audio, synthetic-detect, graph, hash-compare, embed"
          />
          <FlowArrow />
          <StageBox icon={Database} label="VAST DB + Qdrant" sublabel="source_videos + extracted_clips + delivery_packages + package_renditions + assets + subclip embeddings" tone="blue" />
          <FlowArrow />
          <StageBox icon={Server} label="Trino + webapp" sublabel="/packages, /search, /settings — React over Flask over Trino over VAST DB" />
        </div>
        <p style={{ color: "var(--text-dim)", fontSize: 13, marginTop: 12 }}>
          The pre-ingest stages (Phase 1–3) are a <strong>second pipeline</strong> that
          runs before the existing 8-function provenance fan-out. They operate on raw
          uploads, quality-control them, extract AI-selected clips, and emit C2PA-signed
          delivery bundles. The signed renditions then land in{" "}
          <code style={{ color: "var(--vast-blue)" }}>s3://james-media-catalog/</code> with{" "}
          <code>x-amz-meta-source-id</code> / <code>clip-id</code> / <code>package-id</code>{" "}
          tags so the existing pipeline can stitch the catalog row back to the
          source / clip / package that produced it. No orchestrator, no shared state — every
          function is kicked off by the S3 event from the previous stage's output.
        </p>
      </Section>

      {/* Phase 1: QC inspector */}
      <Section
        icon={CheckCircle}
        title="Phase 1 — QC Inspector"
        subtitle={
          <>
            Audit every upload before it enters the pipeline. Runs 9 non-AI detectors via ffmpeg/ffprobe,
            applies a policy, and routes the file to <code>qc-passed</code> or <code>qc-failed</code> with
            a full result row in <code>source_videos</code>. Every threshold is editable at runtime via{" "}
            <code>/settings</code> — no redeploys.
          </>
        }
      >
        <div style={{
          background: "var(--surface)", border: "1px solid var(--border)",
          borderRadius: 16, padding: 20,
        }}>
          <div style={{
            display: "flex", alignItems: "center", gap: 10, marginBottom: 18, flexWrap: "wrap",
          }}>
            <Cloud size={16} color="var(--vast-blue)" />
            <span style={{ fontSize: 13, color: "var(--text-dim)" }}>
              <strong style={{ color: "var(--vast-blue)" }}>james-media-inbox</strong> write
              → <strong style={{ color: "var(--vast-blue)" }}>james-inbox-trigger</strong>
              → <strong style={{ color: "var(--vast-blue)" }}>james-qc-inspector</strong>
            </span>
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))", gap: 10 }}>
            {QC_CHECKS.map((q) => (
              <div key={q.name} style={{
                background: "var(--vast-dark)",
                border: "1px solid var(--border)",
                borderRadius: 10, padding: 12,
              }}>
                <div style={{ fontSize: 13, fontWeight: 600, color: "var(--text)", marginBottom: 4 }}>
                  {q.name}
                </div>
                <div style={{ fontSize: 12, color: "var(--text-dim)", lineHeight: 1.5 }}>
                  {q.detail}
                </div>
              </div>
            ))}
          </div>
          <div style={{
            display: "flex", gap: 10, marginTop: 14, justifyContent: "center", alignItems: "center",
          }}>
            <StageBox icon={CheckCircle} label="passed / warn" sublabel="→ s3://james-media-qc-passed/" tone="blue" />
            <ArrowRight size={18} color="var(--vast-blue)" />
            <StageBox icon={Scissors} label="AI clipper" sublabel="Phase 2 fires" />
          </div>
          <div style={{
            display: "flex", gap: 10, marginTop: 10, justifyContent: "center", alignItems: "center",
          }}>
            <StageBox icon={XCircle} label="failed" sublabel="→ s3://james-media-qc-failed/ (quarantine)" />
          </div>
          <div style={{
            marginTop: 12, padding: 12,
            background: "var(--vast-dark)",
            border: "1px dashed var(--border)",
            borderRadius: 10,
            fontSize: 12, color: "var(--text-dim)",
          }}>
            <strong style={{ color: "var(--vast-blue)" }}>Observability tricks:</strong>{" "}
            the handler writes progressive checkpoint markers to the{" "}
            <code style={{ color: "var(--vast-blue)" }}>source_videos</code> row —{" "}
            <code>pending:seeded → pending:downloaded → pending:probed → pending:black_starting → pending:black_done → …</code>{" "}
            — so a stuck run shows exactly which detector is hung, even without tail-following logs.
            All ffmpeg/ffprobe subprocess calls use <code>-nostdin</code> + <code>stdin=DEVNULL</code> + <code>timeout=120</code>{" "}
            so a bad file can't wedge the pod.
          </div>
        </div>
      </Section>

      {/* Phase 2: AI clipper */}
      <Section
        icon={Scissors}
        title="Phase 2 — AI Clipper"
        subtitle={
          <>
            Given a natural-language prompt (from an <code>x-amz-meta-clip-prompt</code> tag on the
            upload, or a config default), extract the spans of the video that match it. ffmpeg detects
            scene boundaries, a vision model classifies each shot, adjacent matching shots get merged,
            and the result is cut to <code>s3://james-media-clips/&lt;source_id&gt;/</code>.
          </>
        }
      >
        <div style={{
          background: "var(--surface)", border: "1px solid var(--border)",
          borderRadius: 16, padding: 20,
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 18 }}>
            <Cloud size={16} color="var(--vast-blue)" />
            <span style={{ fontSize: 13, color: "var(--text-dim)" }}>
              <strong style={{ color: "var(--vast-blue)" }}>james-media-qc-passed</strong> write
              → <strong style={{ color: "var(--vast-blue)" }}>james-qc-passed-trigger</strong>
              → <strong style={{ color: "var(--vast-blue)" }}>james-ai-clipper</strong>
            </span>
          </div>
          <Pipe>
            {[
              { n: 1, title: "Resolve prompt",  detail: "x-amz-meta-clip-prompt → sidecar JSON → ai-clipper:default_clip_prompt config. Prompt source recorded in extracted_clips.prompt_source." },
              { n: 2, title: "Detect shots",    detail: "ffmpeg scene filter with configurable threshold. Min/max shot length enforced — long static takes get split, rapid cuts get merged forward." },
              { n: 3, title: "Classify each",   detail: "Extract mid-frame JPEG of each shot. Call 11B vision model (primary). Escalate borderline-confidence shots (0.35 ≤ conf < 0.75) to 90B. Model responds with structured JSON: {match, confidence, reason}." },
              { n: 4, title: "Merge + constrain", detail: "Adjacent matching shots within merge_gap_seconds collapse into one clip span. Constrain to min_clip_seconds / max_clip_seconds / max_clips_per_source (all editable)." },
              { n: 5, title: "Cut + upload",    detail: "ffmpeg -c copy (configurable — stream copy is fast but snaps to keyframes; disable to force frame-exact re-encode). Each clip uploaded with source-id / prompt / confidence as S3 metadata." },
              { n: 6, title: "Emit _ready.json", detail: "Write a sentinel JSON to s3://james-media-clips/<source_id>/_ready.json — this PUT is what fires Phase 3. Packager skips any other key in the bucket." },
            ].map((s) => (
              <div key={s.n} style={{ position: "relative", marginBottom: 14 }}>
                <div style={{
                  position: "absolute", left: -34, top: 2,
                  width: 24, height: 24, borderRadius: "50%",
                  background: "var(--vast-blue)", color: "var(--vast-dark)",
                  display: "flex", alignItems: "center", justifyContent: "center",
                  fontSize: 11, fontWeight: 700,
                }}>{s.n}</div>
                <div style={{ fontSize: 13, fontWeight: 600, color: "var(--text)" }}>{s.title}</div>
                <div style={{ fontSize: 12, color: "var(--text-dim)", marginTop: 2 }}>{s.detail}</div>
              </div>
            ))}
          </Pipe>
          <div style={{
            marginTop: 10, padding: 12,
            background: "var(--vast-dark)",
            border: "1px dashed var(--border)",
            borderRadius: 10,
            fontSize: 12, color: "var(--text-dim)",
          }}>
            <strong style={{ color: "var(--vast-blue)" }}>Two-tier model strategy:</strong>{" "}
            the cheap 11B vision model handles every shot. If its confidence lands in the
            "borderline" band (low ≤ conf &lt; high, configurable), the frame is re-sent to
            the 90B model for a second opinion — the escalated verdict wins. If escalation
            fails (timeout, 5xx), the primary verdict is used. Set primary = escalation to
            disable escalation entirely.
          </div>
        </div>
      </Section>

      {/* Phase 3: Media packager + C2PA — the headline */}
      <Section
        icon={Package}
        title="Phase 3 — Media Packager + C2PA Provenance"
        subtitle={
          <>
            Transcode each clip into every configured rendition preset,{" "}
            <strong style={{ color: "var(--vast-blue)" }}>
              cryptographically sign every output with an embedded C2PA manifest
            </strong>
            , build a JSON sidecar manifest with licensing + lineage, and upload the whole
            bundle to <code>s3://james-media-deliveries/&lt;package_id&gt;/</code>. This is
            the regulatory-compliant deliverable — AI disclosure, tamper-evidence, and
            attribution all live inside the MP4 file itself, not in a database.
          </>
        }
      >
        <div style={{
          background: "var(--surface)", border: "1px solid var(--border)",
          borderRadius: 16, padding: 20,
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 18 }}>
            <Cloud size={16} color="var(--vast-blue)" />
            <span style={{ fontSize: 13, color: "var(--text-dim)" }}>
              <strong style={{ color: "var(--vast-blue)" }}>james-media-clips/…/_ready.json</strong> write
              → <strong style={{ color: "var(--vast-blue)" }}>james-clips-ready-trigger</strong>
              → <strong style={{ color: "var(--vast-blue)" }}>james-media-packager</strong>
            </span>
          </div>
          <Pipe>
            {PACKAGER_STEPS.map((s) => (
              <div key={s.n} style={{ position: "relative", marginBottom: 14 }}>
                <div style={{
                  position: "absolute", left: -34, top: 2,
                  width: 24, height: 24, borderRadius: "50%",
                  background: "var(--vast-blue)", color: "var(--vast-dark)",
                  display: "flex", alignItems: "center", justifyContent: "center",
                  fontSize: 11, fontWeight: 700,
                }}>{s.n}</div>
                <div style={{ fontSize: 13, fontWeight: 600, color: "var(--text)" }}>{s.title}</div>
                <div style={{ fontSize: 12, color: "var(--text-dim)", marginTop: 2 }}>{s.detail}</div>
              </div>
            ))}
          </Pipe>

          <div style={{
            marginTop: 16, padding: 16,
            background: "var(--vast-blue-dim)",
            border: "1px solid var(--vast-blue)",
            borderRadius: 12,
          }}>
            <div style={{
              display: "flex", alignItems: "center", gap: 8, marginBottom: 12,
            }}>
              <ShieldCheck size={18} color="var(--vast-blue)" />
              <strong style={{ fontSize: 14, color: "var(--vast-blue)" }}>
                Every rendition carries 5 C2PA assertions
              </strong>
            </div>
            <div style={{ display: "grid", gap: 8 }}>
              {C2PA_ASSERTIONS.map((a) => (
                <div key={a.label} style={{
                  display: "grid", gridTemplateColumns: "260px 1fr", gap: 12,
                  fontSize: 12, lineHeight: 1.5,
                }}>
                  <code style={{
                    color: "var(--vast-blue)",
                    fontFamily: "SF Mono, Menlo, monospace",
                    fontSize: 12,
                  }}>{a.label}</code>
                  <span style={{ color: "var(--text-dim)" }}>{a.what}</span>
                </div>
              ))}
            </div>
            <div style={{
              marginTop: 14, paddingTop: 12,
              borderTop: "1px dashed var(--vast-blue)",
              fontSize: 12, color: "var(--text-dim)",
            }}>
              Signed with a self-signed ES256 X.509 cert (demo). In production the cert
              would chain to a C2PA-recognized CA for universal verifier trust. Files verify
              today in Adobe Premiere's Content Credentials panel, Leica camera readers,
              c2patool CLI, and{" "}
              <a href="https://contentcredentials.org/verify" target="_blank" rel="noreferrer"
                 style={{ color: "var(--vast-blue)" }}>
                contentcredentials.org/verify
              </a>. The <code>/packages/&lt;id&gt;</code> page in this webapp re-runs
              c2patool live against each rendition so you can inspect the full manifest
              tree without downloading anything.
            </div>
          </div>

          <div style={{
            marginTop: 12, padding: 12,
            background: "var(--vast-dark)",
            border: "1px dashed var(--border)",
            borderRadius: 10,
            fontSize: 12, color: "var(--text-dim)",
          }}>
            <strong style={{ color: "var(--vast-blue)" }}>Why this matters:</strong>{" "}
            regulatory AI-disclosure is tightening fast (EU AI Act, California AB 942, UK AI
            White Paper, Adobe Stock / Getty policies). C2PA is the converging answer:
            machine-readable disclosure baked into the file. Our pipeline emitting{" "}
            <code>com.vast.ai_clip_selection</code> with model + prompt + confidence +
            timespan is exactly what those regs ask for. Media &amp; entertainment
            customers actively RFP for this today.
          </div>
        </div>
      </Section>

      {/* Config system */}
      <Section
        icon={Sliders}
        title="Runtime configuration"
        subtitle={
          <>
            Every threshold, preset, model choice, and licensing default across the pre-ingest
            pipeline lives in one VAST DB table (<code>function_configs</code>) and is editable
            in the webapp at <code>/settings</code>. Functions read from the table with a
            60-second per-pod cache, so edits take effect on the next handler invocation
            without a redeploy.
          </>
        }
      >
        <div style={{
          background: "var(--surface)", border: "1px solid var(--border)",
          borderRadius: 16, padding: 20,
          display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20,
        }}>
          <div>
            <div style={{ fontSize: 12, fontWeight: 700, textTransform: "uppercase", letterSpacing: 0.5, color: "var(--vast-blue)", marginBottom: 10 }}>
              Config scopes
            </div>
            <div style={{ display: "grid", gap: 6, fontSize: 12 }}>
              <ScopeRow scope="qc-inspector" count={27} what="Thresholds, min run lengths, codec allow-lists, policy gates" />
              <ScopeRow scope="ai-clipper"   count={16} what="Scene threshold, vision models, escalation band, prompt default, clip assembly" />
              <ScopeRow scope="packager"     count={12} what="Rendition presets, thumbnail settings, licensing defaults" />
              <ScopeRow scope="provenance"   count={10} what="C2PA signing: cert paths, algorithm, claim generator, AI disclosure toggle, training-mining flags" />
            </div>
            <div style={{ fontSize: 12, color: "var(--text-dim)", marginTop: 10 }}>
              <strong style={{ color: "var(--vast-blue)" }}>65 knobs total</strong> — declared
              alongside the library code that uses them via{" "}
              <code>register_defaults(scope, schema)</code>, seeded by a single idempotent
              script (<code>scripts/seed_function_configs.py</code>).
            </div>
          </div>
          <div>
            <div style={{ fontSize: 12, fontWeight: 700, textTransform: "uppercase", letterSpacing: 0.5, color: "var(--vast-blue)", marginBottom: 10 }}>
              Settings UI (/settings)
            </div>
            <div style={{
              background: "var(--vast-dark)", border: "1px solid var(--border)",
              borderRadius: 10, padding: 12, fontSize: 12, color: "var(--text-dim)",
              lineHeight: 1.6,
            }}>
              Schema-driven editor: the page renders widgets per <code>value_type</code>
              (bool → toggle, duration_seconds → number + "s", percent → 0-100 slider with
              % suffix stored as 0-1, json → textarea with syntax validation). Each row
              shows current vs. factory default with an individual Reset button. Top bar has{" "}
              <strong>Update</strong> (bulk-apply only the rows you edited) and{" "}
              <strong>Restore defaults</strong> (scope-wide reset to seed). Backend uses a
              single transaction for bulk updates.
            </div>
          </div>
        </div>
      </Section>

      {/* Foundation layer */}
      <Section
        icon={Layers}
        title="Layer 1 — Foundation functions"
        subtitle="Run directly off the source-video S3 event. Each one extracts something primary (metadata, hashes, keyframes, subclips, audio) and upserts it into the assets row."
      >
        <div style={{ display: "grid", gap: 12 }}>
          {FOUNDATION_FUNCS.map((fn) => (
            <FuncCard key={fn.name} fn={fn} />
          ))}
        </div>
      </Section>

      {/* Analysis layer */}
      <Section
        icon={Eye}
        title="Layer 2 — Analysis functions"
        subtitle="Also fan out from the same source event, but do cross-asset or interpretive work on top of the foundation fields."
      >
        <div style={{ display: "grid", gap: 12 }}>
          {ANALYSIS_FUNCS.map((fn) => (
            <FuncCard key={fn.name} fn={fn} />
          ))}
        </div>
      </Section>

      {/* Subclip AI pipeline */}
      <Section
        icon={Film}
        title="Subclip AI pipeline"
        subtitle="A second trigger fires on every object written into the subclips bucket. That one trigger runs subclip-ai-analyzer, which chains 6 inference calls against the shared endpoint, then embeds the combined output and writes a point into Qdrant (capped at 2 concurrent instances so it doesn't flood the endpoint)."
      >
        <div style={{
          background: "var(--surface)",
          border: "1px solid var(--border)",
          borderRadius: 16,
          padding: 20,
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 18 }}>
            <Cloud size={16} color="var(--vast-blue)" />
            <span style={{ fontSize: 13, color: "var(--text-dim)" }}>
              <strong style={{ color: "var(--vast-blue)" }}>james-media-subclips</strong> write
              → <strong style={{ color: "var(--vast-blue)" }}>james-subclips-trigger</strong>
              → <strong style={{ color: "var(--vast-blue)" }}>subclip-ai-analyzer</strong>
            </span>
          </div>
          <Pipe>
            {SUBCLIP_STEPS.map((s) => (
              <div key={s.n} style={{
                position: "relative",
                marginBottom: 14,
              }}>
                <div style={{
                  position: "absolute",
                  left: -34, top: 2,
                  width: 24, height: 24, borderRadius: "50%",
                  background: "var(--vast-blue)",
                  color: "var(--vast-dark)",
                  display: "flex", alignItems: "center", justifyContent: "center",
                  fontSize: 11, fontWeight: 700,
                }}>
                  {s.n}
                </div>
                <div style={{ fontSize: 13, fontWeight: 600, color: "var(--text)" }}>
                  {s.title}
                </div>
                <div style={{ fontSize: 12, color: "var(--text-dim)", marginTop: 2 }}>
                  {s.detail}
                </div>
              </div>
            ))}
          </Pipe>
          <div style={{
            marginTop: 10, padding: 12,
            background: "var(--vast-dark)",
            border: "1px dashed var(--border)",
            borderRadius: 10,
            fontSize: 12, color: "var(--text-dim)",
          }}>
            <strong style={{ color: "var(--vast-blue)" }}>Reliability &amp; observability:</strong>{" "}
            every inference call is wrapped by{" "}
            <code style={{ color: "var(--vast-blue)" }}>_call_with_retry_and_timing()</code>:{" "}
            <strong>6 attempts</strong> with exponential backoff
            {" "}<code>[5, 15, 30, 60, 120]s</code>{" "}+ jitter so parallel subclip workers don't
            retry in lockstep. Each call emits a timing log under the{" "}
            <code style={{ color: "var(--vast-blue)" }}>[timing]</code> prefix showing
            model, attempt, latency, and cumulative elapsed — <code>grep [timing]</code>{" "}
            the function logs for a CSV-ish latency-per-step view. 2-second pacing between
            steps so bursts don't slam the endpoint. Function timeout 600s covers the full
            retry budget (~230s per call × worst case).
          </div>
        </div>
      </Section>

      {/* Semantic search */}
      <Section
        icon={Search}
        title="Semantic search"
        subtitle="The final step of subclip-ai-analyzer embeds the combined transcript + OCR + scene description + summary and upserts a point into Qdrant. The /search page reverses that flow — embed the query with the same model, cosine-search Qdrant, render hits with inline video."
      >
        {/* Two-lane diagram: write path on left, read path on right */}
        <div style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: 20,
          marginBottom: 16,
        }}>
          {/* WRITE path */}
          <div style={{
            background: "var(--surface)",
            border: "1px solid var(--border)",
            borderRadius: 16,
            padding: 20,
          }}>
            <div style={{
              fontSize: 11, fontWeight: 700, textTransform: "uppercase",
              letterSpacing: 0.5, color: "var(--vast-blue)", marginBottom: 14,
              display: "flex", alignItems: "center", gap: 8,
            }}>
              <Zap size={14} /> Write path — at ingest
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
              <StageBox icon={Bot} label="subclip-ai-analyzer" sublabel="steps 4–9 populate results" />
              <FlowArrow />
              <StageBox label="Build passage" sublabel="summary + scene + OCR + transcript + kw" />
              <FlowArrow />
              <StageBox
                icon={Cpu}
                label="/v1/embeddings"
                sublabel="nvidia/nv-embed-v1 · input_type=passage"
                tone="blue"
              />
              <FlowArrow />
              <StageBox
                icon={Boxes}
                label="Qdrant upsert"
                sublabel="collection=subclips · id=asset_id · dim=4096 · cosine"
                tone="blue"
              />
            </div>
          </div>

          {/* READ path */}
          <div style={{
            background: "var(--surface)",
            border: "1px solid var(--border)",
            borderRadius: 16,
            padding: 20,
          }}>
            <div style={{
              fontSize: 11, fontWeight: 700, textTransform: "uppercase",
              letterSpacing: 0.5, color: "var(--vast-blue)", marginBottom: 14,
              display: "flex", alignItems: "center", gap: 8,
            }}>
              <Search size={14} /> Read path — at query time
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
              <StageBox icon={Search} label="/search UI" sublabel="natural-language query" />
              <FlowArrow />
              <StageBox
                label="/api/semantic-search"
                sublabel="Flask handler embeds + queries"
              />
              <FlowArrow />
              <StageBox
                icon={Cpu}
                label="/v1/embeddings"
                sublabel="same model · input_type=query"
                tone="blue"
              />
              <FlowArrow />
              <StageBox
                icon={Boxes}
                label="Qdrant cosine search"
                sublabel="top-K with payload"
                tone="blue"
              />
              <FlowArrow />
              <StageBox
                icon={Play}
                label="Inline video"
                sublabel="/api/video range-proxy · <video> tag"
              />
            </div>
          </div>
        </div>

        {/* Step-by-step details for the query path */}
        <div style={{
          background: "var(--surface)",
          border: "1px solid var(--border)",
          borderRadius: 16,
          padding: 20,
        }}>
          <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 14 }}>
            Query flow detail
          </div>
          <Pipe>
            {SEARCH_STEPS.map((s) => (
              <div key={s.n} style={{ position: "relative", marginBottom: 14 }}>
                <div style={{
                  position: "absolute",
                  left: -34, top: 2,
                  width: 24, height: 24, borderRadius: "50%",
                  background: "var(--vast-blue)",
                  color: "var(--vast-dark)",
                  display: "flex", alignItems: "center", justifyContent: "center",
                  fontSize: 11, fontWeight: 700,
                }}>
                  {s.n}
                </div>
                <div style={{ fontSize: 13, fontWeight: 600, color: "var(--text)" }}>
                  {s.title}
                </div>
                <div style={{ fontSize: 12, color: "var(--text-dim)", marginTop: 2 }}>
                  {s.detail}
                </div>
              </div>
            ))}
          </Pipe>
          <div style={{
            marginTop: 10, padding: 12,
            background: "var(--vast-dark)",
            border: "1px dashed var(--border)",
            borderRadius: 10,
            fontSize: 12, color: "var(--text-dim)",
          }}>
            <strong style={{ color: "var(--vast-blue)" }}>No separate indexing worker.</strong>{" "}
            Embedding happens inline at the tail of <code style={{ color: "var(--vast-blue)" }}>subclip-ai-analyzer</code>,
            so the search index is always consistent with what's in the assets table —
            a subclip either has all of <code>(transcript, ocr, scene, summary, embedding)</code> or
            none of them. Retries are idempotent because Qdrant point IDs equal the asset_id.
          </div>
        </div>
      </Section>

      {/* Storage */}
      <Section
        icon={Database}
        title="Storage layer"
        subtitle="Everything lives on VAST — S3 for objects, VAST DB for tables, Trino as the SQL front door."
      >
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20 }}>
          <div>
            <div style={{
              fontSize: 12, fontWeight: 700, textTransform: "uppercase",
              letterSpacing: 0.5, color: "var(--vast-blue)", marginBottom: 10,
            }}>
              S3 buckets
            </div>
            <div style={{ display: "grid", gap: 10 }}>
              {BUCKETS.map((b) => (
                <div key={b.name} style={{
                  background: "var(--surface)",
                  border: "1px solid var(--border)",
                  borderRadius: 10,
                  padding: 12,
                }}>
                  <div style={{
                    fontFamily: "SF Mono, Menlo, monospace",
                    fontSize: 12, color: "var(--vast-blue)", fontWeight: 700,
                  }}>
                    {b.name}
                  </div>
                  <div style={{ fontSize: 12, color: "var(--text-dim)", marginTop: 4 }}>
                    {b.purpose}
                  </div>
                </div>
              ))}
            </div>
          </div>
          <div>
            <div style={{
              fontSize: 12, fontWeight: 700, textTransform: "uppercase",
              letterSpacing: 0.5, color: "var(--vast-blue)", marginBottom: 10,
            }}>
              VAST DB tables (media-catalog schema)
            </div>
            <div style={{
              background: "var(--surface)",
              border: "1px solid var(--border)",
              borderRadius: 10,
              padding: 14,
              display: "flex", flexWrap: "wrap", gap: 8,
            }}>
              {TABLES.map((t) => (
                <span key={t} className="badge badge-info" style={{
                  fontFamily: "SF Mono, Menlo, monospace",
                  textTransform: "none",
                }}>
                  {t}
                </span>
              ))}
            </div>
            <div style={{ fontSize: 12, color: "var(--text-dim)", marginTop: 10, lineHeight: 1.6 }}>
              <code style={{ color: "var(--vast-blue)" }}>assets</code> is the central
              row-per-file table — all foundation and analysis functions upsert into it
              keyed by <code style={{ color: "var(--vast-blue)" }}>asset_id</code>.
              Subclips get their own <code style={{ color: "var(--vast-blue)" }}>assets</code>
              row with <code style={{ color: "var(--vast-blue)" }}>is_subclip=true</code>
              so the main list hides them by default.
            </div>
          </div>
        </div>
      </Section>

      {/* Query path */}
      <Section
        icon={Shield}
        title="Query path (this webapp)"
        subtitle="The React UI never touches VAST directly — it hits a Flask backend that proxies to Trino, which runs the actual SQL against VAST DB via the native connector."
      >
        <div style={{
          background: "var(--surface)",
          border: "1px solid var(--border)",
          borderRadius: 16,
          padding: 24,
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          gap: 8,
        }}>
          <StageBox label="React UI" sublabel="frontend/src/pages" />
          <FlowArrow />
          <StageBox label="Flask API" sublabel="webapp/backend/app.py" />
          <FlowArrow />
          <StageBox label="Trino" sublabel="vast connector" tone="blue" />
          <FlowArrow />
          <StageBox icon={Database} label="VAST DB" sublabel='james-db / "media-catalog"' tone="blue" />
        </div>
      </Section>

      <div style={{ height: 60 }} />
    </>
  );
}
