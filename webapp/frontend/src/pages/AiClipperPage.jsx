import React, { useEffect, useMemo, useState } from "react";
import {
  Scissors, Film, AlertCircle, CheckCircle2, AlertTriangle, XCircle,
  Loader2, Bot, Clock, Search, ArrowDownToLine,
} from "lucide-react";
import { listSources, getSource, videoURL } from "../api";

/**
 * /ai-clipper — every source video the QC + AI-clipper pipeline has
 * processed, with full-video preview alongside each AI-extracted clip.
 *
 * Left:  list of sources (sortable by upload time)
 * Right: selected source detail
 *          - header with prompt + counts
 *          - full video player (from qc-passed/qc-failed bucket)
 *          - one row per extracted clip with its own player + the
 *            timestamp band, confidence, vision verdict reason, and
 *            the model that picked it.
 */
export default function AiClipperPage() {
  const [sources,  setSources]  = useState(null);
  const [selected, setSelected] = useState(null);   // source_id
  const [filter,   setFilter]   = useState("");
  const [err,      setErr]      = useState(null);

  useEffect(() => {
    let alive = true;
    listSources()
      .then((d) => {
        if (!alive) return;
        setSources(d.sources || []);
        if ((d.sources || []).length && !selected) {
          // Auto-select the most-recent source that actually got clips
          const withClips = d.sources.find((s) => (s.clip_count || 0) > 0);
          setSelected((withClips || d.sources[0]).source_id);
        }
      })
      .catch((e) => alive && setErr(String(e)));
    return () => { alive = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const filtered = useMemo(() => {
    if (!sources) return [];
    if (!filter.trim()) return sources;
    const q = filter.toLowerCase();
    return sources.filter((s) =>
      sourceLabel(s).toLowerCase().includes(q) ||
      (s.clip_prompt || "").toLowerCase().includes(q) ||
      (s.qc_status || "").toLowerCase().includes(q),
    );
  }, [sources, filter]);

  return (
    <div>
      <div className="page-header">
        <h1 style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <Scissors size={24} style={{ color: "var(--vast-blue)" }} />
          AI Clipper
        </h1>
        <p>
          Every source video that has been through the pre-ingest pipeline,
          with the AI-selected clips alongside the original. Each clip's
          player streams from <code>s3://james-media-clips/</code>; the full
          video streams from <code>qc-passed</code> (or <code>qc-failed</code>)
          via the same range-enabled S3 proxy.
        </p>
      </div>

      {err && (
        <div className="card" style={{
          padding: "10px 14px", marginBottom: 14,
          background: "rgba(255,80,80,0.12)", color: "var(--danger)",
          border: "1px solid rgba(255,80,80,0.3)", fontSize: 13,
        }}>
          <AlertCircle size={14} style={{ verticalAlign: "text-bottom", marginRight: 6 }} />
          {err}
        </div>
      )}

      <div style={{
        display: "grid",
        gridTemplateColumns: "320px minmax(0, 1fr)",
        gap: 16,
        alignItems: "start",
      }}>
        {/* Sources sidebar */}
        <div>
          <div style={{ position: "relative", marginBottom: 8 }}>
            <Search size={13} style={{
              position: "absolute", left: 10, top: 9, color: "var(--text-dim)",
            }} />
            <input
              type="text"
              placeholder="Filter by filename, prompt, status…"
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              style={{
                width: "100%", padding: "6px 10px 6px 28px",
                background: "var(--bg-raised, #0f1a33)",
                border: "1px solid var(--border)", borderRadius: 6,
                color: "var(--text)", fontSize: 12,
              }}
            />
          </div>

          {!sources && !err && <div className="loading">Loading sources…</div>}
          {sources && filtered.length === 0 && (
            <div style={{ color: "var(--text-dim)", fontSize: 12, padding: 12 }}>
              {filter ? "No matches." : "No sources have been ingested yet."}
            </div>
          )}
          <div style={{ display: "grid", gap: 6 }}>
            {filtered.map((s) => (
              <SourceListItem
                key={s.source_id}
                source={s}
                selected={s.source_id === selected}
                onClick={() => setSelected(s.source_id)}
              />
            ))}
          </div>
        </div>

        {/* Detail */}
        <div style={{ minWidth: 0 }}>
          {selected ? (
            <SourceDetail key={selected} sourceId={selected} />
          ) : (
            <div className="card" style={{ padding: 24, textAlign: "center", color: "var(--text-dim)" }}>
              Pick a source on the left.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}


// ── Source list item ────────────────────────────────────────────────

function SourceListItem({ source: s, selected, onClick }) {
  const StatusIcon =
    s.qc_status === "passed"          ? CheckCircle2 :
    s.qc_status === "warn"            ? AlertTriangle :
    s.qc_status === "failed"          ? XCircle :
    (s.qc_status || "").startsWith("failed:") ? XCircle :
                                        Loader2;
  const statusColor =
    s.qc_status === "passed" ? "var(--vast-blue)" :
    s.qc_status === "warn"   ? "var(--warning)"   :
    (s.qc_status || "").startsWith("fail") ? "var(--danger)" :
                               "var(--text-dim)";
  return (
    <button
      onClick={onClick}
      className="card"
      style={{
        textAlign: "left", padding: 10, cursor: "pointer",
        background: selected ? "var(--vast-blue-dim)" : undefined,
        border: selected ? "1px solid var(--vast-blue)" : undefined,
      }}
    >
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 6 }}>
        <div style={{
          fontSize: 13, fontWeight: 600, color: "var(--text)",
          overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
          flex: 1, minWidth: 0,
        }}>
          {sourceLabel(s)}
        </div>
        <StatusIcon size={13} style={{ color: statusColor, flex: "0 0 auto" }} />
      </div>
      <div style={{
        fontSize: 11, color: "var(--text-dim)", marginTop: 4,
        display: "flex", justifyContent: "space-between", gap: 8,
      }}>
        <span>
          {s.duration_seconds != null
            ? `${Math.round(s.duration_seconds)}s`
            : "—"}
          {s.width && s.height ? ` · ${s.width}×${s.height}` : ""}
        </span>
        <span style={{
          color: (s.clip_count || 0) > 0 ? "var(--vast-blue)" : "var(--text-dim)",
          fontWeight: 600,
        }}>
          {(s.clip_count ?? 0)} clip{(s.clip_count ?? 0) === 1 ? "" : "s"}
        </span>
      </div>
      {s.clip_prompt && (
        <div style={{
          fontSize: 11, color: "var(--text-dim)", marginTop: 4,
          fontStyle: "italic",
          overflow: "hidden", textOverflow: "ellipsis",
          display: "-webkit-box", WebkitLineClamp: 2, WebkitBoxOrient: "vertical",
        }}>
          "{s.clip_prompt}"
        </div>
      )}
    </button>
  );
}


// ── Detail pane ─────────────────────────────────────────────────────

function SourceDetail({ sourceId }) {
  const [data, setData] = useState(null);
  const [err,  setErr]  = useState(null);

  useEffect(() => {
    let alive = true;
    setErr(null);
    setData(null);
    getSource(sourceId)
      .then((d) => alive && setData(d))
      .catch((e) => alive && setErr(String(e)));
    return () => { alive = false; };
  }, [sourceId]);

  if (err) {
    return <div className="card" style={{
      padding: 14, color: "var(--danger)",
      background: "rgba(255,80,80,0.10)",
      border: "1px solid rgba(255,80,80,0.3)",
    }}>
      <AlertCircle size={14} style={{ verticalAlign: "text-bottom", marginRight: 6 }} />
      {err}
    </div>;
  }
  if (!data) return <div className="loading">Loading source…</div>;

  const { source, clips } = data;
  return (
    <div>
      <Header source={source} clipCount={clips.length} />
      <FullVideo source={source} />
      <ClipsSection clips={clips} source={source} />
    </div>
  );
}


function Header({ source, clipCount }) {
  return (
    <div className="card" style={{ padding: 14, marginBottom: 12 }}>
      <div style={{
        display: "flex", alignItems: "flex-start",
        justifyContent: "space-between", gap: 12, flexWrap: "wrap",
      }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          <h2 style={{
            margin: 0, fontSize: 17, fontWeight: 600,
            overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
          }}>
            {sourceLabel(source)}
          </h2>
          <div style={{
            fontSize: 11, color: "var(--text-dim)", marginTop: 4,
            fontFamily: "SF Mono, Menlo, monospace",
          }}>
            source_id: {source.source_id}
          </div>
        </div>
        <Stat label="duration" value={formatDuration(source.duration_seconds)} />
        <Stat label="resolution" value={source.width && source.height ? `${source.width}×${source.height}` : "—"} />
        <Stat label="clips" value={clipCount} highlight={clipCount > 0} />
      </div>

      <div style={{
        marginTop: 10, paddingTop: 10,
        borderTop: "1px solid var(--border)",
        display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12,
        fontSize: 12,
      }}>
        <KV label="QC" value={
          <>
            <span style={{ color: source.qc_status === "passed" ? "var(--vast-blue)" :
                                  source.qc_status === "warn" ? "var(--warning)" :
                                  "var(--danger)", fontWeight: 600 }}>
              {source.qc_status || "—"}
            </span>
            {source.qc_verdict_reason && (
              <span style={{ marginLeft: 6, color: "var(--text-dim)" }}>
                — {source.qc_verdict_reason}
              </span>
            )}
          </>
        } />
        <KV label="Extraction" value={source.clip_extraction_status || "—"} />
        <KV label="Packaging" value={source.packaging_status || "—"} />
      </div>

      {source.clip_prompt && (
        <div style={{
          marginTop: 10, padding: "10px 12px",
          background: "var(--vast-blue-dim)",
          border: "1px solid var(--vast-blue)",
          borderRadius: 6,
          fontSize: 12,
        }}>
          <div style={{
            fontSize: 10, fontWeight: 700, textTransform: "uppercase",
            letterSpacing: 0.5, color: "var(--vast-blue)", marginBottom: 4,
            display: "flex", alignItems: "center", gap: 6,
          }}>
            <Bot size={11} /> AI prompt ({source.clip_prompt_source || "?"})
          </div>
          <div style={{ color: "var(--text)", fontStyle: "italic" }}>
            "{source.clip_prompt}"
          </div>
        </div>
      )}
    </div>
  );
}


function FullVideo({ source }) {
  if (!source.current_s3_path) {
    return null;
  }
  return (
    <div className="card" style={{ padding: 0, marginBottom: 12, overflow: "hidden" }}>
      <div style={{
        padding: "10px 14px",
        borderBottom: "1px solid var(--border)",
        fontWeight: 600, fontSize: 13,
        display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8,
      }}>
        <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
          <Film size={14} /> Full video
        </span>
        <span style={{
          fontSize: 11, color: "var(--text-dim)",
          fontFamily: "SF Mono, Menlo, monospace",
          overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
          maxWidth: "60%",
        }} title={source.current_s3_path}>
          {source.current_s3_path}
        </span>
      </div>
      <video
        src={videoURL(source.current_s3_path)}
        controls
        preload="metadata"
        style={{ width: "100%", maxHeight: 480, display: "block", background: "#000" }}
      />
    </div>
  );
}


function ClipsSection({ clips, source }) {
  if (!clips.length) {
    return (
      <div className="card" style={{ padding: 24, textAlign: "center", color: "var(--text-dim)" }}>
        <Scissors size={28} style={{ opacity: 0.5 }} />
        <div style={{ marginTop: 10, fontSize: 13 }}>
          {source.clip_extraction_status === "done"
            ? "Extraction completed but no clips matched the prompt."
            : source.clip_extraction_status?.startsWith("failed")
              ? `Extraction failed: ${source.clip_extraction_status}`
              : source.clip_extraction_status?.startsWith("pending")
                ? "Extraction in progress…"
                : "AI clipper hasn't run on this source yet."}
        </div>
      </div>
    );
  }
  return (
    <div className="card" style={{ padding: 0 }}>
      <div style={{
        padding: "10px 14px", borderBottom: "1px solid var(--border)",
        fontWeight: 600, fontSize: 13,
        display: "flex", alignItems: "center", gap: 6,
      }}>
        <Scissors size={14} /> AI-extracted clips ({clips.length})
      </div>
      {clips.map((c) => <ClipRow key={c.clip_id} clip={c} sourceDuration={source.duration_seconds} />)}
    </div>
  );
}


function ClipRow({ clip, sourceDuration }) {
  const start = clip.start_seconds || 0;
  const end   = clip.end_seconds   || 0;
  const conf  = clip.match_confidence || 0;
  const dur   = sourceDuration && sourceDuration > 0 ? sourceDuration : null;
  const startPct = dur ? (start / dur) * 100 : null;
  const widthPct = dur ? Math.max(0.5, ((end - start) / dur) * 100) : null;

  return (
    <div style={{
      padding: 14, borderBottom: "1px solid var(--border)",
      display: "grid", gridTemplateColumns: "minmax(0, 1fr) 320px", gap: 14,
    }}>
      {/* Left: meta + timeline indicator */}
      <div style={{ minWidth: 0 }}>
        <div style={{
          display: "flex", alignItems: "center", gap: 8, marginBottom: 6,
        }}>
          <span style={{
            fontFamily: "SF Mono, Menlo, monospace", color: "var(--vast-blue)",
            fontWeight: 600, fontSize: 13,
          }}>
            clip-{String(clip.clip_index ?? 0).padStart(3, "0")}
          </span>
          <span style={{ color: "var(--text-dim)", fontSize: 12 }}>
            {start.toFixed(2)}s → {end.toFixed(2)}s · {(end - start).toFixed(1)}s
          </span>
          <span style={{ marginLeft: "auto", fontSize: 12 }}>
            <ConfidenceChip conf={conf} />
          </span>
        </div>

        {/* Timeline strip showing where this clip sits in the source */}
        {dur && (
          <div style={{
            position: "relative", height: 10,
            background: "rgba(255,255,255,0.04)",
            border: "1px solid var(--border)", borderRadius: 4,
            marginBottom: 8,
          }} title={`Position in source (${formatDuration(dur)})`}>
            <div style={{
              position: "absolute", top: -1, bottom: -1,
              left: `${startPct}%`, width: `${widthPct}%`,
              background: "var(--vast-blue)", borderRadius: 4,
              minWidth: 2,
            }} />
          </div>
        )}

        {clip.match_reason && (
          <div style={{
            fontSize: 12, color: "var(--text-dim)",
            fontStyle: "italic", lineHeight: 1.5,
            display: "-webkit-box", WebkitLineClamp: 4, WebkitBoxOrient: "vertical",
            overflow: "hidden",
          }}>
            "{clip.match_reason}"
          </div>
        )}

        <div style={{
          fontSize: 11, color: "var(--text-dim)", marginTop: 6,
          display: "flex", gap: 12, flexWrap: "wrap",
        }}>
          {clip.vision_model && (
            <span style={{ display: "inline-flex", alignItems: "center", gap: 3 }}>
              <Bot size={10} /> {clip.vision_model.split("/").pop()}
            </span>
          )}
          {clip.shot_count > 1 && (
            <span style={{ display: "inline-flex", alignItems: "center", gap: 3 }}>
              <Scissors size={10} /> {clip.shot_count} shots merged
            </span>
          )}
          {clip.file_size_bytes && (
            <span style={{ display: "inline-flex", alignItems: "center", gap: 3 }}>
              <ArrowDownToLine size={10} /> {formatBytes(clip.file_size_bytes)}
            </span>
          )}
        </div>
      </div>

      {/* Right: clip player */}
      <div>
        <video
          src={videoURL(clip.clip_s3_path)}
          controls
          preload="none"
          style={{ width: "100%", maxHeight: 200, display: "block", background: "#000", borderRadius: 4 }}
        />
        <div style={{
          fontSize: 10, color: "var(--text-dim)", marginTop: 4,
          fontFamily: "SF Mono, Menlo, monospace",
          overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
        }} title={clip.clip_s3_path}>
          {clip.clip_s3_path}
        </div>
      </div>
    </div>
  );
}


// ── Tiny helpers ────────────────────────────────────────────────────

function ConfidenceChip({ conf }) {
  const pct = Math.round((conf || 0) * 100);
  const tone =
    pct >= 75 ? { bg: "rgba(31,217,254,0.10)", fg: "var(--vast-blue)" } :
    pct >= 35 ? { bg: "rgba(255,193,7,0.10)",  fg: "var(--warning)" }   :
                { bg: "rgba(255,80,80,0.08)",  fg: "var(--danger)" };
  return (
    <span style={{
      padding: "1px 8px", borderRadius: 10,
      background: tone.bg, color: tone.fg,
      fontSize: 11, fontWeight: 600,
      fontFamily: "SF Mono, Menlo, monospace",
    }}>
      {pct}%
    </span>
  );
}

function Stat({ label, value, highlight }) {
  return (
    <div style={{ minWidth: 70, textAlign: "right" }}>
      <div style={{
        fontSize: 17, fontWeight: 700,
        color: highlight ? "var(--vast-blue)" : "var(--text)",
        fontFamily: "SF Mono, Menlo, monospace",
      }}>{value}</div>
      <div style={{
        fontSize: 10, color: "var(--text-dim)",
        textTransform: "uppercase", letterSpacing: 0.5,
      }}>{label}</div>
    </div>
  );
}

function KV({ label, value }) {
  return (
    <div>
      <div style={{
        fontSize: 10, color: "var(--text-dim)", textTransform: "uppercase",
        letterSpacing: 0.5, marginBottom: 2,
      }}>{label}</div>
      <div style={{ color: "var(--text)" }}>{value}</div>
    </div>
  );
}

// Display label for a source row. Prefer filename, but fall back through
// the various S3 path fields and finally to a short source_id prefix —
// orphan rows (e.g. ai-clipper on a direct-to-qc-passed upload) used to
// have null filename and would otherwise render as "(unknown)".
function sourceLabel(s) {
  if (!s) return "(unknown)";
  return (
    s.filename ||
    basename(s.current_s3_path) ||
    basename(s.s3_inbox_path) ||
    (s.source_id ? `source ${s.source_id.slice(0, 8)}…` : "(unknown)")
  );
}

function basename(p) {
  if (!p) return null;
  const i = p.lastIndexOf("/");
  return i >= 0 ? p.slice(i + 1) : p;
}

function formatDuration(s) {
  if (s == null || isNaN(s)) return "—";
  if (s < 60)    return `${Math.round(s)}s`;
  if (s < 3600)  return `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`;
  return `${Math.floor(s / 3600)}h ${Math.round((s % 3600) / 60)}m`;
}

function formatBytes(n) {
  if (n == null) return "—";
  const u = ["B", "KB", "MB", "GB"];
  let i = 0;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return `${n.toFixed(n < 10 ? 1 : 0)}${u[i]}`;
}
