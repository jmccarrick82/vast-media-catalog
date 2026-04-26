import React, { useCallback, useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import {
  ArrowLeft, Shield, ShieldCheck, ShieldAlert, ShieldX,
  FileJson, Loader2, AlertCircle, ChevronRight, ChevronDown,
  ChevronLeft as ChevLeft, ChevronRight as ChevRight,
  Copy, ExternalLink, Film, Hash, Clock, Award, Bot, Scissors,
} from "lucide-react";
import {
  getPackage, getPackageManifest, getRenditionC2pa, videoURL,
} from "../api";

/**
 * /packages/<package_id> — deep dive on one delivery bundle.
 *
 * Layout:
 *   ┌─────────────────────────────────────────────────────────┐
 *   │  Header: package_id, source, status, licensing summary  │
 *   ├──────────────────────────┬──────────────────────────────┤
 *   │  Left: video player +    │  Right: C2PA panel           │
 *   │  rendition picker,       │  (signature, cert, actions,  │
 *   │  clip list               │   AI disclosure, assertions) │
 *   └──────────────────────────┴──────────────────────────────┘
 */
export default function PackageDetailPage() {
  const { packageId } = useParams();
  const [data, setData] = useState(null);
  const [err, setErr]   = useState(null);
  const [selected, setSelected] = useState(null); // {clipIndex, renditionId}

  useEffect(() => {
    let alive = true;
    setErr(null);
    getPackage(packageId)
      .then((d) => {
        if (!alive) return;
        setData(d);
        // auto-select the first rendition of the first clip
        const firstClip = (d.clips || [])[0];
        const firstRend = (firstClip?.renditions || [])[0];
        if (firstRend) {
          setSelected({
            clipIndex:     firstClip.clip_index ?? 0,
            renditionId:   firstRend.rendition_id,
          });
        }
      })
      .catch((e) => { if (alive) setErr(String(e)); });
    return () => { alive = false; };
  }, [packageId]);

  if (err) return <ErrorBox text={err} />;
  if (!data) return <div className="loading">Loading package…</div>;

  const { package: pkg, source, clips } = data;

  const selectedClip = clips.find((c) => c.clip_index === selected?.clipIndex);
  const selectedRend = selectedClip?.renditions.find((r) => r.rendition_id === selected?.renditionId);

  return (
    <div>
      <div style={{ marginBottom: 12 }}>
        <Link to="/packages" style={{
          color: "var(--text-dim)", textDecoration: "none", fontSize: 13,
          display: "inline-flex", alignItems: "center", gap: 4,
        }}>
          <ArrowLeft size={14} /> All packages
        </Link>
      </div>

      <PackageHeader pkg={pkg} source={source} clips={clips} />

      {/* Clip strip — quick jump to any of the N clips */}
      <ClipStrip
        clips={clips}
        selectedClipIndex={selected?.clipIndex}
        onSelectClip={(clipIndex) => {
          const clip = clips.find((c) => c.clip_index === clipIndex);
          const rend = clip?.renditions?.[0];
          if (clip && rend) setSelected({ clipIndex, renditionId: rend.rendition_id });
        }}
      />

      <div style={{
        display: "grid", gridTemplateColumns: "minmax(0, 1fr) 420px",
        gap: 20, marginTop: 16,
      }}>
        <div style={{ minWidth: 0 }}>
          <Player
            rend={selectedRend}
            clip={selectedClip}
            clips={clips}
            onPrev={() => navigateClip(selected, clips, -1, setSelected)}
            onNext={() => navigateClip(selected, clips, +1, setSelected)}
          />
          <ClipList
            clips={clips}
            selected={selected}
            onSelect={setSelected}
          />
          <SidecarManifestCard packageId={packageId} pkg={pkg} />
        </div>
        <div>
          <C2paPanel
            packageId={packageId}
            rend={selectedRend}
            clip={selectedClip}
            pkg={pkg}
          />
          <LicensingCard pkg={pkg} />
        </div>
      </div>
    </div>
  );
}


function navigateClip(selected, clips, dir, setSelected) {
  if (!clips.length) return;
  const order = clips.map((c) => c.clip_index).sort((a, b) => a - b);
  const cur = selected?.clipIndex;
  let i = order.indexOf(cur);
  if (i < 0) i = 0;
  let next = i + dir;
  if (next < 0) next = order.length - 1;
  if (next >= order.length) next = 0;
  const nextIdx = order[next];
  const clip = clips.find((c) => c.clip_index === nextIdx);
  // Try to keep the same rendition preset; fall back to first.
  const sameName = clip?.renditions?.find((r) => {
    const cur_clip = clips.find((c) => c.clip_index === cur);
    const cur_rend = cur_clip?.renditions?.find((r) => r.rendition_id === selected?.renditionId);
    return cur_rend && r.rendition_name === cur_rend.rendition_name;
  });
  const rend = sameName || clip?.renditions?.[0];
  if (clip && rend) setSelected({ clipIndex: nextIdx, renditionId: rend.rendition_id });
}


// ── Clip strip — horizontal quick-jump ──────────────────────────────

function ClipStrip({ clips, selectedClipIndex, onSelectClip }) {
  if (clips.length <= 1) return null;
  const sorted = [...clips].sort((a, b) => (a.clip_index ?? 0) - (b.clip_index ?? 0));
  return (
    <div className="card" style={{
      padding: "10px 14px", marginTop: 12,
      display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap",
    }}>
      <div style={{
        fontSize: 11, color: "var(--text-dim)", fontWeight: 700,
        textTransform: "uppercase", letterSpacing: 0.5,
        marginRight: 6,
      }}>
        Clips
      </div>
      <div style={{
        display: "flex", gap: 6, flex: 1, flexWrap: "wrap",
      }}>
        {sorted.map((c, i) => {
          const sel = c.clip_index === selectedClipIndex;
          return (
            <button
              key={c.clip_id}
              onClick={() => onSelectClip(c.clip_index)}
              title={`${c.start_seconds?.toFixed(1)}s – ${c.end_seconds?.toFixed(1)}s` +
                     (c.match_reason ? ` — ${c.match_reason.slice(0, 60)}` : "")}
              style={{
                display: "flex", alignItems: "center", gap: 6,
                padding: "6px 12px",
                borderRadius: 8, border: sel ? "1px solid var(--vast-blue)" : "1px solid var(--border)",
                background: sel ? "var(--vast-blue-dim)" : "transparent",
                color: sel ? "var(--vast-blue)" : "var(--text)",
                cursor: "pointer", fontSize: 12,
                fontFamily: "SF Mono, Menlo, monospace",
                fontWeight: sel ? 700 : 500,
              }}
            >
              <span>#{i + 1}</span>
              <span style={{ color: sel ? "var(--vast-blue)" : "var(--text-dim)" }}>
                {Math.round(c.start_seconds ?? 0)}s
              </span>
              {c.renditions?.[0]?.c2pa_signed && <ShieldCheck size={10} />}
            </button>
          );
        })}
      </div>
    </div>
  );
}


// ── Header ──────────────────────────────────────────────────────────

function PackageHeader({ pkg, source, clips }) {
  const signed = pkg.c2pa_signed_count || 0;
  const total  = pkg.rendition_count  || 0;
  return (
    <div className="card" style={{ padding: 16 }}>
      <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 12, flexWrap: "wrap" }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <h1 style={{
              margin: 0, fontSize: 18, fontWeight: 600,
              overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
            }}>
              {source?.filename
                || basename(source?.current_s3_path)
                || basename(source?.s3_inbox_path)
                || (source?.source_id
                    ? <span style={{ color: "var(--text-dim)" }}>source {source.source_id.slice(0, 8)}…</span>
                    : <em>unknown source</em>)}
            </h1>
            <Pill
              color={pkg.status === "ready" ? "blue" : pkg.status === "failed" ? "red" : "yellow"}
              text={pkg.status}
            />
          </div>
          <div style={{
            fontSize: 12, color: "var(--text-dim)", marginTop: 6,
            fontFamily: "SF Mono, Menlo, monospace",
          }}>
            package_id: <strong style={{ color: "var(--text)" }}>{pkg.package_id}</strong>
            {source?.source_id && <> · source_id: {source.source_id.slice(0, 12)}…</>}
          </div>
        </div>
        <Stat label="clips"      value={clips.length} />
        <Stat label="renditions" value={total} />
        <Stat label="C2PA signed" value={`${signed} / ${total}`} highlight={signed === total && total > 0} />
      </div>

      <div style={{
        marginTop: 10, display: "flex", gap: 16, fontSize: 12,
        color: "var(--text-dim)", flexWrap: "wrap",
      }}>
        {source?.clip_prompt && <span><strong>prompt:</strong> "{source.clip_prompt}"</span>}
        {source?.qc_status && <span><strong>qc:</strong> {source.qc_status}</span>}
        {pkg.created_at && <span><strong>created:</strong> {new Date(pkg.created_at * 1000).toLocaleString()}</span>}
      </div>
    </div>
  );
}


// ── Video player + rendition picker ─────────────────────────────────

function Player({ rend, clip, clips, onPrev, onNext }) {
  if (!rend) {
    return <div className="card" style={{ padding: 32, textAlign: "center", color: "var(--text-dim)" }}>
      No rendition selected.
    </div>;
  }
  const total = clips?.length || 0;
  const order = (clips || []).map((c) => c.clip_index).sort((a, b) => a - b);
  const pos   = order.indexOf(clip?.clip_index ?? -1);
  const labelN = pos >= 0 ? pos + 1 : "?";

  return (
    <div className="card" style={{ padding: 0, overflow: "hidden", marginBottom: 12 }}>
      {/* Player header with clip nav */}
      <div style={{
        padding: "8px 12px",
        borderBottom: "1px solid var(--border)",
        display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8,
        background: "var(--vast-blue-dim)",
      }}>
        <button
          onClick={onPrev}
          disabled={total <= 1}
          className="picker-btn picker-btn-dim"
          title="Previous clip"
          style={{ padding: "4px 8px", opacity: total <= 1 ? 0.4 : 1 }}
        >
          <ChevLeft size={14} /> Prev
        </button>
        <div style={{ fontSize: 13, fontWeight: 600, color: "var(--vast-blue)" }}>
          Clip <span style={{ fontFamily: "SF Mono, Menlo, monospace", fontSize: 14 }}>
            {labelN} of {total}
          </span>
          {clip?.start_seconds != null && (
            <span style={{ color: "var(--text-dim)", fontWeight: 400, marginLeft: 10 }}>
              · {clip.start_seconds.toFixed(1)}s – {clip.end_seconds.toFixed(1)}s
              {" "}({(clip.end_seconds - clip.start_seconds).toFixed(1)}s)
            </span>
          )}
        </div>
        <button
          onClick={onNext}
          disabled={total <= 1}
          className="picker-btn picker-btn-dim"
          title="Next clip"
          style={{ padding: "4px 8px", opacity: total <= 1 ? 0.4 : 1 }}
        >
          Next <ChevRight size={14} />
        </button>
      </div>

      <video
        key={rend.rendition_id}
        src={videoURL(rend.rendition_s3_path)}
        controls
        autoPlay
        preload="metadata"
        style={{ width: "100%", maxHeight: 480, display: "block", background: "#000" }}
      />
      <div style={{ padding: "10px 14px", display: "flex", justifyContent: "space-between", alignItems: "center", gap: 10 }}>
        <div style={{ fontSize: 13 }}>
          <strong style={{ color: "var(--vast-blue)", fontFamily: "SF Mono, Menlo, monospace" }}>
            {rend.rendition_name}
          </strong>
          <span style={{ color: "var(--text-dim)", marginLeft: 8 }}>
            {rend.width}×{rend.height} · {rend.video_codec}/{rend.audio_codec}
          </span>
        </div>
        <div style={{ fontSize: 11, color: "var(--text-dim)" }}>
          {rend.c2pa_signed ? (
            <span style={{ color: "var(--vast-blue)", display: "inline-flex", alignItems: "center", gap: 4 }}>
              <ShieldCheck size={12} /> C2PA signed
            </span>
          ) : (
            <span style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>
              <Shield size={12} /> unsigned
            </span>
          )}
        </div>
      </div>
      {clip?.match_reason && (
        <div style={{
          padding: "10px 14px",
          borderTop: "1px solid var(--border)",
          fontSize: 12, color: "var(--text-dim)", fontStyle: "italic",
          background: "rgba(255,255,255,0.02)",
        }}>
          <strong style={{ color: "var(--text)", fontStyle: "normal" }}>Why this clip:</strong>{" "}
          "{clip.match_reason}"
          {clip.shot_count > 1 && (
            <span style={{ marginLeft: 8 }}>
              <Scissors size={10} style={{ verticalAlign: "text-bottom" }} /> {clip.shot_count} shots merged
            </span>
          )}
        </div>
      )}
    </div>
  );
}


// ── Clip + rendition list ───────────────────────────────────────────

function ClipList({ clips, selected, onSelect }) {
  return (
    <div className="card" style={{ padding: 0, marginBottom: 12 }}>
      <div style={{ padding: "10px 14px", borderBottom: "1px solid var(--border)", fontWeight: 600, fontSize: 13 }}>
        All clips ({clips.length}) — click any row to play, or pick a specific rendition
      </div>
      {clips.map((c) => {
        const isActiveClip = selected?.clipIndex === c.clip_index;
        const firstRend = c.renditions?.[0];
        return (
        <div
          key={c.clip_id}
          onClick={() => firstRend && onSelect({ clipIndex: c.clip_index, renditionId: firstRend.rendition_id })}
          style={{
            padding: "10px 14px",
            borderBottom: "1px solid var(--border)",
            cursor: "pointer",
            background: isActiveClip ? "var(--vast-blue-dim)" : "transparent",
            borderLeft: isActiveClip ? "3px solid var(--vast-blue)" : "3px solid transparent",
            transition: "background 100ms ease",
          }}
          onMouseEnter={(e) => { if (!isActiveClip) e.currentTarget.style.background = "rgba(255,255,255,0.03)"; }}
          onMouseLeave={(e) => { if (!isActiveClip) e.currentTarget.style.background = "transparent"; }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 10 }}>
            <div style={{ fontSize: 13 }}>
              <span style={{
                fontFamily: "SF Mono, Menlo, monospace", color: "var(--vast-blue)",
                fontWeight: 600, marginRight: 8,
              }}>
                clip-{String(c.clip_index).padStart(3, "0")}
              </span>
              <span style={{ color: "var(--text-dim)" }}>
                {c.start_seconds?.toFixed(2)}s → {c.end_seconds?.toFixed(2)}s
              </span>
              <span style={{ color: "var(--text-dim)", marginLeft: 12 }}>
                conf: <strong style={{ color: "var(--text)" }}>{(c.match_confidence * 100).toFixed(0)}%</strong>
              </span>
            </div>
            {c.shot_count > 1 && (
              <span style={{ fontSize: 11, color: "var(--text-dim)" }}>
                {c.shot_count} shots merged
              </span>
            )}
          </div>
          {c.match_reason && (
            <div style={{ fontSize: 12, color: "var(--text-dim)", marginTop: 4, fontStyle: "italic" }}>
              "{c.match_reason}"
            </div>
          )}
          <div
            style={{ display: "flex", gap: 6, marginTop: 8, flexWrap: "wrap" }}
            onClick={(e) => e.stopPropagation() /* clip-row click would override rendition pick */}
          >
            {c.renditions.map((r) => {
              const sel = selected?.clipIndex === c.clip_index && selected?.renditionId === r.rendition_id;
              return (
                <button
                  key={r.rendition_id}
                  onClick={() => onSelect({ clipIndex: c.clip_index, renditionId: r.rendition_id })}
                  className={sel ? "picker-btn" : "picker-btn picker-btn-dim"}
                  style={{ fontSize: 11, padding: "4px 8px", gap: 4 }}
                >
                  <Film size={10} />
                  <span>{r.rendition_name}</span>
                  {r.c2pa_signed && <ShieldCheck size={10} />}
                </button>
              );
            })}
          </div>
        </div>
        );
      })}
    </div>
  );
}


// ── C2PA panel (live-verified via c2patool on the backend) ──────────

function C2paPanel({ packageId, rend, clip, pkg }) {
  const [report, setReport] = useState(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState(null);

  const verify = useCallback(async () => {
    if (!rend) return;
    setLoading(true);
    setErr(null);
    setReport(null);
    try {
      const r = await getRenditionC2pa(packageId, rend.rendition_id);
      setReport(r);
    } catch (e) {
      setErr(String(e));
    } finally {
      setLoading(false);
    }
  }, [packageId, rend]);

  useEffect(() => { verify(); }, [verify]);

  return (
    <div className="card" style={{ padding: 0, marginBottom: 12, overflow: "hidden" }}>
      <div style={{
        padding: "12px 14px", borderBottom: "1px solid var(--border)",
        display: "flex", alignItems: "center", justifyContent: "space-between", gap: 10,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <Shield size={16} style={{ color: "var(--vast-blue)" }} />
          <strong style={{ fontSize: 14 }}>Content Credentials (C2PA)</strong>
        </div>
        <button
          onClick={verify}
          disabled={loading || !rend}
          className="picker-btn picker-btn-dim"
          style={{ fontSize: 11, padding: "4px 8px" }}
        >
          {loading ? <Loader2 size={12} className="spin" /> : "Re-verify"}
        </button>
      </div>

      {err && <div style={{ padding: 14, color: "var(--danger)", fontSize: 12 }}>
        <AlertCircle size={12} style={{ verticalAlign: "text-bottom" }} /> {err}
      </div>}

      {loading && <div style={{ padding: 20, textAlign: "center", color: "var(--text-dim)" }}>
        <Loader2 className="spin" size={20} /> <div style={{ marginTop: 8, fontSize: 12 }}>Running c2patool on the rendition…</div>
      </div>}

      {report && !report.signed && !loading && (
        <div style={{ padding: 20, textAlign: "center", color: "var(--text-dim)" }}>
          <ShieldX size={24} /> <div style={{ marginTop: 6, fontSize: 12 }}>No C2PA manifest embedded in this rendition.</div>
        </div>
      )}

      {report && report.signed && !loading && (
        <C2paSummary report={report} rend={rend} />
      )}
    </div>
  );
}


function C2paSummary({ report, rend }) {
  const active = report.active;
  const sig    = active?.signature_info || {};
  const ai     = report.ai_disclosure;

  return (
    <div>
      {/* Signature info */}
      <Section title="Signature" defaultOpen>
        <KV k="Signer"    v={sig.issuer} mono />
        <KV k="Algorithm" v={sig.alg} mono />
        <KV k="Signed at" v={sig.time ? new Date(sig.time).toLocaleString() : null} />
        <KV k="Manifest"  v={active?.label} mono copy />
        {sig.cert_serial_number && (
          <KV k="Cert serial" v={`${sig.cert_serial_number.slice(0, 12)}…`} mono />
        )}
        <KV k="Validation" v={
          <span style={{ color: "var(--vast-blue)", display: "inline-flex", alignItems: "center", gap: 4 }}>
            <ShieldCheck size={12} /> Valid signature
          </span>
        } />
      </Section>

      {/* AI disclosure — the headline */}
      {ai && (
        <Section
          title="AI Disclosure"
          icon={<Bot size={14} style={{ color: "var(--vast-blue)" }} />}
          defaultOpen
          badge="com.vast.ai_clip_selection"
        >
          <KV k="Model"      v={ai.model} mono />
          <KV k="Prompt"     v={`"${ai.prompt}"`} />
          <KV k="Confidence" v={ai.match_confidence != null ? `${(ai.match_confidence * 100).toFixed(0)}%` : null} />
          {ai.source_span && (
            <KV k="Source span" v={`${ai.source_span.start?.toFixed(2)}s → ${ai.source_span.end?.toFixed(2)}s`} />
          )}
        </Section>
      )}

      {/* Actions (the provenance chain) */}
      <ActionsSection assertions={active?.assertions || []} />

      {/* Training / mining policy */}
      <TrainingMiningSection assertions={active?.assertions || []} />

      {/* Creative work / attribution */}
      <CreativeWorkSection assertions={active?.assertions || []} />

      {/* All assertions — collapsed */}
      <Section title="All assertions" icon={<FileJson size={14} />}>
        <ul style={{ margin: 0, padding: "0 0 0 16px", fontSize: 12, color: "var(--text-dim)" }}>
          {(active?.assertions || []).map((a, i) => (
            <li key={i} style={{ fontFamily: "SF Mono, Menlo, monospace" }}>{a.label}</li>
          ))}
        </ul>
      </Section>

      {/* External verification link */}
      <div style={{ padding: "10px 14px", borderTop: "1px solid var(--border)", fontSize: 11, color: "var(--text-dim)" }}>
        Want a second opinion? Download the rendition and check it on{" "}
        <a href="https://contentcredentials.org/verify" target="_blank" rel="noreferrer"
           style={{ color: "var(--vast-blue)", textDecoration: "none" }}>
          contentcredentials.org/verify <ExternalLink size={10} style={{ verticalAlign: "text-bottom" }} />
        </a>
      </div>
    </div>
  );
}


function ActionsSection({ assertions }) {
  const actionsAssert = assertions.find((a) => a.label?.startsWith("c2pa.actions"));
  if (!actionsAssert) return null;
  const actions = actionsAssert.data?.actions || [];
  const iconFor = (action) =>
    action === "c2pa.created" ? <Hash size={12} style={{ color: "var(--vast-blue)" }} /> :
    action === "c2pa.placed"  ? <Scissors size={12} style={{ color: "var(--vast-blue)" }} /> :
    action === "c2pa.edited"  ? <Film size={12} style={{ color: "var(--vast-blue)" }} /> :
                                <Clock size={12} />;
  return (
    <Section
      title="Actions (provenance chain)"
      icon={<Award size={14} style={{ color: "var(--vast-blue)" }} />}
      defaultOpen
      badge={`${actions.length} step${actions.length === 1 ? "" : "s"}`}
    >
      <ol style={{ margin: 0, padding: 0, listStyle: "none" }}>
        {actions.map((a, i) => (
          <li key={i} style={{ display: "flex", gap: 10, padding: "6px 0", borderBottom: i < actions.length - 1 ? "1px dashed var(--border)" : "none" }}>
            <div style={{ marginTop: 2 }}>{iconFor(a.action)}</div>
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ fontFamily: "SF Mono, Menlo, monospace", fontSize: 12, color: "var(--text)" }}>
                {a.action}
              </div>
              {a.softwareAgent?.name && (
                <div style={{ fontSize: 11, color: "var(--text-dim)" }}>
                  agent: {a.softwareAgent.name}{a.softwareAgent.version ? ` @ ${a.softwareAgent.version}` : ""}
                </div>
              )}
              {a.parameters?.description && (
                <div style={{ fontSize: 12, color: "var(--text-dim)", marginTop: 2, fontStyle: "italic" }}>
                  {a.parameters.description}
                </div>
              )}
              {a.digitalSourceType && (
                <div style={{ fontSize: 10, color: "var(--text-dim)", marginTop: 2 }} title={a.digitalSourceType}>
                  iptc source type: {a.digitalSourceType.split("/").slice(-1)[0]}
                </div>
              )}
            </div>
          </li>
        ))}
      </ol>
    </Section>
  );
}


function TrainingMiningSection({ assertions }) {
  const a = assertions.find((x) => x.label === "c2pa.training-mining");
  if (!a) return null;
  const entries = a.data?.entries || {};
  return (
    <Section title="Training & Data Mining">
      <table style={{ width: "100%", fontSize: 12 }}>
        <tbody>
          {Object.entries(entries).map(([k, v]) => (
            <tr key={k}>
              <td style={{ padding: "3px 0", color: "var(--text-dim)", fontFamily: "SF Mono, Menlo, monospace" }}>
                {k.replace("c2pa.", "")}
              </td>
              <td style={{ textAlign: "right", padding: "3px 0" }}>
                <span style={{
                  padding: "1px 6px", borderRadius: 3,
                  background: v.use === "notAllowed" ? "rgba(255,80,80,0.10)" : "rgba(31,217,254,0.08)",
                  color:      v.use === "notAllowed" ? "var(--danger)" : "var(--vast-blue)",
                  fontSize: 10, fontFamily: "SF Mono, Menlo, monospace",
                }}>
                  {v.use}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </Section>
  );
}


function CreativeWorkSection({ assertions }) {
  const a = assertions.find((x) => x.label === "c2pa.creative_work");
  if (!a) return null;
  const authors = (a.data?.author || []).map((x) => x.name).filter(Boolean);
  return (
    <Section title="Creative Work">
      <KV k="Title"   v={a.data?.name} />
      {authors.length > 0 && <KV k="Author(s)" v={authors.join(", ")} />}
    </Section>
  );
}


// ── Licensing card ──────────────────────────────────────────────────

function LicensingCard({ pkg }) {
  return (
    <div className="card" style={{ padding: 0 }}>
      <div style={{ padding: "12px 14px", borderBottom: "1px solid var(--border)", fontWeight: 600, fontSize: 14 }}>
        Licensing
      </div>
      <div style={{ padding: "10px 14px" }}>
        <KV k="Attribution" v={pkg.source_attribution} />
        <KV k="Rights cleared for" v={(pkg.rights_cleared_for || []).join(", ") || "—"} mono />
        <KV k="Restrictions" v={(pkg.restrictions || []).join(", ") || "—"} mono />
        <KV k="Clearance expires"
            v={pkg.clearance_expires_at
              ? new Date(pkg.clearance_expires_at * 1000).toLocaleDateString()
              : "—"} />
        {pkg.licensing_notes && <KV k="Notes" v={pkg.licensing_notes} />}
      </div>
    </div>
  );
}


// ── Sidecar manifest viewer (on-demand fetch) ───────────────────────

function SidecarManifestCard({ packageId, pkg }) {
  const [open, setOpen] = useState(false);
  const [manifest, setManifest] = useState(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState(null);

  const toggle = useCallback(async () => {
    const willOpen = !open;
    setOpen(willOpen);
    if (willOpen && !manifest) {
      setLoading(true);
      try {
        const m = await getPackageManifest(packageId);
        setManifest(m);
      } catch (e) {
        setErr(String(e));
      } finally {
        setLoading(false);
      }
    }
  }, [open, manifest, packageId]);

  if (!pkg.manifest_s3_path) return null;

  return (
    <div className="card" style={{ padding: 0 }}>
      <button
        onClick={toggle}
        style={{
          width: "100%", padding: "12px 14px", border: "none",
          background: "transparent", color: "var(--text)", cursor: "pointer",
          display: "flex", alignItems: "center", justifyContent: "space-between",
          fontSize: 14, fontWeight: 600, textAlign: "left",
        }}
      >
        <span style={{ display: "inline-flex", alignItems: "center", gap: 8 }}>
          {open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
          <FileJson size={14} /> Sidecar manifest (manifest.json)
        </span>
        <span style={{ fontSize: 11, color: "var(--text-dim)", fontFamily: "SF Mono, Menlo, monospace" }}>
          {pkg.manifest_s3_path?.split("/").slice(-1)[0]}
        </span>
      </button>
      {open && (
        <div style={{ borderTop: "1px solid var(--border)" }}>
          {loading && <div style={{ padding: 20, textAlign: "center" }}><Loader2 size={18} className="spin" /></div>}
          {err && <div style={{ padding: 14, color: "var(--danger)", fontSize: 12 }}>{err}</div>}
          {manifest && (
            <pre style={{
              padding: 14, margin: 0, fontSize: 11,
              fontFamily: "SF Mono, Menlo, monospace",
              color: "var(--text)", overflow: "auto", maxHeight: 400,
              background: "rgba(0,0,0,0.15)",
            }}>
              {JSON.stringify(manifest, null, 2)}
            </pre>
          )}
        </div>
      )}
    </div>
  );
}


// ── Shared bits ─────────────────────────────────────────────────────

function Section({ title, icon, defaultOpen = false, badge, children }) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div style={{ borderBottom: "1px solid var(--border)" }}>
      <button
        onClick={() => setOpen((o) => !o)}
        style={{
          width: "100%", padding: "10px 14px", border: "none",
          background: "transparent", color: "var(--text)", cursor: "pointer",
          display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8,
          fontSize: 13, fontWeight: 600, textAlign: "left",
        }}
      >
        <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
          {open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
          {icon}
          <span>{title}</span>
        </span>
        {badge && <span style={{
          fontSize: 10, color: "var(--text-dim)",
          padding: "1px 6px", borderRadius: 3, background: "rgba(255,255,255,0.04)",
          fontFamily: "SF Mono, Menlo, monospace",
        }}>{badge}</span>}
      </button>
      {open && <div style={{ padding: "0 14px 10px" }}>{children}</div>}
    </div>
  );
}


function KV({ k, v, mono, copy }) {
  if (v == null || v === "") return null;
  return (
    <div style={{ display: "flex", gap: 8, padding: "3px 0", fontSize: 12 }}>
      <div style={{ color: "var(--text-dim)", minWidth: 110, flex: "0 0 auto" }}>{k}</div>
      <div style={{
        flex: 1, color: "var(--text)", wordBreak: "break-word",
        fontFamily: mono ? "SF Mono, Menlo, monospace" : "inherit",
        display: "flex", alignItems: "center", gap: 4,
      }}>
        <span style={{ flex: 1, minWidth: 0, overflow: "hidden", textOverflow: "ellipsis" }}>{v}</span>
        {copy && typeof v === "string" && (
          <button
            onClick={() => navigator.clipboard.writeText(v)}
            style={{
              background: "none", border: "none", cursor: "pointer",
              color: "var(--text-dim)", padding: 2,
            }}
            title="Copy"
          >
            <Copy size={11} />
          </button>
        )}
      </div>
    </div>
  );
}


function Pill({ color, text }) {
  const palette = {
    blue:   { bg: "rgba(31,217,254,0.10)", fg: "var(--vast-blue)" },
    red:    { bg: "rgba(255,80,80,0.10)",  fg: "var(--danger)"    },
    yellow: { bg: "rgba(255,193,7,0.10)",  fg: "var(--warning)"   },
  }[color] || { bg: "rgba(255,255,255,0.04)", fg: "var(--text-dim)" };
  return <span style={{
    padding: "2px 8px", borderRadius: 3, background: palette.bg, color: palette.fg,
    fontSize: 11, fontWeight: 600, fontFamily: "SF Mono, Menlo, monospace",
  }}>{text}</span>;
}


function Stat({ label, value, highlight }) {
  return (
    <div style={{ minWidth: 70, textAlign: "right" }}>
      <div style={{
        fontSize: 18, fontWeight: 700,
        color: highlight ? "var(--vast-blue)" : "var(--text)",
        fontFamily: "SF Mono, Menlo, monospace",
      }}>{value}</div>
      <div style={{ fontSize: 10, color: "var(--text-dim)", textTransform: "uppercase", letterSpacing: 0.5 }}>{label}</div>
    </div>
  );
}


function basename(p) {
  if (!p) return null;
  const i = p.lastIndexOf("/");
  return i >= 0 ? p.slice(i + 1) : p;
}

function ErrorBox({ text }) {
  return (
    <div className="card" style={{
      padding: 14, background: "rgba(255,80,80,0.10)",
      color: "var(--danger)", border: "1px solid rgba(255,80,80,0.3)",
    }}>
      <AlertCircle size={14} style={{ verticalAlign: "text-bottom", marginRight: 6 }} />
      {text}
    </div>
  );
}
