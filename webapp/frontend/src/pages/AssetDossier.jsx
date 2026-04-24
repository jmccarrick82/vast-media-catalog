import React, { useEffect, useState } from "react";
import { useParams, Link, useNavigate } from "react-router-dom";
import { getAssetFullDetail, videoURL } from "../api";
import DataTable from "../components/DataTable";
import Graph from "../components/Graph";

const TABS = [
  { key: "overview", label: "Overview" },
  { key: "subclips", label: "Subclips" },
  { key: "relationships", label: "Relationships" },
  { key: "hash_matches", label: "Duplicates" },
  { key: "version_history", label: "Versions" },
  { key: "talent_music", label: "Talent & Music" },
  { key: "gdpr_personal_data", label: "GDPR" },
  { key: "syndication_records", label: "Syndication" },
  { key: "production_entities", label: "Production" },
];

export default function AssetDossier() {
  const { assetId } = useParams();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [tab, setTab] = useState("overview");

  useEffect(() => {
    setLoading(true);
    getAssetFullDetail(assetId)
      .then(setData)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [assetId]);

  if (loading) return <div className="loading">Querying VAST DB...</div>;
  if (!data?.asset?.data) return <div className="loading">Asset not found</div>;

  const d = data.asset.data;

  return (
    <div>
      {/* Breadcrumb + title */}
      <div className="page-header">
        <div className="breadcrumb">
          <Link to="/">Assets</Link>
          <span>/</span>
          <span>{d.filename || assetId.slice(0, 12)}</span>
        </div>
        <h1>{d.filename || "Asset Detail"}</h1>
        <p>{d.s3_path}</p>
      </div>

      {/* Summary metrics strip */}
      <MetricsStrip d={d} data={data} />

      {/* Tab bar */}
      <div className="tab-bar">
        {TABS.map(({ key, label }) => {
          let count =
            key === "overview" ? null : data[key]?.count ?? 0;
          if (key === "hash_matches" && data[key]?.rows) {
            const seen = new Set();
            for (const r of data[key].rows) {
              seen.add(r.asset_a_id === assetId ? r.asset_b_id : r.asset_a_id);
            }
            count = seen.size;
          }
          return (
            <button
              key={key}
              className={`tab-btn ${tab === key ? "active" : ""}`}
              onClick={() => setTab(key)}
            >
              {label}
              {count !== null && <span className="tab-count">{count}</span>}
            </button>
          );
        })}
      </div>

      {/* Tab content */}
      <div style={{ marginTop: 20 }}>
        {tab === "overview" && <OverviewTab d={d} />}
        {tab === "subclips" && (
          <SubclipsTab section={data.subclips} />
        )}
        {tab === "relationships" && (
          <RelationshipsTab
            rels={data.relationships}
            assetId={assetId}
            filename={d.filename}
          />
        )}
        {tab === "hash_matches" && (
          <DuplicatesTab section={data.hash_matches} assetId={assetId} filename={d.filename} />
        )}
        {tab !== "overview" && tab !== "relationships" && tab !== "subclips" && tab !== "hash_matches" && (
          <SecondaryTab section={data[tab]} label={TABS.find((t) => t.key === tab)?.label} />
        )}
      </div>
    </div>
  );
}

/* ── Summary Metrics Strip ────────────────────────────── */

function MetricsStrip({ d, data }) {
  const metrics = [
    {
      label: "File Size",
      value: d.file_size_bytes ? formatBytes(d.file_size_bytes) : null,
    },
    {
      label: "Duration",
      value: d.duration_seconds ? `${Number(d.duration_seconds).toFixed(1)}s` : null,
    },
    { label: "Classification", value: d.asset_classification },
    {
      label: "AI Probability",
      value: d.ai_probability != null ? `${(Number(d.ai_probability) * 100).toFixed(0)}%` : null,
      color:
        d.ai_probability > 0.7
          ? "var(--danger)"
          : d.ai_probability > 0.3
          ? "var(--warning)"
          : "var(--success)",
    },
    { label: "Value Tier", value: d.value_tier },
    {
      label: "Relationships",
      value: data.relationships?.count ?? 0,
      color: "var(--vast-blue)",
    },
    {
      label: "Duplicates",
      value: data.hash_matches?.count ?? 0,
      color: data.hash_matches?.count > 0 ? "var(--warning)" : undefined,
    },
    {
      label: "Conflict",
      value: d.conflict_detected ? "Yes" : "No",
      color: d.conflict_detected ? "var(--danger)" : "var(--success)",
    },
  ];

  return (
    <div className="metrics-strip">
      {metrics.map((m) =>
        m.value != null ? (
          <div key={m.label} className="metric-pill">
            <span className="metric-label">{m.label}</span>
            <span className="metric-value" style={m.color ? { color: m.color } : undefined}>
              {typeof m.value === "number" ? m.value.toLocaleString() : m.value}
            </span>
          </div>
        ) : null
      )}
    </div>
  );
}

/* ── Overview Tab ─────────────────────────────────────── */

function OverviewTab({ d }) {
  const sections = groupFields(d);
  return (
    <>
      {sections.map((section) => (
        <div key={section.title} style={{ marginBottom: 32 }}>
          <h3 style={{ marginBottom: 12 }}>{section.title}</h3>
          <div className="card" style={{ overflow: "auto" }}>
            <table className="data-table">
              <tbody>
                {section.fields.map(([k, v]) => (
                  <tr key={k}>
                    <td style={{ fontWeight: 600, width: 260, color: "var(--text-dim)" }}>
                      {k.replace(/_/g, " ")}
                    </td>
                    <td>{formatVal(v)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      ))}
    </>
  );
}

/* ── Relationships Tab ────────────────────────────────── */

function RelationshipsTab({ rels, assetId, filename }) {
  if (!rels?.rows?.length) {
    return <EmptyState label="No relationships found for this asset." />;
  }

  // Build a lookup from asset_id → filename using the joined data
  const nameMap = {};
  nameMap[assetId] = filename;
  rels.rows.forEach((r) => {
    if (r.parent_filename) nameMap[r.parent_asset_id] = r.parent_filename;
    if (r.child_filename) nameMap[r.child_asset_id] = r.child_filename;
  });

  // Build graph
  const nodeSet = new Set();
  const graphLinks = [];
  rels.rows.forEach((r) => {
    nodeSet.add(r.parent_asset_id);
    nodeSet.add(r.child_asset_id);
    graphLinks.push({
      source: r.parent_asset_id,
      target: r.child_asset_id,
      label: r.relationship_type,
    });
  });
  nodeSet.add(assetId);
  const graphNodes = [...nodeSet].map((id) => ({
    id,
    label: nameMap[id] || id.slice(0, 12),
    color: id === assetId ? "#1fd9fe" : "#8b9ab5",
  }));

  return (
    <>
      {graphLinks.length > 0 && (
        <div style={{ marginBottom: 32 }}>
          <h3 style={{ marginBottom: 12 }}>Relationship Graph</h3>
          <Graph nodes={graphNodes} links={graphLinks} />
        </div>
      )}
      <div className="card" style={{ overflow: "auto" }}>
        <DataTable columns={rels.columns} rows={rels.rows} />
      </div>
    </>
  );
}

/* ── Subclips Tab ────────────────────────────────────── */

function SubclipsTab({ section }) {
  if (!section?.rows?.length) {
    return <EmptyState label="No subclips found for this asset." />;
  }
  const analyzedCount = section.rows.filter((r) => r.ai_analyzed_at).length;
  return (
    <>
      <div style={{ marginBottom: 12, fontSize: 13, color: "var(--text-dim)" }}>
        {section.count} subclip{section.count !== 1 ? "s" : ""}
        {analyzedCount > 0 && (
          <span style={{ marginLeft: 12, color: "var(--vast-blue)" }}>
            · {analyzedCount} with AI analysis
          </span>
        )}
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
        {section.rows.map((r) => (
          <SubclipCard key={r.asset_id} r={r} />
        ))}
      </div>
    </>
  );
}

function SubclipCard({ r }) {
  const nav = useNavigate();
  const dur = r.duration_seconds ?? r.subclip_duration_seconds;
  const idx = r.subclip_index != null ? Number(r.subclip_index) + 1 : null;
  const start = r.subclip_start_seconds != null ? `${Number(r.subclip_start_seconds).toFixed(1)}s` : null;
  const hasAi = !!r.ai_analyzed_at;

  const pill = (label, value, color) =>
    value ? (
      <span
        className="metric-pill"
        style={{
          padding: "4px 10px",
          fontSize: 12,
          background: "var(--bg-raised, #0f1a33)",
          borderRadius: 999,
          color: color || "var(--text)",
          display: "inline-flex",
          gap: 6,
        }}
      >
        <span style={{ color: "var(--text-dim)" }}>{label}</span>
        <strong>{value}</strong>
      </span>
    ) : null;

  return (
    <div
      className="card"
      style={{ padding: 16, cursor: "pointer" }}
      onClick={() => nav(`/assets/${r.asset_id}`)}
    >
      {/* Two-column: video on the left, metadata on the right */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: r.s3_path ? "minmax(240px, 320px) 1fr" : "1fr",
          gap: 16,
          alignItems: "start",
        }}
      >
        {r.s3_path && (
          <div
            style={{ background: "#000", borderRadius: 6, overflow: "hidden", alignSelf: "start" }}
            onClick={(e) => e.stopPropagation()}
          >
            <video
              src={videoURL(r.s3_path)}
              controls
              preload="metadata"
              style={{ width: "100%", display: "block", maxHeight: 220 }}
            />
          </div>
        )}

        <div>
      {/* Header row */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 12, marginBottom: 8 }}>
        <div>
          <span style={{ fontSize: 13, color: "var(--text-dim)", marginRight: 8 }}>#{idx ?? "—"}</span>
          <span style={{ fontSize: 15, fontWeight: 600, color: "var(--vast-blue)" }}>{r.filename || "—"}</span>
        </div>
        <div style={{ fontSize: 12, color: "var(--text-dim)" }}>
          {start && <span>start {start}</span>}
          {dur != null && <span style={{ marginLeft: 12 }}>{Number(dur).toFixed(1)}s</span>}
          {r.file_size_bytes && <span style={{ marginLeft: 12 }}>{formatBytes(r.file_size_bytes)}</span>}
          {r.width && r.height && <span style={{ marginLeft: 12 }}>{r.width}×{r.height}</span>}
        </div>
      </div>

      {/* AI analysis block */}
      {hasAi ? (
        <>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginBottom: 10 }}>
            {pill("category", r.content_category)}
            {pill("mood", r.content_mood)}
            {pill("rating", r.content_rating)}
            {pill(
              "safety",
              r.content_safety_rating,
              r.content_safety_rating && r.content_safety_rating.toLowerCase() !== "safe"
                ? "var(--warning)"
                : "var(--success)"
            )}
          </div>

          {r.content_summary && (
            <div style={{ fontSize: 13, color: "var(--text)", marginBottom: 8, lineHeight: 1.5 }}>
              {r.content_summary}
            </div>
          )}

          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))", gap: 12, marginTop: 10 }}>
            <AiField label="Scene" value={r.scene_description} />
            <AiField label="OCR Text" value={r.ocr_text} mono />
            <AiField label="Transcript" value={r.transcript} mono />
            <AiField label="Content Tags" value={formatListish(r.content_tags)} />
            <AiField label="Keywords" value={formatListish(r.searchable_keywords)} />
          </div>
        </>
      ) : (
        <div style={{ fontSize: 12, color: "var(--text-dim)", fontStyle: "italic" }}>
          AI analysis pending
        </div>
      )}
        </div>
      </div>
    </div>
  );
}

function AiField({ label, value, mono }) {
  if (!value) return null;
  return (
    <div>
      <div style={{ fontSize: 11, color: "var(--text-dim)", textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 4 }}>
        {label}
      </div>
      <div
        style={{
          fontSize: 12,
          color: "var(--text)",
          fontFamily: mono ? "var(--font-mono, ui-monospace, monospace)" : undefined,
          lineHeight: 1.5,
          maxHeight: 120,
          overflow: "auto",
          whiteSpace: "pre-wrap",
          wordBreak: "break-word",
        }}
      >
        {value}
      </div>
    </div>
  );
}

function formatListish(v) {
  if (v == null) return null;
  if (Array.isArray(v)) return v.join(", ");
  if (typeof v === "string") {
    // Could be JSON array literal or comma-separated already
    const s = v.trim();
    if (s.startsWith("[") && s.endsWith("]")) {
      try {
        const arr = JSON.parse(s);
        if (Array.isArray(arr)) return arr.join(", ");
      } catch {}
    }
    return s;
  }
  return String(v);
}

/* ── Duplicates Tab ─────────────────────────────────── */

function DuplicatesTab({ section, assetId, filename }) {
  const nav = useNavigate();

  if (!section?.rows?.length) {
    return <EmptyState label="No duplicates found for this asset." />;
  }

  // Deduplicate rows — collect unique duplicate file paths
  const seen = new Set();
  const dupPaths = [];
  for (const r of section.rows) {
    const otherId = r.asset_a_id === assetId ? r.asset_b_id : r.asset_a_id;
    const otherName = r.asset_a_id === assetId ? r.asset_b_filename : r.asset_a_filename;
    if (seen.has(otherId)) continue;
    seen.add(otherId);
    dupPaths.push({ id: otherId, filename: otherName || otherId.slice(0, 12) });
  }

  return (
    <>
      <div style={{ marginBottom: 8, fontSize: 13, color: "var(--text-dim)" }}>
        {dupPaths.length} duplicate{dupPaths.length !== 1 ? "s" : ""} of{" "}
        <strong style={{ color: "var(--text)" }}>{filename || assetId.slice(0, 12)}</strong>
      </div>
      <div className="card" style={{ overflow: "auto" }}>
        <table className="data-table">
          <thead>
            <tr>
              <th>Duplicate File</th>
              <th style={{ width: 140 }}>Total Duplicates</th>
            </tr>
          </thead>
          <tbody>
            {dupPaths.map((dup) => (
              <tr
                key={dup.id}
                style={{ cursor: "pointer" }}
                onClick={() => nav(`/assets/${dup.id}`)}
              >
                <td style={{ color: "var(--vast-blue)" }}>{dup.filename}</td>
                <td>{dupPaths.length}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

/* ── Generic Secondary Tab ────────────────────────────── */

function SecondaryTab({ section, label }) {
  if (!section?.rows?.length) {
    return <EmptyState label={`No ${label?.toLowerCase() || "records"} found for this asset.`} />;
  }
  return (
    <>
      <div style={{ marginBottom: 8, fontSize: 13, color: "var(--text-dim)" }}>
        {section.count} record{section.count !== 1 ? "s" : ""}
      </div>
      <div className="card" style={{ overflow: "auto" }}>
        <DataTable columns={section.columns} rows={section.rows} />
      </div>
    </>
  );
}

/* ── Empty state ──────────────────────────────────────── */

function EmptyState({ label }) {
  return (
    <div
      style={{
        padding: 48,
        textAlign: "center",
        color: "var(--text-dim)",
        fontSize: 14,
      }}
    >
      {label}
    </div>
  );
}

/* ── Helpers ──────────────────────────────────────────── */

function formatBytes(bytes) {
  if (!bytes) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  let n = Number(bytes);
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024;
    i++;
  }
  return `${n.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

function formatVal(v) {
  if (v === null || v === undefined) return "\u2014";
  if (typeof v === "boolean") return v ? "Yes" : "No";
  if (typeof v === "number") return v.toLocaleString();
  return String(v);
}

function groupFields(data) {
  const foundation = [
    "asset_id", "s3_path", "filename", "file_size_bytes", "duration_seconds",
    "video_codec", "audio_codec", "width", "height", "fps", "bitrate",
    "container_format", "creation_date", "color_space", "audio_channels",
    "ingested_at", "sha256", "perceptual_hash", "hash_computed_at",
  ];
  const rights = [
    "conflict_detected", "license_type", "territories", "restrictions",
    "rights_expiry", "rights_checked_at", "orphan_status", "orphan_resolved_from_asset_id",
    "orphan_resolved_at", "license_audit_licensor", "license_audit_usage_type",
    "license_audit_derivative_count", "license_audit_at",
  ];
  const classification = [
    "asset_classification", "classification_confidence", "parent_asset_id",
    "dependent_count", "is_leaf", "is_root", "deletion_safe", "deletion_evaluated_at",
    "duplicate_count", "storage_savings_bytes",
  ];
  const ai = [
    "ai_probability", "ai_tool_detected", "ai_model_version", "ai_detection_method",
    "ai_detected_at", "training_dataset_id", "is_original_training_data",
    "training_rights_cleared", "processing_chain", "contamination_risk",
    "contamination_depth",
  ];
  const aiAnalysis = [
    "content_summary", "content_category", "content_mood", "content_rating",
    "content_safety_rating", "content_tags", "searchable_keywords",
    "scene_description", "ocr_text", "transcript", "ai_content_assessment",
    "ai_probability_vision", "ai_analyzed_at",
  ];
  const security = [
    "legal_hold_active", "legal_hold_id", "legal_hold_date",
    "sha256_at_hold", "integrity_verified", "delivery_chain",
    "delivery_recipient", "delivery_date", "leak_hash_fingerprint",
    "recovery_priority", "is_unique_original", "has_backup",
    "surviving_derivatives_count",
  ];
  const business = [
    "commercial_value_score", "value_tier", "reuse_count",
    "replacement_cost_tier", "is_irreplaceable", "digital_copy_count",
    "commercial_history_score",
  ];

  const all = Object.entries(data).filter(
    ([, v]) => v !== null && v !== undefined
  );
  const inSet = (keys) => all.filter(([k]) => keys.includes(k));
  const remaining = all.filter(
    ([k]) =>
      ![...foundation, ...rights, ...classification, ...ai, ...aiAnalysis, ...security, ...business].includes(k)
  );

  const sections = [];
  const f = inSet(foundation);
  if (f.length) sections.push({ title: "Foundation Metadata", fields: f });
  const r = inSet(rights);
  if (r.length) sections.push({ title: "Rights & Licensing", fields: r });
  const c = inSet(classification);
  if (c.length) sections.push({ title: "Classification & Structure", fields: c });
  const aa = inSet(aiAnalysis);
  if (aa.length) sections.push({ title: "AI Content Analysis", fields: aa });
  const a = inSet(ai);
  if (a.length) sections.push({ title: "AI & Training Provenance", fields: a });
  const s = inSet(security);
  if (s.length) sections.push({ title: "Security & Legal", fields: s });
  const b = inSet(business);
  if (b.length) sections.push({ title: "Business & Valuation", fields: b });
  if (remaining.length) sections.push({ title: "Other Metadata", fields: remaining });

  return sections;
}
