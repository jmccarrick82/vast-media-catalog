import React, { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import {
  Package, Shield, ShieldCheck, ShieldAlert, Video, Film, Clock,
  AlertCircle,
} from "lucide-react";
import { listPackages } from "../api";

/**
 * /packages — grid of delivery packages. Each card shows the source
 * filename, clip/rendition counts, C2PA signing status, and links
 * into /packages/<id> for the full detail view with video player
 * and per-rendition provenance.
 */
export default function PackagesPage() {
  const [packages, setPackages] = useState(null);
  const [err, setErr] = useState(null);
  const [refreshedAt, setRefreshedAt] = useState(0);

  useEffect(() => {
    let alive = true;
    setErr(null);
    listPackages()
      .then((d) => { if (alive) setPackages(d.packages || []); })
      .catch((e) => { if (alive) setErr(String(e)); });
    return () => { alive = false; };
  }, [refreshedAt]);

  return (
    <div>
      <div className="page-header">
        <h1 style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <Package size={24} style={{ color: "var(--vast-blue)" }} />
          Delivery Packages
        </h1>
        <p>
          Each package wraps one source video's clips in every configured
          rendition, with a JSON sidecar manifest and{" "}
          <strong style={{ color: "var(--vast-blue)" }}>C2PA-signed provenance</strong>{" "}
          embedded in every MP4. Open a package to inspect the clips,
          verify signatures, and see the full AI disclosure.
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

      {!packages && !err && <div className="loading">Loading packages…</div>}

      {packages && packages.length === 0 && (
        <div className="card" style={{ padding: 24, textAlign: "center" }}>
          <Package size={48} style={{ color: "var(--text-dim)", opacity: 0.5 }} />
          <p style={{ color: "var(--text-dim)", marginTop: 12 }}>
            No delivery packages yet. Upload a video to{" "}
            <code>s3://james-media-inbox/</code> with an{" "}
            <code>x-amz-meta-clip-prompt</code> header to kick off the pipeline.
          </p>
        </div>
      )}

      {packages && packages.length > 0 && (
        <div style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fill, minmax(320px, 1fr))",
          gap: 14,
        }}>
          {packages.map((p) => <PackageCard key={p.package_id} pkg={p} />)}
        </div>
      )}

      <div style={{ marginTop: 24, textAlign: "center" }}>
        <button
          onClick={() => setRefreshedAt(Date.now())}
          className="picker-btn picker-btn-dim"
          style={{ fontSize: 12 }}
        >
          Refresh
        </button>
      </div>
    </div>
  );
}


function PackageCard({ pkg }) {
  const signed = pkg.c2pa_signed_count || 0;
  const total  = pkg.rendition_count  || 0;
  const allSigned = pkg.c2pa_enabled && total > 0 && signed === total;
  const partial   = pkg.c2pa_enabled && signed > 0 && signed < total;
  const unsigned  = !pkg.c2pa_enabled || signed === 0;

  const c2paIcon =
    allSigned ? <ShieldCheck size={14} style={{ color: "var(--vast-blue)" }} /> :
    partial   ? <ShieldAlert size={14} style={{ color: "var(--warning)" }} /> :
                <Shield size={14} style={{ color: "var(--text-dim)" }} />;
  const c2paLabel =
    allSigned ? `C2PA-signed (${signed}/${total})` :
    partial   ? `C2PA partial (${signed}/${total})` :
                "Unsigned";

  return (
    <Link
      to={`/packages/${pkg.package_id}`}
      className="card"
      style={{
        padding: 16, textDecoration: "none", color: "inherit",
        display: "block",
        transition: "transform 120ms ease, border-color 120ms ease",
      }}
    >
      <div style={{ display: "flex", alignItems: "start", justifyContent: "space-between", gap: 10 }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{
            fontSize: 12, color: "var(--text-dim)",
            fontFamily: "SF Mono, Menlo, monospace",
            overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
          }}>
            {pkg.package_id}
          </div>
          <div style={{
            fontSize: 15, fontWeight: 600, color: "var(--text)",
            marginTop: 4, marginBottom: 8,
            overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
          }}>
            {pkg.source_filename || (pkg.source_id ? <span style={{ color: "var(--text-dim)" }}>source {pkg.source_id.slice(0, 8)}…</span> : <em style={{ color: "var(--text-dim)" }}>(no source)</em>)}
          </div>
        </div>
        <span style={{
          display: "inline-flex", alignItems: "center", gap: 4,
          padding: "3px 8px", borderRadius: 10,
          background: allSigned ? "rgba(31,217,254,0.10)"
                   : partial   ? "rgba(255,193,7,0.10)"
                               : "rgba(255,255,255,0.04)",
          color:      allSigned ? "var(--vast-blue)"
                   : partial   ? "var(--warning)"
                               : "var(--text-dim)",
          fontSize: 11, fontWeight: 600, whiteSpace: "nowrap",
        }}>
          {c2paIcon} <span>{c2paLabel}</span>
        </span>
      </div>

      <div style={{
        display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8,
        fontSize: 12, color: "var(--text-dim)", marginTop: 8,
      }}>
        <Stat icon={<Film size={12} />} label="clips" value={pkg.clip_count ?? "—"} />
        <Stat icon={<Video size={12} />} label="renditions" value={pkg.rendition_count ?? "—"} />
        <Stat icon={<Clock size={12} />} label="size" value={formatBytes(pkg.total_size_bytes)} />
      </div>

      <div style={{
        marginTop: 10, paddingTop: 10,
        borderTop: "1px solid var(--border)",
        fontSize: 11, color: "var(--text-dim)",
        display: "flex", justifyContent: "space-between",
      }}>
        <span style={{
          padding: "1px 6px", borderRadius: 3,
          background: pkg.status === "ready"  ? "rgba(31,217,254,0.08)"
                   : pkg.status === "failed" ? "rgba(255,80,80,0.12)"
                                             : "rgba(255,193,7,0.10)",
          color:      pkg.status === "ready"  ? "var(--vast-blue)"
                   : pkg.status === "failed" ? "var(--danger)"
                                             : "var(--warning)",
          fontFamily: "SF Mono, Menlo, monospace",
        }}>
          {pkg.status}
        </span>
        <span>{pkg.created_at ? new Date(pkg.created_at * 1000).toLocaleString() : ""}</span>
      </div>
    </Link>
  );
}


function Stat({ icon, label, value }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
      {icon}
      <span style={{ fontWeight: 600, color: "var(--text)" }}>{value}</span>
      <span style={{ color: "var(--text-dim)" }}>{label}</span>
    </div>
  );
}


function formatBytes(n) {
  if (n == null || n === 0) return "—";
  const u = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return `${n.toFixed(n < 10 ? 1 : 0)} ${u[i]}`;
}
