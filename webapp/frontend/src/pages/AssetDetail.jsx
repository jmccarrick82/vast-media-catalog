import React, { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import { getAssetDetail, getAssetRelationships } from "../api";
import DataTable from "../components/DataTable";
import Graph from "../components/Graph";

export default function AssetDetail() {
  const { assetId } = useParams();
  const [asset, setAsset] = useState(null);
  const [rels, setRels] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    Promise.all([getAssetDetail(assetId), getAssetRelationships(assetId)])
      .then(([a, r]) => {
        setAsset(a);
        setRels(r);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [assetId]);

  if (loading) return <div className="loading">Loading...</div>;
  if (!asset?.data) return <div className="loading">Asset not found</div>;

  const d = asset.data;

  // Build graph from relationships
  const nodeSet = new Set();
  const graphLinks = [];
  if (rels?.rows) {
    rels.rows.forEach((r) => {
      nodeSet.add(r.parent_asset_id);
      nodeSet.add(r.child_asset_id);
      graphLinks.push({
        source: r.parent_asset_id,
        target: r.child_asset_id,
        label: r.relationship_type,
      });
    });
  }
  nodeSet.add(assetId);
  const graphNodes = [...nodeSet].map((id) => ({
    id,
    label: id === assetId ? d.filename || id.slice(0, 8) : id.slice(0, 8),
    color: id === assetId ? "#1fd9fe" : "#8b9ab5",
  }));

  // Group asset fields into sections
  const sections = groupFields(d);

  return (
    <div>
      <div className="page-header">
        <div className="breadcrumb">
          <Link to="/">Home</Link>
          <span>/</span>
          <Link to="/assets">Assets</Link>
          <span>/</span>
          <span>{d.filename || assetId.slice(0, 12)}</span>
        </div>
        <h1>{d.filename || "Asset Detail"}</h1>
        <p>{d.s3_path}</p>
      </div>

      {graphLinks.length > 0 && (
        <div style={{ marginBottom: 32 }}>
          <h3 style={{ marginBottom: 12 }}>Relationship Graph</h3>
          <Graph nodes={graphNodes} links={graphLinks} />
        </div>
      )}

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
    </div>
  );
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
      ![...foundation, ...rights, ...classification, ...ai, ...security, ...business].includes(k)
  );

  const sections = [];
  const f = inSet(foundation);
  if (f.length) sections.push({ title: "Foundation Metadata", fields: f });
  const r = inSet(rights);
  if (r.length) sections.push({ title: "Rights & Licensing", fields: r });
  const c = inSet(classification);
  if (c.length) sections.push({ title: "Classification & Structure", fields: c });
  const a = inSet(ai);
  if (a.length) sections.push({ title: "AI & Training Provenance", fields: a });
  const s = inSet(security);
  if (s.length) sections.push({ title: "Security & Legal", fields: s });
  const b = inSet(business);
  if (b.length) sections.push({ title: "Business & Valuation", fields: b });
  if (remaining.length) sections.push({ title: "Other Metadata", fields: remaining });

  return sections;
}

function formatVal(v) {
  if (v === null || v === undefined) return "—";
  if (typeof v === "boolean") return v ? "Yes" : "No";
  if (typeof v === "number") return v.toLocaleString();
  return String(v);
}
