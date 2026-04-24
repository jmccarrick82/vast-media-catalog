import React, { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import { getUseCaseData } from "../api";
import DataTable from "../components/DataTable";
import MetricsCard from "../components/MetricsCard";
import Timeline from "../components/Timeline";
import Graph from "../components/Graph";
import {
  PieChart, Pie, Cell, BarChart, Bar, XAxis, YAxis, Tooltip,
  ResponsiveContainer, Legend,
} from "recharts";

const COLORS = ["#1fd9fe", "#22c55e", "#f59e0b", "#ef4444", "#8b5cf6", "#ec4899", "#06b6d4", "#84cc16"];

const UC_META = {
  1: { name: "Rights Conflict Detection", persona: "Legal & Business Affairs", personaId: 1 },
  2: { name: "Orphaned Asset Resolution", persona: "Legal & Business Affairs", personaId: 1 },
  3: { name: "Unauthorized Use Detection", persona: "Legal & Business Affairs", personaId: 1 },
  4: { name: "License Audit Trail", persona: "Legal & Business Affairs", personaId: 1 },
  5: { name: "Talent & Music Residuals", persona: "Legal & Business Affairs", personaId: 1 },
  6: { name: "Duplicate Storage Elimination", persona: "Archive & Library", personaId: 2 },
  7: { name: "Safe Deletion", persona: "Archive & Library", personaId: 2 },
  8: { name: "Master vs Derivative Classification", persona: "Archive & Library", personaId: 2 },
  9: { name: "Archive Re-Conformation", persona: "Archive & Library", personaId: 2 },
  10: { name: "Version Control Across the Lifecycle", persona: "Archive & Library", personaId: 2 },
  11: { name: "Training Data Provenance", persona: "AI & Data Science", personaId: 3 },
  12: { name: "Model Contamination Detection", persona: "AI & Data Science", personaId: 3 },
  13: { name: "Synthetic Content Tracking", persona: "AI & Data Science", personaId: 3 },
  14: { name: "Bias Audit", persona: "AI & Data Science", personaId: 3 },
  15: { name: "Re-Use Discovery", persona: "Production & Post-Production", personaId: 4 },
  16: { name: "Clearance Inheritance", persona: "Production & Post-Production", personaId: 4 },
  17: { name: "Compliance Propagation", persona: "Production & Post-Production", personaId: 4 },
  18: { name: "Localization Management", persona: "Production & Post-Production", personaId: 4 },
  19: { name: "Leak Investigation", persona: "Security & IT", personaId: 5 },
  20: { name: "Regulatory Compliance (GDPR / AI Act)", persona: "Security & IT", personaId: 5 },
  21: { name: "Chain of Custody for Legal Hold", persona: "Legal & Business Affairs", personaId: 1 },
  22: { name: "Cybersecurity — Ransomware Impact", persona: "Security & IT", personaId: 5 },
  23: { name: "Content Valuation", persona: "Business & Finance", personaId: 6 },
  24: { name: "Syndication Revenue Tracking", persona: "Business & Finance", personaId: 6 },
  25: { name: "Insurance & Disaster Recovery Valuation", persona: "Business & Finance", personaId: 6 },
  26: { name: "Co-Production Attribution", persona: "Legal & Business Affairs", personaId: 1 },
};

export default function UseCasePage() {
  const { ucId } = useParams();
  const id = parseInt(ucId);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    getUseCaseData(id)
      .then(setData)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [id]);

  const meta = UC_META[id];
  if (!meta) return <div className="loading">Use case not found</div>;

  return (
    <div>
      <div className="page-header">
        <div className="breadcrumb">
          <Link to="/">Home</Link>
          <span>/</span>
          <Link to={`/persona/${meta.personaId}`}>{meta.persona}</Link>
          <span>/</span>
          <span>UC{String(id).padStart(2, "0")}</span>
        </div>
        <h1>
          UC{String(id).padStart(2, "0")} — {meta.name}
        </h1>
      </div>

      {loading && <div className="loading">Loading data...</div>}
      {error && (
        <div className="card" style={{ color: "var(--danger)", marginBottom: 24 }}>
          Error: {error}
        </div>
      )}
      {data && <Visualization ucId={id} data={data} />}
    </div>
  );
}

function Visualization({ ucId, data }) {
  const { rows, columns, count } = data;

  switch (ucId) {
    // ── Graph visualizations ──
    case 1: return <RightsConflictGraph rows={rows} />;
    case 6: return <DuplicateGraph rows={rows} />;
    case 8: return <MasterDerivativeGraph rows={rows} />;
    case 11: return <TrainingProvenanceGraph rows={rows} />;
    case 14: return <BiasAuditGraph rows={rows} />;
    case 16: return <ClearanceGraph rows={rows} columns={columns} />;
    case 17: return <ComplianceGraph rows={rows} columns={columns} />;

    // ── Timeline visualizations ──
    case 4: return <LicenseTimeline rows={rows} />;
    case 10: return <VersionTimeline rows={rows} />;
    case 19: return <LeakTimeline rows={rows} />;
    case 21: return <CustodyTimeline rows={rows} />;

    // ── Pie / chart visualizations ──
    case 13: return <SyntheticPie rows={rows} />;
    case 26: return <CoProductionPie rows={rows} />;

    // ── Bar chart ──
    case 23: return <ValuationBar rows={rows} />;

    // ── Dashboard visualizations ──
    case 22: return <RansomwareDashboard rows={rows} />;
    case 25: return <InsuranceDashboard rows={rows} />;

    // ── Tree visualizations ──
    case 7: return <SafeDeletionTree rows={rows} />;
    case 18: return <LocalizationTree rows={rows} />;

    // ── Table visualizations (default) ──
    default:
      return (
        <div>
          <p style={{ color: "var(--text-dim)", marginBottom: 16 }}>
            {count} records
          </p>
          <DataTable columns={columns} rows={rows} />
        </div>
      );
  }
}

// ── UC01: Rights Conflict Detection (Graph) ──

function RightsConflictGraph({ rows }) {
  const conflicting = rows.filter((r) => r.conflict_detected);
  const clean = rows.filter((r) => !r.conflict_detected);

  const metrics = [
    { label: "Total Assets", value: rows.length },
    { label: "Conflicts", value: conflicting.length, color: "var(--danger)" },
    { label: "Clear", value: clean.length, color: "var(--success)" },
  ];

  return (
    <div>
      <MetricsCard items={metrics} />
      <div style={{ marginTop: 24 }}>
        <DataTable
          columns={["asset_id", "filename", "license_type", "territories", "restrictions", "conflict_detected"]}
          rows={rows}
        />
      </div>
    </div>
  );
}

// ── UC06: Duplicate Graph ──

function DuplicateGraph({ rows }) {
  const nodes = new Map();
  const links = [];

  rows.forEach((r) => {
    if (!nodes.has(r.asset_a_id)) nodes.set(r.asset_a_id, { id: r.asset_a_id, label: (r.asset_a_filename || r.asset_a_id || "").slice(0, 12) });
    if (!nodes.has(r.asset_b_id)) nodes.set(r.asset_b_id, { id: r.asset_b_id, label: (r.asset_b_filename || r.asset_b_id || "").slice(0, 12) });
    links.push({ source: r.asset_a_id, target: r.asset_b_id });
  });

  const totalSavings = rows.reduce((s, r) => s + (r.storage_savings_bytes || 0), 0);

  return (
    <div>
      <MetricsCard
        items={[
          { label: "Duplicate Pairs", value: rows.length },
          { label: "Storage Savings", value: formatBytes(totalSavings), color: "var(--success)" },
        ]}
      />
      {nodes.size > 0 && (
        <div style={{ marginTop: 24 }}>
          <Graph nodes={[...nodes.values()]} links={links} />
        </div>
      )}
      <div style={{ marginTop: 24 }}>
        <DataTable columns={["asset_a_id", "asset_b_id", "match_type", "similarity_score", "storage_savings_bytes"]} rows={rows} />
      </div>
    </div>
  );
}

// ── UC08: Master/Derivative Graph ──

function MasterDerivativeGraph({ rows }) {
  const nodes = new Map();
  const links = [];

  rows.forEach((r) => {
    const id = r.asset_id;
    if (!nodes.has(id))
      nodes.set(id, {
        id,
        label: (r.filename || id).slice(0, 12),
        color: r.asset_classification === "master" ? "#22c55e" : r.asset_classification === "derivative" ? "#f59e0b" : "#1fd9fe",
      });
    if (r.parent_asset_id && r.parent_asset_id !== id) {
      if (!nodes.has(r.parent_asset_id))
        nodes.set(r.parent_asset_id, { id: r.parent_asset_id, label: (r.parent_filename || r.parent_asset_id).slice(0, 12), color: "#22c55e" });
      links.push({ source: r.parent_asset_id, target: id });
    }
  });

  const masters = rows.filter((r) => r.asset_classification === "master").length;
  const derivatives = rows.filter((r) => r.asset_classification === "derivative").length;

  return (
    <div>
      <MetricsCard
        items={[
          { label: "Total Assets", value: rows.length },
          { label: "Masters", value: masters, color: "var(--success)" },
          { label: "Derivatives", value: derivatives, color: "var(--warning)" },
        ]}
      />
      {nodes.size > 0 && (
        <div style={{ marginTop: 24 }}>
          <Graph nodes={[...nodes.values()]} links={links} />
        </div>
      )}
      <div style={{ marginTop: 24 }}>
        <DataTable columns={["asset_id", "filename", "asset_classification", "classification_confidence", "parent_asset_id"]} rows={rows} />
      </div>
    </div>
  );
}

// ── UC11: Training Data Provenance (Graph) ──

function TrainingProvenanceGraph({ rows }) {
  const nodes = new Map();
  const links = [];

  rows.forEach((r) => {
    const id = r.asset_id;
    if (!nodes.has(id)) nodes.set(id, { id, label: (r.filename || id).slice(0, 12), color: r.is_original_training_data ? "#22c55e" : "#f59e0b" });
    if (r.training_dataset_id) {
      const dsId = `ds-${r.training_dataset_id}`;
      if (!nodes.has(dsId)) nodes.set(dsId, { id: dsId, label: `Dataset ${r.training_dataset_id}`, color: "#8b5cf6" });
      links.push({ source: id, target: dsId });
    }
  });

  return (
    <div>
      <MetricsCard
        items={[
          { label: "Training Assets", value: rows.length },
          { label: "Original Data", value: rows.filter((r) => r.is_original_training_data).length, color: "var(--success)" },
          { label: "Rights Cleared", value: rows.filter((r) => r.training_rights_cleared).length },
        ]}
      />
      {nodes.size > 0 && <div style={{ marginTop: 24 }}><Graph nodes={[...nodes.values()]} links={links} /></div>}
      <div style={{ marginTop: 24 }}>
        <DataTable columns={["asset_id", "filename", "training_dataset_id", "is_original_training_data", "training_rights_cleared", "processing_chain"]} rows={rows} />
      </div>
    </div>
  );
}

// ── UC14: Bias Audit (Graph) ──

function BiasAuditGraph({ rows }) {
  return (
    <div>
      <MetricsCard
        items={[
          { label: "Audited Assets", value: rows.length },
          { label: "High Risk", value: rows.filter((r) => r.bias_risk_level === "high").length, color: "var(--danger)" },
          { label: "Low Risk", value: rows.filter((r) => r.bias_risk_level === "low").length, color: "var(--success)" },
        ]}
      />
      <div style={{ marginTop: 24 }}>
        <DataTable columns={["asset_id", "filename", "bias_audit_result", "bias_risk_level", "bias_model_id", "ai_tool_detected", "training_data_ids_audited"]} rows={rows} />
      </div>
    </div>
  );
}

// ── UC16: Clearance Inheritance (Graph) ──

function ClearanceGraph({ rows, columns }) {
  const nodes = new Map();
  const links = [];

  rows.forEach((r) => {
    const id = r.asset_id;
    if (!nodes.has(id))
      nodes.set(id, { id, label: (r.filename || id).slice(0, 12), color: r.clearance_status === "cleared" ? "#22c55e" : "#f59e0b" });
    if (r.clearance_inherited_from) {
      if (!nodes.has(r.clearance_inherited_from))
        nodes.set(r.clearance_inherited_from, { id: r.clearance_inherited_from, label: (r.parent_clearance_status || r.clearance_inherited_from).slice(0, 12), color: "#22c55e" });
      links.push({ source: r.clearance_inherited_from, target: id });
    }
  });

  return (
    <div>
      <MetricsCard items={[{ label: "Assets with Clearance Data", value: rows.length }]} />
      {nodes.size > 0 && <div style={{ marginTop: 24 }}><Graph nodes={[...nodes.values()]} links={links} /></div>}
      <div style={{ marginTop: 24 }}><DataTable columns={columns} rows={rows} /></div>
    </div>
  );
}

// ── UC17: Compliance Propagation (Graph) ──

function ComplianceGraph({ rows, columns }) {
  const nodes = new Map();
  const links = [];

  rows.forEach((r) => {
    const id = r.asset_id;
    const rating = r.compliance_rating || "unknown";
    const color = rating === "compliant" ? "#22c55e" : rating === "non_compliant" ? "#ef4444" : "#f59e0b";
    if (!nodes.has(id)) nodes.set(id, { id, label: (r.filename || id).slice(0, 12), color });
    if (r.compliance_inherited_from) {
      if (!nodes.has(r.compliance_inherited_from))
        nodes.set(r.compliance_inherited_from, { id: r.compliance_inherited_from, label: r.compliance_inherited_from.slice(0, 12), color: "#8b9ab5" });
      links.push({ source: r.compliance_inherited_from, target: id });
    }
  });

  return (
    <div>
      <MetricsCard items={[{ label: "Assets with Compliance Data", value: rows.length }]} />
      {nodes.size > 0 && <div style={{ marginTop: 24 }}><Graph nodes={[...nodes.values()]} links={links} /></div>}
      <div style={{ marginTop: 24 }}><DataTable columns={columns} rows={rows} /></div>
    </div>
  );
}

// ── UC04: License Audit Trail (Timeline) ──

function LicenseTimeline({ rows }) {
  const items = rows.map((r) => ({
    timestamp: r.license_audit_at,
    label: `${r.filename || r.asset_id?.slice(0, 8)} — ${r.license_audit_licensor || "Unknown"}`,
    detail: `${r.license_audit_usage_type || ""} · ${r.license_audit_derivative_count ?? 0} derivatives`,
  }));

  return (
    <div>
      <MetricsCard items={[{ label: "Audit Records", value: rows.length }]} />
      <div className="card" style={{ marginTop: 24, padding: 32 }}>
        <Timeline items={items} labelKey="label" timeKey="timestamp" detailKey="detail" />
      </div>
    </div>
  );
}

// ── UC10: Version Timeline ──

function VersionTimeline({ rows }) {
  const items = rows.map((r) => ({
    timestamp: r.created_at,
    label: `v${r.version_number ?? "?"} — ${r.version_label || r.filename || ""}`,
    detail: r.prev_filename ? `Previous: ${r.prev_filename}` : null,
  }));

  return (
    <div>
      <MetricsCard items={[{ label: "Version Records", value: rows.length }]} />
      <div className="card" style={{ marginTop: 24, padding: 32 }}>
        <Timeline items={items} labelKey="label" timeKey="timestamp" detailKey="detail" />
      </div>
    </div>
  );
}

// ── UC19: Leak Investigation (Timeline) ──

function LeakTimeline({ rows }) {
  const items = rows.map((r) => ({
    timestamp: r.delivery_date,
    label: `${r.filename || r.asset_id?.slice(0, 8)} → ${r.delivery_recipient || "Unknown"}`,
    detail: r.delivery_chain,
  }));

  return (
    <div>
      <MetricsCard items={[{ label: "Delivery Records", value: rows.length }]} />
      <div className="card" style={{ marginTop: 24, padding: 32 }}>
        <Timeline items={items} labelKey="label" timeKey="timestamp" detailKey="detail" />
      </div>
    </div>
  );
}

// ── UC21: Chain of Custody (Timeline) ──

function CustodyTimeline({ rows }) {
  const items = rows.map((r) => ({
    timestamp: r.legal_hold_date,
    label: `Hold ${r.legal_hold_id || "?"} — ${r.filename || r.asset_id?.slice(0, 8)}`,
    detail: `Integrity: ${r.integrity_status || "Unknown"} · SHA256 match: ${r.sha256_at_hold === r.sha256 ? "Yes" : "No"}`,
  }));

  const intact = rows.filter((r) => r.integrity_status === "INTACT").length;
  const modified = rows.filter((r) => r.integrity_status === "MODIFIED").length;

  return (
    <div>
      <MetricsCard
        items={[
          { label: "Legal Holds", value: rows.length },
          { label: "Intact", value: intact, color: "var(--success)" },
          { label: "Modified", value: modified, color: "var(--danger)" },
        ]}
      />
      <div className="card" style={{ marginTop: 24, padding: 32 }}>
        <Timeline items={items} labelKey="label" timeKey="timestamp" detailKey="detail" />
      </div>
    </div>
  );
}

// ── UC13: Synthetic Content (Pie) ──

function SyntheticPie({ rows }) {
  const high = rows.filter((r) => r.ai_probability > 0.7).length;
  const medium = rows.filter((r) => r.ai_probability > 0.3 && r.ai_probability <= 0.7).length;
  const low = rows.filter((r) => r.ai_probability <= 0.3).length;

  const pieData = [
    { name: "AI-Generated (>70%)", value: high },
    { name: "Uncertain (30-70%)", value: medium },
    { name: "Likely Organic (<30%)", value: low },
  ].filter((d) => d.value > 0);

  return (
    <div>
      <MetricsCard
        items={[
          { label: "Total Scanned", value: rows.length },
          { label: "AI-Generated", value: high, color: "var(--danger)" },
          { label: "Organic", value: low, color: "var(--success)" },
        ]}
      />
      <div style={{ display: "flex", gap: 24, marginTop: 24, flexWrap: "wrap" }}>
        <div className="card" style={{ flex: "0 0 400px" }}>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie data={pieData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={100} label>
                {pieData.map((_, i) => <Cell key={i} fill={COLORS[i]} />)}
              </Pie>
              <Tooltip />
              <Legend />
            </PieChart>
          </ResponsiveContainer>
        </div>
        <div style={{ flex: 1, minWidth: 300 }}>
          <DataTable columns={["asset_id", "filename", "ai_probability", "ai_tool_detected", "ai_detection_method"]} rows={rows} />
        </div>
      </div>
    </div>
  );
}

// ── UC26: Co-Production Attribution (Pie) ──

function CoProductionPie({ rows }) {
  // Aggregate by production company
  const byCompany = {};
  rows.forEach((r) => {
    const co = r.production_company || "Unknown";
    byCompany[co] = (byCompany[co] || 0) + (r.ownership_split_pct || 0);
  });
  const pieData = Object.entries(byCompany).map(([name, value]) => ({ name, value: Math.round(value * 100) / 100 }));

  return (
    <div>
      <MetricsCard items={[{ label: "Production Records", value: rows.length }]} />
      <div style={{ display: "flex", gap: 24, marginTop: 24, flexWrap: "wrap" }}>
        <div className="card" style={{ flex: "0 0 400px" }}>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie data={pieData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={100} label>
                {pieData.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
              </Pie>
              <Tooltip />
              <Legend />
            </PieChart>
          </ResponsiveContainer>
        </div>
        <div style={{ flex: 1, minWidth: 300 }}>
          <DataTable columns={["asset_id", "filename", "production_company", "crew_origin", "ownership_split_pct", "contribution_type"]} rows={rows} />
        </div>
      </div>
    </div>
  );
}

// ── UC23: Content Valuation (Bar) ──

function ValuationBar({ rows }) {
  const chartData = rows.slice(0, 20).map((r) => ({
    name: (r.filename || r.asset_id || "").slice(0, 15),
    value: r.commercial_value_score || 0,
    tier: r.value_tier,
  }));

  return (
    <div>
      <MetricsCard
        items={[
          { label: "Valued Assets", value: rows.length },
          { label: "Premium Tier", value: rows.filter((r) => r.value_tier === "premium").length, color: "var(--vast-blue)" },
        ]}
      />
      <div className="card" style={{ marginTop: 24, padding: 24 }}>
        <ResponsiveContainer width="100%" height={400}>
          <BarChart data={chartData}>
            <XAxis dataKey="name" tick={{ fill: "#8b9ab5", fontSize: 11 }} angle={-35} textAnchor="end" height={80} />
            <YAxis tick={{ fill: "#8b9ab5", fontSize: 11 }} />
            <Tooltip contentStyle={{ background: "#0a1e3d", border: "1px solid rgba(31,217,254,0.2)" }} />
            <Bar dataKey="value" fill="#1fd9fe" radius={[4, 4, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
      <div style={{ marginTop: 24 }}>
        <DataTable columns={["asset_id", "filename", "commercial_value_score", "value_tier", "reuse_count", "derivative_count"]} rows={rows} />
      </div>
    </div>
  );
}

// ── UC22: Ransomware Impact (Dashboard) ──

function RansomwareDashboard({ rows }) {
  // rows are aggregated by recovery_priority
  const chartData = rows.map((r) => ({
    name: r.recovery_priority || "Unknown",
    total: r.total_assets || 0,
    unique: r.unique_originals || 0,
    backed_up: r.backed_up_count || 0,
  }));

  const totalAssets = rows.reduce((s, r) => s + (r.total_assets || 0), 0);
  const totalUnique = rows.reduce((s, r) => s + (r.unique_originals || 0), 0);
  const totalBackedUp = rows.reduce((s, r) => s + (r.backed_up_count || 0), 0);

  return (
    <div>
      <MetricsCard
        items={[
          { label: "Total Assets", value: totalAssets },
          { label: "Unique Originals", value: totalUnique, color: "var(--danger)" },
          { label: "Backed Up", value: totalBackedUp, color: "var(--success)" },
          { label: "At Risk", value: totalUnique - totalBackedUp, color: "var(--warning)" },
        ]}
      />
      <div className="card" style={{ marginTop: 24, padding: 24 }}>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={chartData}>
            <XAxis dataKey="name" tick={{ fill: "#8b9ab5", fontSize: 11 }} />
            <YAxis tick={{ fill: "#8b9ab5", fontSize: 11 }} />
            <Tooltip contentStyle={{ background: "#0a1e3d", border: "1px solid rgba(31,217,254,0.2)" }} />
            <Legend />
            <Bar dataKey="total" fill="#1fd9fe" name="Total" radius={[4, 4, 0, 0]} />
            <Bar dataKey="unique" fill="#ef4444" name="Unique Originals" radius={[4, 4, 0, 0]} />
            <Bar dataKey="backed_up" fill="#22c55e" name="Backed Up" radius={[4, 4, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
      <div style={{ marginTop: 24 }}>
        <DataTable columns={["recovery_priority", "total_assets", "unique_originals", "backed_up_count", "avg_surviving_derivatives"]} rows={rows} />
      </div>
    </div>
  );
}

// ── UC25: Insurance Dashboard ──

function InsuranceDashboard({ rows }) {
  const irreplaceable = rows.filter((r) => r.is_irreplaceable).length;
  const replaceable = rows.length - irreplaceable;

  const pieData = [
    { name: "Irreplaceable", value: irreplaceable },
    { name: "Replaceable", value: replaceable },
  ].filter((d) => d.value > 0);

  return (
    <div>
      <MetricsCard
        items={[
          { label: "Insured Assets", value: rows.length },
          { label: "Irreplaceable", value: irreplaceable, color: "var(--danger)" },
          { label: "Replaceable", value: replaceable, color: "var(--success)" },
        ]}
      />
      <div style={{ display: "flex", gap: 24, marginTop: 24, flexWrap: "wrap" }}>
        <div className="card" style={{ flex: "0 0 400px" }}>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie data={pieData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={100} label>
                {pieData.map((_, i) => <Cell key={i} fill={COLORS[i]} />)}
              </Pie>
              <Tooltip />
              <Legend />
            </PieChart>
          </ResponsiveContainer>
        </div>
        <div style={{ flex: 1, minWidth: 300 }}>
          <DataTable columns={["asset_id", "filename", "replacement_cost_tier", "is_irreplaceable", "digital_copy_count", "commercial_history_score"]} rows={rows} />
        </div>
      </div>
    </div>
  );
}

// ── UC07: Safe Deletion (Tree) ──

function SafeDeletionTree({ rows }) {
  const safe = rows.filter((r) => r.deletion_safe).length;
  const unsafe = rows.length - safe;

  return (
    <div>
      <MetricsCard
        items={[
          { label: "Evaluated", value: rows.length },
          { label: "Safe to Delete", value: safe, color: "var(--success)" },
          { label: "Unsafe", value: unsafe, color: "var(--danger)" },
        ]}
      />
      <div style={{ marginTop: 24 }}>
        <DataTable columns={["asset_id", "filename", "dependent_count", "is_leaf", "is_root", "deletion_safe"]} rows={rows} />
      </div>
    </div>
  );
}

// ── UC18: Localization (Tree) ──

function LocalizationTree({ rows }) {
  const languages = [...new Set(rows.map((r) => r.detected_language).filter(Boolean))];

  return (
    <div>
      <MetricsCard
        items={[
          { label: "Localized Assets", value: rows.length },
          { label: "Languages", value: languages.length },
        ]}
      />
      <div style={{ marginTop: 24 }}>
        <DataTable columns={["asset_id", "filename", "detected_language", "subtitle_track_count", "dubbed_from_asset_id", "source_language"]} rows={rows} />
      </div>
    </div>
  );
}

// ── Utility ──

function formatBytes(bytes) {
  if (!bytes) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  let val = bytes;
  while (val >= 1024 && i < units.length - 1) { val /= 1024; i++; }
  return `${val.toFixed(1)} ${units[i]}`;
}
