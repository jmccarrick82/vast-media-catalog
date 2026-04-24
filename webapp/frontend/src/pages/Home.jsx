import React, { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { getPersonas, getStats } from "../api";
import {
  Scale,
  Archive,
  Cpu,
  Film,
  Shield,
  TrendingUp,
  Database,
} from "lucide-react";

const ICONS = {
  scale: Scale,
  archive: Archive,
  cpu: Cpu,
  film: Film,
  shield: Shield,
  "trending-up": TrendingUp,
};

export default function Home() {
  const [personas, setPersonas] = useState([]);
  const [stats, setStats] = useState({});
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([getPersonas(), getStats()])
      .then(([p, s]) => {
        setPersonas(p);
        setStats(s);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <div className="loading">Loading...</div>;

  return (
    <div>
      <div className="page-header">
        <h1>Content Provenance Graph</h1>
        <p>
          Track every asset from creation through distribution — rights,
          lineage, AI provenance, security, and business value.
        </p>
      </div>

      <div className="stats-grid">
        <StatCard label="Total Assets" value={stats.total_assets ?? "—"} />
        <StatCard label="Classified" value={stats.classified_assets ?? "—"} />
        <StatCard
          label="Relationships"
          value={stats.total_relationships ?? "—"}
        />
        <StatCard
          label="Conflicts"
          value={stats.conflicts_detected ?? "—"}
          color="var(--danger)"
        />
        <StatCard
          label="AI-Generated"
          value={stats.ai_generated ?? "—"}
          color="var(--warning)"
        />
        <StatCard
          label="Unique Originals"
          value={stats.unique_originals ?? "—"}
          color="var(--success)"
        />
      </div>

      <Link to="/assets" className="hero-link-card" style={{ marginTop: 24 }}>
        <Database size={32} color="var(--vast-blue)" />
        <div>
          <h3 style={{ fontSize: 18, fontWeight: 600 }}>
            Browse Assets
          </h3>
          <p style={{ fontSize: 13, color: "var(--text-dim)", marginTop: 4 }}>
            Query the VAST DB assets table — {stats.total_assets ?? 0} assets with
            customizable column selection
          </p>
        </div>
        <span style={{ marginLeft: "auto", color: "var(--vast-blue)", fontSize: 22 }}>
          &rarr;
        </span>
      </Link>

      <div className="card-grid">
        {personas.map((p) => {
          const Icon = ICONS[p.icon] || Scale;
          return (
            <Link
              key={p.id}
              to={`/persona/${p.id}`}
              style={{ textDecoration: "none", color: "inherit" }}
            >
              <div className="card">
                <div
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: 12,
                    marginBottom: 12,
                  }}
                >
                  <Icon size={24} color="var(--vast-blue)" />
                  <h3 style={{ fontSize: 17, fontWeight: 600 }}>{p.name}</h3>
                </div>
                <p
                  style={{
                    fontSize: 13,
                    color: "var(--text-dim)",
                    lineHeight: 1.5,
                  }}
                >
                  {p.description}
                </p>
                <div
                  style={{
                    marginTop: 16,
                    fontSize: 13,
                    color: "var(--vast-blue)",
                  }}
                >
                  {p.use_cases.length} use cases →
                </div>
              </div>
            </Link>
          );
        })}
      </div>
    </div>
  );
}

function StatCard({ label, value, color }) {
  return (
    <div className="stat-card">
      <div className="stat-value" style={color ? { color } : undefined}>
        {typeof value === "number" ? value.toLocaleString() : value}
      </div>
      <div className="stat-label">{label}</div>
    </div>
  );
}
