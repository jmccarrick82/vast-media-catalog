import React from "react";

export default function MetricsCard({ items }) {
  if (!items || items.length === 0) return null;
  return (
    <div className="stats-grid">
      {items.map((item, i) => (
        <div key={i} className="stat-card">
          <div
            className="stat-value"
            style={item.color ? { color: item.color } : undefined}
          >
            {typeof item.value === "number"
              ? item.value.toLocaleString()
              : item.value ?? "—"}
          </div>
          <div className="stat-label">{item.label}</div>
        </div>
      ))}
    </div>
  );
}
