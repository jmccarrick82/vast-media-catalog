import React from "react";

export default function Timeline({ items, labelKey, timeKey, detailKey }) {
  if (!items || items.length === 0) {
    return <div className="loading">No timeline data</div>;
  }

  return (
    <div style={{ position: "relative", paddingLeft: 28 }}>
      <div
        style={{
          position: "absolute",
          left: 10,
          top: 0,
          bottom: 0,
          width: 2,
          background: "var(--border)",
        }}
      />
      {items.map((item, i) => (
        <div
          key={i}
          style={{
            position: "relative",
            marginBottom: 20,
            paddingLeft: 16,
          }}
        >
          <div
            style={{
              position: "absolute",
              left: -22,
              top: 6,
              width: 10,
              height: 10,
              borderRadius: "50%",
              background: "var(--vast-blue)",
              border: "2px solid var(--vast-dark)",
            }}
          />
          <div style={{ fontSize: 12, color: "var(--text-dim)" }}>
            {formatTime(item[timeKey || "timestamp"])}
          </div>
          <div style={{ fontSize: 14, fontWeight: 600, marginTop: 2 }}>
            {item[labelKey || "label"]}
          </div>
          {detailKey && item[detailKey] && (
            <div
              style={{ fontSize: 13, color: "var(--text-dim)", marginTop: 4 }}
            >
              {item[detailKey]}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}

function formatTime(val) {
  if (!val) return "";
  if (typeof val === "number") {
    return new Date(val * 1000).toLocaleString();
  }
  return String(val);
}
