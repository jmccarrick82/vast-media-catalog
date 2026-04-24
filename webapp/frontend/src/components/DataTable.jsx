import React, { useRef, useState } from "react";

export default function DataTable({
  columns,
  rows,
  onRowClick,
  sortCol,
  sortDir,
  onSort,
  onColumnReorder,
}) {
  const [dragIdx, setDragIdx] = useState(null);
  const [overIdx, setOverIdx] = useState(null);
  const dragRef = useRef(null);

  if (!rows || rows.length === 0) {
    return <div className="loading">No data available</div>;
  }

  const cols = columns || Object.keys(rows[0]);
  const sortable = typeof onSort === "function";
  const draggable = typeof onColumnReorder === "function";

  const handleDragStart = (e, idx) => {
    dragRef.current = idx;
    setDragIdx(idx);
    e.dataTransfer.effectAllowed = "move";
  };

  const handleDragOver = (e, idx) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = "move";
    if (idx !== overIdx) setOverIdx(idx);
  };

  const handleDrop = (e, idx) => {
    e.preventDefault();
    const from = dragRef.current;
    if (from !== null && from !== idx) {
      onColumnReorder(from, idx);
    }
    setDragIdx(null);
    setOverIdx(null);
    dragRef.current = null;
  };

  const handleDragEnd = () => {
    setDragIdx(null);
    setOverIdx(null);
    dragRef.current = null;
  };

  return (
    <div style={{ overflowX: "auto" }}>
      <table className="data-table">
        <thead>
          <tr>
            {cols.map((col, idx) => (
              <th
                key={col}
                className={[
                  sortable ? "sortable" : "",
                  draggable ? "draggable-col" : "",
                  dragIdx === idx ? "dragging" : "",
                  overIdx === idx && dragIdx !== idx ? "drag-over" : "",
                ].filter(Boolean).join(" ")}
                onClick={() => sortable && onSort(col)}
                draggable={draggable}
                onDragStart={(e) => draggable && handleDragStart(e, idx)}
                onDragOver={(e) => draggable && handleDragOver(e, idx)}
                onDrop={(e) => draggable && handleDrop(e, idx)}
                onDragEnd={draggable ? handleDragEnd : undefined}
              >
                <span className="th-content">
                  {formatHeader(col)}
                  {sortable && sortCol === col && (
                    <span className="sort-arrow">
                      {sortDir === "asc" ? " ▲" : " ▼"}
                    </span>
                  )}
                </span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr
              key={i}
              onClick={() => onRowClick && onRowClick(row)}
              style={onRowClick ? { cursor: "pointer" } : undefined}
            >
              {cols.map((col) => (
                <td key={col}>{formatCell(row[col], col)}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function formatHeader(key) {
  return key.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

const TIMESTAMP_SUFFIXES = ["_at", "_date", "_time"];
const BYTE_COLUMNS = ["file_size_bytes", "storage_savings_bytes", "total_storage_savings_bytes"];

function isTimestampCol(col) {
  return TIMESTAMP_SUFFIXES.some((s) => col.endsWith(s));
}

function formatCell(val, col) {
  if (val === null || val === undefined) return "—";
  if (typeof val === "boolean") return val ? "Yes" : "No";
  if (typeof val === "number") {
    // Timestamp columns: format as date
    if (col && isTimestampCol(col)) {
      // Handle both seconds and milliseconds epoch
      const ms = val > 1e12 ? val : val * 1000;
      return new Date(ms).toLocaleString();
    }
    // Byte columns: format as file size
    if (col && BYTE_COLUMNS.includes(col)) {
      return formatBytes(val);
    }
    return val.toLocaleString();
  }
  if (typeof val === "string" && val.length > 80) return val.slice(0, 80) + "...";
  return String(val);
}

function formatBytes(bytes) {
  if (!bytes) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  let n = Math.abs(bytes);
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024;
    i++;
  }
  return `${n.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}
