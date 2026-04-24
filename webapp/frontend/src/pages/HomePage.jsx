import React, { useEffect, useState, useCallback, useMemo, useRef } from "react";
import { useNavigate } from "react-router-dom";
import { getAssets, getAssetColumns, getStats, uploadFiles } from "../api";
import DataTable from "../components/DataTable";
import { ChevronDown, ChevronUp, X, Plus, RotateCcw, Search, GripVertical, Upload } from "lucide-react";

export default function HomePage() {
  const [data, setData] = useState(null);
  const [stats, setStats] = useState({});
  const [loading, setLoading] = useState(true);
  const [columnMeta, setColumnMeta] = useState(null);
  const [selected, setSelected] = useState([]);
  const [pickerOpen, setPickerOpen] = useState(false);
  const [search, setSearch] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [sortCol, setSortCol] = useState(null);
  const [sortDir, setSortDir] = useState(null);
  const [showSubclips, setShowSubclips] = useState(false);
  const nav = useNavigate();
  const debounceRef = useRef(null);
  const chipDragRef = useRef(null);
  const [chipOverIdx, setChipOverIdx] = useState(null);
  const fileInputRef = useRef(null);
  const [uploading, setUploading] = useState(false);
  const [uploadMsg, setUploadMsg] = useState(null);

  // Debounce search input
  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => setDebouncedSearch(search), 300);
    return () => clearTimeout(debounceRef.current);
  }, [search]);

  // Load column metadata + stats once
  useEffect(() => {
    Promise.all([getAssetColumns(), getStats()])
      .then(([meta, s]) => {
        setColumnMeta(meta);
        setSelected(meta.defaults);
        setStats(s);
      })
      .catch(() => {});
  }, []);

  // Fetch assets whenever selected columns or search changes
  const fetchData = useCallback(() => {
    if (!selected.length) return;
    setLoading(true);
    getAssets(200, selected, debouncedSearch, showSubclips)
      .then(setData)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [selected, debouncedSearch, showSubclips]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // ── Sort logic ──────────────────────────────────────────
  const handleSort = (col) => {
    if (sortCol === col) {
      if (sortDir === "asc") setSortDir("desc");
      else { setSortCol(null); setSortDir(null); }
    } else {
      setSortCol(col);
      setSortDir("asc");
    }
  };

  const sortedRows = useMemo(() => {
    if (!data?.rows || !sortCol || !sortDir) return data?.rows ?? [];
    return [...data.rows].sort((a, b) => {
      let va = a[sortCol];
      let vb = b[sortCol];
      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;
      if (typeof va === "number" && typeof vb === "number") {
        return sortDir === "asc" ? va - vb : vb - va;
      }
      va = String(va).toLowerCase();
      vb = String(vb).toLowerCase();
      if (va < vb) return sortDir === "asc" ? -1 : 1;
      if (va > vb) return sortDir === "asc" ? 1 : -1;
      return 0;
    });
  }, [data?.rows, sortCol, sortDir]);

  // ── Column reorder (from DataTable header drag) ─────────
  const handleColumnReorder = (fromIdx, toIdx) => {
    setSelected((prev) => {
      const next = [...prev];
      const [moved] = next.splice(fromIdx, 1);
      next.splice(toIdx, 0, moved);
      return next;
    });
    // Also reorder data.columns for instant visual update
    if (data?.columns) {
      const cols = [...data.columns];
      const [moved] = cols.splice(fromIdx, 1);
      cols.splice(toIdx, 0, moved);
      setData((prev) => ({ ...prev, columns: cols }));
    }
  };

  // ── Column chip drag ────────────────────────────────────
  const handleChipDragStart = (e, idx) => {
    chipDragRef.current = idx;
    e.dataTransfer.effectAllowed = "move";
  };

  const handleChipDragOver = (e, idx) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = "move";
    if (idx !== chipOverIdx) setChipOverIdx(idx);
  };

  const handleChipDrop = (e, idx) => {
    e.preventDefault();
    const from = chipDragRef.current;
    if (from !== null && from !== idx) {
      handleColumnReorder(from, idx);
    }
    chipDragRef.current = null;
    setChipOverIdx(null);
  };

  const handleChipDragEnd = () => {
    chipDragRef.current = null;
    setChipOverIdx(null);
  };

  const handleUpload = async (e) => {
    const files = e.target.files;
    if (!files?.length) return;
    setUploading(true);
    setUploadMsg(`Uploading ${files.length} file${files.length > 1 ? "s" : ""}...`);
    try {
      const result = await uploadFiles(files);
      const ok = result.uploaded.filter((r) => r.status === "ok").length;
      setUploadMsg(`Uploaded ${ok} file${ok !== 1 ? "s" : ""}. Pipeline will process shortly.`);
      setTimeout(() => { setUploadMsg(null); fetchData(); }, 3000);
    } catch (err) {
      setUploadMsg(`Upload failed: ${err.message}`);
      setTimeout(() => setUploadMsg(null), 5000);
    } finally {
      setUploading(false);
      if (fileInputRef.current) fileInputRef.current.value = "";
    }
  };

  const toggleColumn = (col) => {
    setSelected((prev) =>
      prev.includes(col) ? prev.filter((c) => c !== col) : [...prev, col]
    );
  };

  const addGroup = (cols) => {
    setSelected((prev) => {
      const s = new Set(prev);
      cols.forEach((c) => s.add(c));
      return [...s];
    });
  };

  const removeGroup = (cols) => {
    const remove = new Set(cols);
    remove.delete("asset_id");
    setSelected((prev) => prev.filter((c) => !remove.has(c)));
  };

  const resetToDefaults = () => {
    if (columnMeta) {
      setSelected(columnMeta.defaults);
      setSortCol(null);
      setSortDir(null);
    }
  };

  if (!columnMeta) return <div className="loading">Loading...</div>;

  return (
    <div>
      <div className="page-header">
        <h1>Content Provenance Graph</h1>
        <p>
          Track every asset from creation through distribution &mdash; rights,
          lineage, AI provenance, security, and business value.
        </p>
      </div>

      {/* Stats row */}
      <div className="stats-grid">
        <StatCard label="Total Assets" value={stats.total_assets} />
        <StatCard label="Classified" value={stats.classified_assets} />
        <StatCard label="Relationships" value={stats.total_relationships} />
        <StatCard label="Conflicts" value={stats.conflicts_detected} color="var(--danger)" />
        <StatCard label="AI-Generated" value={stats.ai_generated} color="var(--warning)" />
        <StatCard label="Unique Originals" value={stats.unique_originals} color="var(--success)" />
      </div>

      {/* Search bar + subclip toggle */}
      <div style={{ marginTop: 24, marginBottom: 20, display: "flex", alignItems: "center", gap: 16 }}>
        <div style={{ position: "relative", maxWidth: 480, flex: 1 }}>
          <Search
            size={16}
            style={{ position: "absolute", left: 12, top: 11, color: "var(--text-dim)" }}
          />
          <input
            type="text"
            placeholder="Search by filename..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="search-input"
          />
          {search && (
            <X
              size={14}
              style={{
                position: "absolute",
                right: 12,
                top: 12,
                cursor: "pointer",
                color: "var(--text-dim)",
              }}
              onClick={() => setSearch("")}
            />
          )}
        </div>
        <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 13, color: "var(--text-dim)", cursor: "pointer", whiteSpace: "nowrap" }}>
          <input
            type="checkbox"
            checked={showSubclips}
            onChange={(e) => setShowSubclips(e.target.checked)}
          />
          Show subclips
        </label>
        <input
          ref={fileInputRef}
          type="file"
          accept="video/*"
          multiple
          style={{ display: "none" }}
          onChange={handleUpload}
        />
        <button
          className="picker-btn"
          style={{ whiteSpace: "nowrap" }}
          onClick={() => fileInputRef.current?.click()}
          disabled={uploading}
        >
          <Upload size={14} />
          <span>{uploading ? "Uploading..." : "Upload"}</span>
        </button>
      </div>
      {uploadMsg && (
        <div style={{
          marginBottom: 16, padding: "10px 16px", borderRadius: 8,
          background: "var(--vast-blue-dim)", color: "var(--vast-blue)",
          fontSize: 13, display: "inline-block",
        }}>
          {uploadMsg}
        </div>
      )}

      {/* Column picker toolbar */}
      <div
        style={{
          marginBottom: 20,
          display: "flex",
          gap: 12,
          alignItems: "center",
          flexWrap: "wrap",
        }}
      >
        <button className="picker-btn" onClick={() => setPickerOpen(!pickerOpen)}>
          {pickerOpen ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
          <span>Columns ({selected.length})</span>
        </button>
        <button className="picker-btn picker-btn-dim" onClick={resetToDefaults}>
          <RotateCcw size={13} />
          <span>Reset</span>
        </button>

        {/* Active column chips — draggable to reorder */}
        <div style={{ display: "flex", gap: 6, flexWrap: "wrap", flex: 1 }}>
          {selected.map((col, idx) => (
            <span
              key={col}
              className={`col-chip ${chipOverIdx === idx ? "chip-drag-over" : ""}`}
              draggable
              onDragStart={(e) => handleChipDragStart(e, idx)}
              onDragOver={(e) => handleChipDragOver(e, idx)}
              onDrop={(e) => handleChipDrop(e, idx)}
              onDragEnd={handleChipDragEnd}
            >
              <GripVertical size={10} style={{ opacity: 0.5, flexShrink: 0 }} />
              {formatCol(col)}
              {col !== "asset_id" && (
                <X
                  size={12}
                  style={{ cursor: "pointer", marginLeft: 4, opacity: 0.7 }}
                  onClick={() => toggleColumn(col)}
                />
              )}
            </span>
          ))}
        </div>
      </div>

      {/* Expandable column picker panel */}
      {pickerOpen && (
        <div className="picker-panel">
          {Object.entries(columnMeta.groups).map(([group, cols]) => {
            const allIn = cols.every((c) => selected.includes(c));
            return (
              <div key={group} className="picker-group">
                <div className="picker-group-header">
                  <span className="picker-group-name">{group}</span>
                  {allIn ? (
                    <button className="picker-group-toggle" onClick={() => removeGroup(cols)}>
                      Remove all
                    </button>
                  ) : (
                    <button className="picker-group-toggle" onClick={() => addGroup(cols)}>
                      <Plus size={11} /> Add all
                    </button>
                  )}
                </div>
                <div className="picker-group-cols">
                  {cols.map((col) => {
                    const active = selected.includes(col);
                    return (
                      <label key={col} className={`picker-col ${active ? "active" : ""}`}>
                        <input
                          type="checkbox"
                          checked={active}
                          onChange={() => toggleColumn(col)}
                          disabled={col === "asset_id"}
                        />
                        <span>{formatCol(col)}</span>
                      </label>
                    );
                  })}
                </div>
              </div>
            );
          })}
        </div>
      )}

      {loading ? (
        <div className="loading">Querying VAST DB...</div>
      ) : (
        <>
          <div style={{ marginBottom: 8, fontSize: 13, color: "var(--text-dim)" }}>
            {data?.count ?? 0} assets{debouncedSearch ? ` matching "${debouncedSearch}"` : ""}
            {sortCol && (
              <span style={{ marginLeft: 8, color: "var(--vast-blue)" }}>
                sorted by {formatCol(sortCol)} {sortDir === "asc" ? "↑" : "↓"}
              </span>
            )}
          </div>
          <div className="card" style={{ overflow: "auto" }}>
            <DataTable
              columns={data?.columns}
              rows={sortedRows}
              onRowClick={(row) => nav(`/assets/${row.asset_id}`)}
              sortCol={sortCol}
              sortDir={sortDir}
              onSort={handleSort}
              onColumnReorder={handleColumnReorder}
            />
          </div>
        </>
      )}
    </div>
  );
}

function StatCard({ label, value, color }) {
  return (
    <div className="stat-card">
      <div className="stat-value" style={color ? { color } : undefined}>
        {typeof value === "number" ? value.toLocaleString() : value ?? "\u2014"}
      </div>
      <div className="stat-label">{label}</div>
    </div>
  );
}

function formatCol(key) {
  return key.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}
