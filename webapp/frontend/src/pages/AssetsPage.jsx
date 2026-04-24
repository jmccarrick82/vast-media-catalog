import React, { useEffect, useState, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { getAssets, getAssetColumns } from "../api";
import DataTable from "../components/DataTable";
import { ChevronDown, ChevronUp, X, Plus, RotateCcw } from "lucide-react";

export default function AssetsPage() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [columnMeta, setColumnMeta] = useState(null); // { groups, defaults }
  const [selected, setSelected] = useState([]); // currently selected columns
  const [pickerOpen, setPickerOpen] = useState(false);
  const nav = useNavigate();

  // Load column metadata once
  useEffect(() => {
    getAssetColumns()
      .then((meta) => {
        setColumnMeta(meta);
        setSelected(meta.defaults);
      })
      .catch(() => {});
  }, []);

  // Fetch assets whenever selected columns change
  const fetchData = useCallback(() => {
    if (!selected.length) return;
    setLoading(true);
    getAssets(100, selected)
      .then(setData)
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [selected]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const toggleColumn = (col) => {
    setSelected((prev) =>
      prev.includes(col)
        ? prev.filter((c) => c !== col)
        : [...prev, col]
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
    // Never remove asset_id
    remove.delete("asset_id");
    setSelected((prev) => prev.filter((c) => !remove.has(c)));
  };

  const resetToDefaults = () => {
    if (columnMeta) setSelected(columnMeta.defaults);
  };

  if (!columnMeta)
    return <div className="loading">Loading column metadata...</div>;

  return (
    <div>
      <div className="page-header">
        <h1>All Assets</h1>
        <p>{data?.count ?? 0} assets in the media catalog</p>
      </div>

      {/* Column picker toolbar */}
      <div style={{ marginBottom: 20, display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
        <button className="picker-btn" onClick={() => setPickerOpen(!pickerOpen)}>
          {pickerOpen ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
          <span>Columns ({selected.length})</span>
        </button>
        <button className="picker-btn picker-btn-dim" onClick={resetToDefaults}>
          <RotateCcw size={13} />
          <span>Reset</span>
        </button>

        {/* Active column chips */}
        <div style={{ display: "flex", gap: 6, flexWrap: "wrap", flex: 1 }}>
          {selected.map((col) => (
            <span key={col} className="col-chip">
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
            const someIn = cols.some((c) => selected.includes(c));
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
        <div className="card" style={{ overflow: "auto" }}>
          <DataTable
            columns={data?.columns}
            rows={data?.rows}
            onRowClick={(row) => nav(`/assets/${row.asset_id}`)}
          />
        </div>
      )}
    </div>
  );
}

function formatCol(key) {
  return key.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}
