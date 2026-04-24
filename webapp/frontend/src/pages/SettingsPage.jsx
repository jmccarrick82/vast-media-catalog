import React, { useEffect, useMemo, useState, useCallback } from "react";
import {
  Settings as SettingsIcon, RotateCcw, Check, AlertCircle, Loader2,
  ChevronDown, ChevronRight, RefreshCw, Save,
} from "lucide-react";
import {
  listConfigScopes, getConfigScope, updateConfigValue, resetConfigValue,
  bulkUpdateScope, resetScope,
} from "../api";

/**
 * /settings — runtime-editable knobs for every function that uses the
 * shared config system. Fully schema-driven: the page has no knowledge
 * of any specific knob. It reads the scope from `/api/configs/:scope`
 * and renders a widget per `value_type`.
 *
 * Design rules:
 *  - Never truncate user edits silently — show dirty state until saved.
 *  - Show the default alongside the current value so it's obvious what
 *    the user changed from factory.
 *  - One-click reset per row (confirms only if dirty).
 *  - "Save all" commits every dirty row; results land with feedback inline.
 *  - Every save stamps updated_by="webapp" — the backend records this.
 */
export default function SettingsPage() {
  const [scopes, setScopes] = useState(null);
  const [activeScope, setActiveScope] = useState(null);
  const [scopeData, setScopeData] = useState(null);
  const [draft, setDraft] = useState({});           // {key: rawUIValue}
  const [status, setStatus] = useState({});         // {key: 'saving' | 'saved' | 'err:<msg>'}
  const [scopeLoading, setScopeLoading] = useState(false);
  const [err, setErr] = useState(null);

  // Load the list of scopes once
  useEffect(() => {
    listConfigScopes()
      .then((d) => {
        setScopes(d.scopes || []);
        if ((d.scopes || []).length && !activeScope) {
          setActiveScope(d.scopes[0].scope);
        }
      })
      .catch((e) => setErr(String(e)));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const loadScope = useCallback((scope) => {
    if (!scope) return;
    setScopeLoading(true);
    setErr(null);
    getConfigScope(scope)
      .then((d) => {
        setScopeData(d);
        setDraft({});
        setStatus({});
      })
      .catch((e) => setErr(String(e)))
      .finally(() => setScopeLoading(false));
  }, []);

  useEffect(() => { loadScope(activeScope); }, [activeScope, loadScope]);

  // Settings flattened + indexed by key for easy lookup
  const allSettings = useMemo(() => {
    if (!scopeData) return [];
    return scopeData.groups.flatMap((g) => g.settings);
  }, [scopeData]);

  const isDirty = useCallback(
    (s) => {
      if (!(s.key in draft)) return false;
      return !valuesEqual(draft[s.key], s.value);
    },
    [draft],
  );

  const dirtyKeys = useMemo(
    () => allSettings.filter(isDirty).map((s) => s.key),
    [allSettings, isDirty],
  );

  const onChange = (key, uiValue) => {
    setDraft((d) => ({ ...d, [key]: uiValue }));
    setStatus((s) => ({ ...s, [key]: null }));
  };

  const saveOne = async (setting) => {
    const key = setting.key;
    const uiVal = draft[key];
    let apiVal;
    try {
      apiVal = uiValueToApiValue(uiVal, setting.value_type);
    } catch (e) {
      setStatus((s) => ({ ...s, [key]: `err:${e.message}` }));
      return;
    }
    setStatus((s) => ({ ...s, [key]: "saving" }));
    try {
      await updateConfigValue(scopeData.scope, key, apiVal);
      // Reflect the server state locally so isDirty flips off
      setScopeData((d) => ({
        ...d,
        groups: d.groups.map((g) => ({
          ...g,
          settings: g.settings.map((s) =>
            s.key === key
              ? { ...s, value: apiVal, updated_by: "webapp", updated_at: Date.now() / 1000 }
              : s,
          ),
        })),
      }));
      setDraft((d) => { const nd = { ...d }; delete nd[key]; return nd; });
      setStatus((s) => ({ ...s, [key]: "saved" }));
      setTimeout(() => setStatus((s) => ({ ...s, [key]: null })), 1800);
    } catch (e) {
      setStatus((s) => ({ ...s, [key]: `err:${e.message || e}` }));
    }
  };

  const resetOne = async (setting) => {
    const key = setting.key;
    setStatus((s) => ({ ...s, [key]: "saving" }));
    try {
      await resetConfigValue(scopeData.scope, key);
      setScopeData((d) => ({
        ...d,
        groups: d.groups.map((g) => ({
          ...g,
          settings: g.settings.map((s) =>
            s.key === key
              ? { ...s, value: s.default_value, updated_by: "webapp(reset)", updated_at: Date.now() / 1000 }
              : s,
          ),
        })),
      }));
      setDraft((d) => { const nd = { ...d }; delete nd[key]; return nd; });
      setStatus((s) => ({ ...s, [key]: "saved" }));
      setTimeout(() => setStatus((s) => ({ ...s, [key]: null })), 1800);
    } catch (e) {
      setStatus((s) => ({ ...s, [key]: `err:${e.message || e}` }));
    }
  };

  // Bulk update via single transaction on the server.
  //   - When `mode === "dirty"` (default "Update" button): send only
  //     the settings the user changed.
  //   - When `mode === "all"` ("Rewrite all"): send the current value of
  //     every setting in the scope, useful after an out-of-band edit to
  //     re-stamp updated_at/updated_by across the whole scope.
  const [bulkStatus, setBulkStatus] = useState(null); // null | "saving" | "saved" | "err:..."
  const saveAll = async (mode = "dirty") => {
    if (!scopeData) return;
    const subset = mode === "all"
      ? allSettings
      : dirtyKeys.map((k) => allSettings.find((s) => s.key === k)).filter(Boolean);
    if (!subset.length) return;

    // Convert each to API shape.
    let updates;
    try {
      updates = subset.map((s) => {
        const uiVal = s.key in draft ? draft[s.key] : s.value;
        return { key: s.key, value: uiValueToApiValue(uiVal, s.value_type) };
      });
    } catch (e) {
      setBulkStatus(`err:${e.message}`);
      return;
    }

    setBulkStatus("saving");
    try {
      await bulkUpdateScope(scopeData.scope, updates);
      // Reflect changes in local state — clear draft, update value field
      setScopeData((d) => ({
        ...d,
        groups: d.groups.map((g) => ({
          ...g,
          settings: g.settings.map((s) => {
            const u = updates.find((x) => x.key === s.key);
            if (!u) return s;
            return { ...s, value: u.value, updated_by: "webapp", updated_at: Date.now() / 1000 };
          }),
        })),
      }));
      setDraft({});
      setStatus({});
      setBulkStatus("saved");
      setTimeout(() => setBulkStatus(null), 2200);
    } catch (e) {
      setBulkStatus(`err:${e.message || e}`);
    }
  };

  const restoreAllDefaults = async () => {
    if (!scopeData) return;
    const yes = window.confirm(
      `Restore ALL ${scopeData.count} settings in "${scopeData.scope}" to factory defaults?\n\n` +
      `Any values you've edited will be overwritten. This cannot be undone.`
    );
    if (!yes) return;

    setBulkStatus("saving");
    try {
      await resetScope(scopeData.scope);
      // Reload from server — defaults may have been tuned since seed.
      await loadScope(scopeData.scope);
      setBulkStatus("saved");
      setTimeout(() => setBulkStatus(null), 2200);
    } catch (e) {
      setBulkStatus(`err:${e.message || e}`);
    }
  };

  // ── Render ──────────────────────────────────────────────────

  return (
    <div>
      <div className="page-header">
        <h1 style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <SettingsIcon size={24} style={{ color: "var(--vast-blue)" }} />
          Function Settings
        </h1>
        <p>
          Runtime-editable knobs for every function. Each scope is a function
          (e.g. <code>qc-inspector</code>); each setting under it is a knob the
          function reads from the <code>function_configs</code> VAST DB table.
          Changes take effect on the next handler invocation (functions cache
          for up to 60s).
        </p>
      </div>

      {err && (
        <div style={{
          padding: "10px 14px", marginBottom: 14, borderRadius: 8,
          background: "rgba(255,80,80,0.12)", color: "var(--danger)",
          fontSize: 13, border: "1px solid rgba(255,80,80,0.3)",
        }}>
          <AlertCircle size={14} style={{ verticalAlign: "text-bottom", marginRight: 6 }} />
          {err}
        </div>
      )}

      <div style={{ display: "grid", gridTemplateColumns: "220px 1fr", gap: 20 }}>
        {/* Sidebar — scopes */}
        <div>
          <div style={{
            fontSize: 11, textTransform: "uppercase", letterSpacing: 0.6,
            color: "var(--text-dim)", marginBottom: 8, paddingLeft: 8,
          }}>
            Scopes
          </div>
          {(scopes || []).map((s) => (
            <button
              key={s.scope}
              onClick={() => setActiveScope(s.scope)}
              style={{
                display: "flex", alignItems: "center", width: "100%",
                justifyContent: "space-between",
                padding: "8px 10px", marginBottom: 4, borderRadius: 6,
                background: s.scope === activeScope ? "var(--vast-blue-dim)" : "transparent",
                color: s.scope === activeScope ? "var(--vast-blue)" : "var(--text)",
                border: "none", cursor: "pointer",
                fontSize: 13, fontWeight: s.scope === activeScope ? 600 : 400,
                fontFamily: "SF Mono, Menlo, monospace",
              }}
            >
              <span>{s.scope}</span>
              <span style={{ fontSize: 11, color: "var(--text-dim)" }}>{s.count}</span>
            </button>
          ))}
          {scopes && !scopes.length && (
            <div style={{ color: "var(--text-dim)", fontSize: 13, padding: 8 }}>
              No configured scopes yet. Run{" "}
              <code>scripts/seed_function_configs.py</code> on the host.
            </div>
          )}
        </div>

        {/* Main — selected scope */}
        <div>
          {scopeLoading && <div className="loading">Loading...</div>}
          {!scopeLoading && scopeData && (
            <>
              <div style={{
                display: "flex", alignItems: "center", justifyContent: "space-between",
                marginBottom: 14, gap: 12, flexWrap: "wrap",
              }}>
                <div style={{ fontSize: 13, color: "var(--text-dim)" }}>
                  <strong style={{ color: "var(--text)" }}>{scopeData.count}</strong> settings
                  in <strong style={{ color: "var(--vast-blue)", fontFamily: "SF Mono, Menlo, monospace" }}>{scopeData.scope}</strong>
                  {dirtyKeys.length > 0 && (
                    <span style={{ marginLeft: 12, color: "var(--warning)" }}>
                      • {dirtyKeys.length} unsaved change{dirtyKeys.length === 1 ? "" : "s"}
                    </span>
                  )}
                  {bulkStatus && bulkStatus.startsWith("err:") && (
                    <span style={{ marginLeft: 12, color: "var(--danger)" }}>
                      <AlertCircle size={12} style={{ verticalAlign: "text-bottom" }} />{" "}
                      {bulkStatus.slice(4)}
                    </span>
                  )}
                  {bulkStatus === "saved" && (
                    <span style={{ marginLeft: 12, color: "var(--vast-blue)" }}>
                      <Check size={12} style={{ verticalAlign: "text-bottom" }} /> Saved
                    </span>
                  )}
                </div>
                <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                  <button
                    onClick={restoreAllDefaults}
                    disabled={bulkStatus === "saving"}
                    className="picker-btn picker-btn-dim"
                    title={`Reset every setting in ${scopeData.scope} to its factory default`}
                    style={{
                      opacity: bulkStatus === "saving" ? 0.5 : 1,
                      cursor: bulkStatus === "saving" ? "not-allowed" : "pointer",
                    }}
                  >
                    <RotateCcw size={14} />
                    <span>Restore defaults</span>
                  </button>
                  <button
                    onClick={() => saveAll("dirty")}
                    disabled={!dirtyKeys.length || bulkStatus === "saving"}
                    className="picker-btn"
                    style={{
                      opacity: (!dirtyKeys.length || bulkStatus === "saving") ? 0.5 : 1,
                      cursor: (!dirtyKeys.length || bulkStatus === "saving") ? "not-allowed" : "pointer",
                      fontWeight: 600,
                    }}
                    title={dirtyKeys.length
                      ? `Apply ${dirtyKeys.length} change${dirtyKeys.length === 1 ? "" : "s"}`
                      : "No changes to apply"}
                  >
                    {bulkStatus === "saving" ? <Loader2 size={14} className="spin" /> : <Save size={14} />}
                    <span>
                      {bulkStatus === "saving"
                        ? "Updating..."
                        : dirtyKeys.length
                          ? `Update (${dirtyKeys.length})`
                          : "Update"}
                    </span>
                  </button>
                  <button
                    onClick={() => loadScope(scopeData.scope)}
                    disabled={bulkStatus === "saving"}
                    className="picker-btn picker-btn-dim"
                    title="Reload current values from the database (discards unsaved edits)"
                    style={{ fontSize: 12 }}
                  >
                    <RefreshCw size={12} />
                  </button>
                </div>
              </div>

              {scopeData.groups.map((g) => (
                <GroupCard
                  key={g.name}
                  group={g}
                  draft={draft}
                  status={status}
                  onChange={onChange}
                  onSave={saveOne}
                  onReset={resetOne}
                  isDirty={isDirty}
                />
              ))}
            </>
          )}
        </div>
      </div>

      <style>{`
        .spin { animation: spin 0.9s linear infinite; }
        @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
      `}</style>
    </div>
  );
}


// ── Group card ──────────────────────────────────────────────────────────

function GroupCard({ group, draft, status, onChange, onSave, onReset, isDirty }) {
  const [open, setOpen] = useState(true);
  return (
    <div className="card" style={{ marginBottom: 12, padding: 0, overflow: "hidden" }}>
      <button
        onClick={() => setOpen((o) => !o)}
        style={{
          display: "flex", alignItems: "center", gap: 8,
          width: "100%", padding: "12px 16px",
          background: "transparent", border: "none",
          color: "var(--text)", fontSize: 14, fontWeight: 600,
          cursor: "pointer", textAlign: "left",
          borderBottom: open ? "1px solid var(--border)" : "none",
        }}
      >
        {open ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
        {group.name}
        <span style={{ marginLeft: "auto", fontSize: 12, color: "var(--text-dim)" }}>
          {group.settings.length}
        </span>
      </button>
      {open && (
        <div>
          {group.settings.map((s) => (
            <SettingRow
              key={s.key}
              setting={s}
              draft={draft}
              status={status[s.key]}
              dirty={isDirty(s)}
              onChange={(v) => onChange(s.key, v)}
              onSave={() => onSave(s)}
              onReset={() => onReset(s)}
            />
          ))}
        </div>
      )}
    </div>
  );
}


// ── Individual setting row ──────────────────────────────────────────────

function SettingRow({ setting, draft, status, dirty, onChange, onSave, onReset }) {
  const uiValue = draft[setting.key] !== undefined
    ? draft[setting.key]
    : apiValueToUiValue(setting.value, setting.value_type);
  const def = apiValueToUiValue(setting.default_value, setting.value_type);
  const isErr = status && String(status).startsWith("err:");
  const isSaving = status === "saving";
  const isSaved = status === "saved";

  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: "minmax(280px, 2fr) minmax(220px, 3fr)",
      gap: 14,
      padding: "12px 16px",
      borderBottom: "1px solid var(--border)",
      alignItems: "start",
      background: dirty ? "rgba(31, 217, 254, 0.04)" : "transparent",
    }}>
      {/* Label + description */}
      <div>
        <div style={{
          fontSize: 13, fontWeight: 500, color: "var(--text)",
          fontFamily: "SF Mono, Menlo, monospace", marginBottom: 3,
        }}>
          {setting.key}
          {dirty && <span style={{ color: "var(--warning)", marginLeft: 8, fontWeight: 400 }}>• unsaved</span>}
        </div>
        {setting.description && (
          <div style={{ fontSize: 12, color: "var(--text-dim)", lineHeight: 1.5 }}>
            {setting.description}
          </div>
        )}
        <div style={{
          fontSize: 11, color: "var(--text-dim)", marginTop: 6,
          display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap",
        }}>
          <span style={{
            padding: "1px 6px", borderRadius: 3,
            background: "rgba(255,255,255,0.04)",
            fontFamily: "SF Mono, Menlo, monospace",
          }}>
            {setting.value_type}
          </span>
          <span>default: <code style={{ color: "var(--text)" }}>{formatForDisplay(def, setting.value_type)}</code></span>
          {setting.min !== null && setting.min !== undefined && (
            <span>min: {setting.min}</span>
          )}
          {setting.max !== null && setting.max !== undefined && (
            <span>max: {setting.max}</span>
          )}
          {setting.updated_by && (
            <span title={`at ${setting.updated_at ? new Date(setting.updated_at * 1000).toLocaleString() : "?"}`}>
              last edited by: {setting.updated_by}
            </span>
          )}
        </div>
      </div>

      {/* Widget + save controls */}
      <div style={{ display: "flex", alignItems: "start", gap: 8, flexWrap: "wrap" }}>
        <div style={{ flex: 1, minWidth: 180 }}>
          <Widget setting={setting} uiValue={uiValue} onChange={onChange} />
          {isErr && (
            <div style={{ fontSize: 11, color: "var(--danger)", marginTop: 4 }}>
              <AlertCircle size={12} style={{ verticalAlign: "text-bottom", marginRight: 4 }} />
              {status.slice(4)}
            </div>
          )}
        </div>
        <button
          onClick={onSave}
          disabled={!dirty || isSaving}
          className="picker-btn"
          style={{
            opacity: !dirty || isSaving ? 0.4 : 1,
            cursor: !dirty || isSaving ? "not-allowed" : "pointer",
            fontSize: 12, padding: "6px 10px",
          }}
          title={dirty ? "Save this change" : "No changes to save"}
        >
          {isSaving ? <Loader2 size={12} className="spin" /> :
            isSaved ? <Check size={12} /> : <Check size={12} />}
          <span>{isSaved ? "Saved" : "Save"}</span>
        </button>
        <button
          onClick={onReset}
          className="picker-btn picker-btn-dim"
          style={{ fontSize: 12, padding: "6px 10px" }}
          title="Reset to default"
        >
          <RotateCcw size={12} />
          <span>Reset</span>
        </button>
      </div>
    </div>
  );
}


// ── Widget per value_type ───────────────────────────────────────────────

function Widget({ setting, uiValue, onChange }) {
  const t = setting.value_type;
  const common = {
    onChange: (e) => onChange(e.target.value),
    style: {
      width: "100%", padding: "6px 10px",
      background: "var(--bg-raised, #0f1a33)",
      border: "1px solid var(--border)", borderRadius: 6,
      color: "var(--text)", fontSize: 13,
      fontFamily: "SF Mono, Menlo, monospace",
    },
  };

  if (t === "bool") {
    const checked = uiValue === true || uiValue === "true" || uiValue === "on" || uiValue === "1";
    return (
      <label style={{ display: "inline-flex", alignItems: "center", gap: 8, cursor: "pointer" }}>
        <input
          type="checkbox"
          checked={checked}
          onChange={(e) => onChange(e.target.checked)}
        />
        <span style={{ fontSize: 13 }}>{checked ? "true" : "false"}</span>
      </label>
    );
  }

  if (t === "int") {
    return (
      <input
        type="number" step="1"
        value={uiValue ?? ""}
        min={setting.min ?? undefined}
        max={setting.max ?? undefined}
        {...common}
      />
    );
  }

  if (t === "float" || t === "duration_seconds" || t === "db") {
    const suffix = t === "duration_seconds" ? "s" : t === "db" ? "dB" : null;
    return (
      <div style={{ position: "relative", display: "flex", alignItems: "center" }}>
        <input
          type="number" step="any"
          value={uiValue ?? ""}
          min={setting.min ?? undefined}
          max={setting.max ?? undefined}
          {...common}
          style={{ ...common.style, paddingRight: suffix ? 32 : common.style.padding }}
        />
        {suffix && (
          <span style={{
            position: "absolute", right: 10, color: "var(--text-dim)",
            fontSize: 12, pointerEvents: "none",
          }}>{suffix}</span>
        )}
      </div>
    );
  }

  if (t === "percent") {
    // UI shows 0–100; stored 0–1
    const pct = uiValue == null || uiValue === "" ? ""
      : typeof uiValue === "number" ? uiValue * 100
      : uiValue;
    return (
      <div style={{ position: "relative", display: "flex", alignItems: "center" }}>
        <input
          type="number" step="1" min="0" max="100"
          value={pct}
          onChange={(e) => onChange(e.target.value === "" ? "" : Number(e.target.value) / 100)}
          style={{ ...common.style, paddingRight: 32 }}
        />
        <span style={{
          position: "absolute", right: 10, color: "var(--text-dim)",
          fontSize: 12, pointerEvents: "none",
        }}>%</span>
      </div>
    );
  }

  if (t === "string") {
    return <input type="text" value={uiValue ?? ""} {...common} />;
  }

  // json / list / dict — use textarea with JSON validation
  const text = typeof uiValue === "string" ? uiValue : JSON.stringify(uiValue, null, 2);
  return (
    <textarea
      value={text}
      onChange={(e) => onChange(e.target.value)}
      rows={Math.min(8, (text.match(/\n/g) || []).length + 2)}
      style={{
        ...common.style,
        fontFamily: "SF Mono, Menlo, monospace",
        minHeight: 38, resize: "vertical",
      }}
    />
  );
}


// ── Value marshalling helpers ───────────────────────────────────────────

function valuesEqual(a, b) {
  if (a === b) return true;
  if (typeof a === "object" || typeof b === "object") {
    try { return JSON.stringify(a) === JSON.stringify(b); }
    catch { return false; }
  }
  // Loose equal for number/string comparisons between UI input and API numbers
  return String(a) === String(b);
}

function apiValueToUiValue(apiVal, vtype) {
  // API already gives typed JS values (boolean, number, array, object, string)
  return apiVal;
}

function uiValueToApiValue(uiVal, vtype) {
  if (vtype === "int") {
    if (uiVal === "" || uiVal == null) return null;
    const n = Math.trunc(Number(uiVal));
    if (Number.isNaN(n)) throw new Error(`not a valid integer: ${uiVal}`);
    return n;
  }
  if (vtype === "float" || vtype === "duration_seconds" || vtype === "db" || vtype === "percent") {
    if (uiVal === "" || uiVal == null) return null;
    const n = Number(uiVal);
    if (Number.isNaN(n)) throw new Error(`not a valid number: ${uiVal}`);
    return n;
  }
  if (vtype === "bool") return !!uiVal;
  if (vtype === "string") return String(uiVal ?? "");
  // json
  if (typeof uiVal === "string") {
    try { return JSON.parse(uiVal); }
    catch (e) { throw new Error(`invalid JSON: ${e.message}`); }
  }
  return uiVal;
}

function formatForDisplay(v, vtype) {
  if (v == null) return "—";
  if (vtype === "percent" && typeof v === "number") return `${Math.round(v * 100)}%`;
  if (vtype === "duration_seconds") return `${v}s`;
  if (vtype === "db") return `${v} dB`;
  if (Array.isArray(v)) return v.length > 3 ? `[${v.slice(0, 3).join(", ")}, …]` : `[${v.join(", ")}]`;
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}
