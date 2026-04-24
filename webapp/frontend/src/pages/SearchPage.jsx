import React, { useState, useRef, useEffect } from "react";
import { Search, Play, Sparkles, Film, Tag, Loader2, ChevronDown, ChevronUp } from "lucide-react";
import { semanticSearch, videoURL } from "../api";

/**
 * Semantic Search page.
 *
 * Hits /api/semantic-search which:
 *   1. Embeds the query via the shared inference endpoint
 *      (nvidia/llama-3.2-nv-embedqa-1b-v2, input_type=query).
 *   2. Runs a cosine similarity search over the `subclips` Qdrant collection
 *      that subclip-ai-analyzer populates at ingest time.
 *
 * For each hit we show:
 *   - relevance score as a percentage (cosine similarity, 0..1)
 *   - AI-generated content summary + metadata badges
 *   - the actual text that was embedded (expandable)
 *   - an inline <video> player that streams the 30s subclip via /api/video
 */
export default function SearchPage() {
  const [q, setQ] = useState("");
  const [limit, setLimit] = useState(10);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState(null);
  const [results, setResults] = useState(null);
  const [submitted, setSubmitted] = useState("");
  const inputRef = useRef(null);

  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  const runSearch = async () => {
    const query = q.trim();
    if (!query) return;
    setLoading(true);
    setErr(null);
    setSubmitted(query);
    try {
      const data = await semanticSearch(query, limit);
      setResults(data);
    } catch (e) {
      setErr(e.message || String(e));
      setResults(null);
    } finally {
      setLoading(false);
    }
  };

  const onKey = (e) => {
    if (e.key === "Enter") runSearch();
  };

  return (
    <div>
      <div className="page-header">
        <h1 style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <Sparkles size={24} style={{ color: "var(--vast-blue)" }} />
          Semantic Search
        </h1>
        <p>
          Natural-language search over every subclip's summary, on-screen text, and spoken transcript.
          Embeddings produced at ingest time by <code>subclip-ai-analyzer</code> via the shared VAST inference
          endpoint; retrieval runs against a Qdrant vector DB keyed by <code>asset_id</code>.
        </p>
      </div>

      <div style={{ display: "flex", gap: 10, alignItems: "center", marginBottom: 8 }}>
        <div style={{ position: "relative", flex: 1, maxWidth: 720 }}>
          <Search
            size={16}
            style={{ position: "absolute", left: 12, top: 11, color: "var(--text-dim)" }}
          />
          <input
            ref={inputRef}
            type="text"
            value={q}
            placeholder='Try: "peaceful beach with ocean", "narrator speaking about history", "platypus swimming"...'
            className="search-input"
            style={{ width: "100%" }}
            onChange={(e) => setQ(e.target.value)}
            onKeyDown={onKey}
          />
        </div>
        <select
          value={limit}
          onChange={(e) => setLimit(Number(e.target.value))}
          className="search-input"
          style={{ width: 120, padding: "8px 12px" }}
          title="Max results"
        >
          <option value={5}>Top 5</option>
          <option value={10}>Top 10</option>
          <option value={25}>Top 25</option>
          <option value={50}>Top 50</option>
        </select>
        <button
          className="picker-btn"
          onClick={runSearch}
          disabled={!q.trim() || loading}
          style={{ whiteSpace: "nowrap" }}
        >
          {loading ? <Loader2 size={14} className="spin" /> : <Search size={14} />}
          <span>{loading ? "Searching..." : "Search"}</span>
        </button>
      </div>

      {err && (
        <div style={{
          marginTop: 12, padding: "10px 14px", borderRadius: 8,
          background: "rgba(255, 80, 80, 0.12)", color: "var(--danger)",
          fontSize: 13, border: "1px solid rgba(255, 80, 80, 0.3)",
        }}>
          Search failed: {err}
        </div>
      )}

      {results && (
        <div style={{ marginTop: 20, fontSize: 13, color: "var(--text-dim)" }}>
          {results.count} result{results.count === 1 ? "" : "s"}
          {submitted && <> for <strong style={{ color: "var(--text)" }}>"{submitted}"</strong></>}
        </div>
      )}

      <div style={{ marginTop: 16, display: "flex", flexDirection: "column", gap: 16 }}>
        {results?.results?.map((hit, i) => (
          <ResultCard key={hit.asset_id} hit={hit} rank={i + 1} />
        ))}
      </div>

      {results && results.count === 0 && (
        <div className="card" style={{ padding: 28, marginTop: 20, textAlign: "center", color: "var(--text-dim)" }}>
          <div style={{ marginBottom: 6 }}><Film size={28} /></div>
          No subclips matched. Try a looser phrasing — retrieval is semantic, so synonyms work.
        </div>
      )}

      <style>{`
        .spin { animation: spin 0.9s linear infinite; }
        @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
      `}</style>
    </div>
  );
}

function ResultCard({ hit, rank }) {
  const [showText, setShowText] = useState(false);
  const scorePct = Math.max(0, Math.min(100, Math.round(hit.score * 100)));
  const scoreColor =
    hit.score >= 0.5 ? "var(--success)" :
    hit.score >= 0.3 ? "var(--vast-blue)" :
    "var(--text-dim)";

  // Parse searchable_keywords (stored as JSON string)
  let keywords = [];
  if (hit.searchable_keywords) {
    try {
      const parsed = JSON.parse(hit.searchable_keywords);
      if (Array.isArray(parsed)) keywords = parsed;
    } catch { /* ignore */ }
  }

  return (
    <div className="card" style={{ padding: 0, overflow: "hidden" }}>
      <div style={{ display: "grid", gridTemplateColumns: "minmax(280px, 360px) 1fr", gap: 0 }}>
        {/* Left: embedded video */}
        <div style={{ background: "#000", display: "flex", alignItems: "center", justifyContent: "center", minHeight: 200 }}>
          {hit.s3_path ? (
            <video
              src={videoURL(hit.s3_path)}
              controls
              preload="metadata"
              style={{ width: "100%", height: "100%", maxHeight: 320, display: "block" }}
            />
          ) : (
            <div style={{ color: "var(--text-dim)", padding: 20 }}>
              <Play size={24} /> No video path
            </div>
          )}
        </div>

        {/* Right: metadata + text */}
        <div style={{ padding: 18, display: "flex", flexDirection: "column", gap: 10 }}>
          <div style={{ display: "flex", alignItems: "baseline", gap: 10, flexWrap: "wrap" }}>
            <span style={{
              fontSize: 12, fontWeight: 600, color: "var(--text-dim)",
              padding: "2px 8px", borderRadius: 4,
              background: "rgba(255,255,255,0.06)",
            }}>
              #{rank}
            </span>
            <span style={{
              fontSize: 18, fontWeight: 700, color: scoreColor,
              fontFamily: "ui-monospace, SFMono-Regular, monospace",
            }}>
              {scorePct}%
            </span>
            <span style={{ fontSize: 12, color: "var(--text-dim)" }}>
              match ({hit.score.toFixed(4)})
            </span>
            <ScoreBar pct={scorePct} color={scoreColor} />
          </div>

          <div style={{ fontSize: 14, fontWeight: 600 }}>
            {hit.filename || hit.asset_id}
            {typeof hit.subclip_index === "number" && (
              <span style={{ marginLeft: 8, color: "var(--text-dim)", fontWeight: 400, fontSize: 12 }}>
                subclip #{hit.subclip_index + 1}
              </span>
            )}
          </div>

          {hit.content_summary && (
            <div style={{ fontSize: 14, color: "var(--text)", lineHeight: 1.5 }}>
              {hit.content_summary}
            </div>
          )}

          {/* Metadata chips */}
          <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
            {hit.content_category && <Chip icon={<Tag size={11} />} label={hit.content_category} />}
            {hit.content_mood && <Chip label={hit.content_mood} tone="mood" />}
            {hit.content_rating && <Chip label={hit.content_rating} tone="rating" />}
            {keywords.slice(0, 6).map((k) => <Chip key={k} label={k} tone="kw" />)}
          </div>

          {/* Show-me-the-evidence toggle */}
          {hit.embedded_text && (
            <div>
              <button
                className="picker-btn picker-btn-dim"
                onClick={() => setShowText((v) => !v)}
                style={{ fontSize: 12, padding: "6px 10px" }}
              >
                {showText ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
                <span>{showText ? "Hide" : "Show"} text the match was based on</span>
              </button>
              {showText && (
                <pre style={{
                  marginTop: 8, padding: 12, borderRadius: 6,
                  background: "rgba(0,0,0,0.3)", border: "1px solid var(--border)",
                  fontSize: 12, lineHeight: 1.5, whiteSpace: "pre-wrap",
                  color: "var(--text-dim)", maxHeight: 300, overflow: "auto",
                }}>
                  {hit.embedded_text}
                </pre>
              )}
            </div>
          )}

          {hit.s3_path && (
            <div style={{ fontSize: 11, color: "var(--text-dim)", fontFamily: "ui-monospace, monospace", wordBreak: "break-all" }}>
              {hit.s3_path}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function ScoreBar({ pct, color }) {
  return (
    <div style={{
      flex: 1, minWidth: 80, maxWidth: 160,
      height: 6, borderRadius: 3, overflow: "hidden",
      background: "rgba(255,255,255,0.08)",
    }}>
      <div style={{
        width: `${pct}%`, height: "100%",
        background: color, transition: "width 0.3s ease",
      }} />
    </div>
  );
}

function Chip({ icon, label, tone }) {
  const toneMap = {
    mood: { bg: "rgba(200, 120, 255, 0.12)", color: "#c78fff" },
    rating: { bg: "rgba(255, 200, 80, 0.12)", color: "#ffc850" },
    kw: { bg: "rgba(255,255,255,0.04)", color: "var(--text-dim)" },
  };
  const s = toneMap[tone] || { bg: "var(--vast-blue-dim)", color: "var(--vast-blue)" };
  return (
    <span style={{
      display: "inline-flex", alignItems: "center", gap: 4,
      padding: "3px 8px", borderRadius: 12,
      fontSize: 11, fontWeight: 500,
      background: s.bg, color: s.color,
      whiteSpace: "nowrap",
    }}>
      {icon}
      {label}
    </span>
  );
}
