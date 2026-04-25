const BASE = "/api";

async function fetchJSON(url) {
  const res = await fetch(`${BASE}${url}`);
  if (!res.ok) throw new Error(`API error ${res.status}`);
  return res.json();
}

export function getPersonas() {
  return fetchJSON("/personas");
}

export function getPersonaUseCases(personaId) {
  return fetchJSON(`/personas/${personaId}/usecases`);
}

export function getUseCaseData(ucId, params = {}) {
  const qs = new URLSearchParams(params).toString();
  return fetchJSON(`/usecases/${ucId}/data${qs ? `?${qs}` : ""}`);
}

export function getAssets(limit = 50, columns = [], search = "", showSubclips = false) {
  const params = new URLSearchParams({ limit });
  if (columns.length) params.set("columns", columns.join(","));
  if (search) params.set("search", search);
  if (showSubclips) params.set("show_subclips", "true");
  return fetchJSON(`/assets?${params}`);
}

export function getAssetFullDetail(assetId) {
  return fetchJSON(`/assets/${assetId}/detail`);
}

export function getAssetColumns() {
  return fetchJSON("/assets/columns");
}

export function getAssetDetail(assetId) {
  return fetchJSON(`/assets/${assetId}`);
}

export function getAssetRelationships(assetId) {
  return fetchJSON(`/assets/${assetId}/relationships`);
}

export function getStats() {
  return fetchJSON("/stats");
}

export async function uploadFiles(files) {
  const form = new FormData();
  for (const f of files) form.append("file", f);
  const res = await fetch(`${BASE}/upload`, { method: "POST", body: form });
  if (!res.ok) throw new Error(`Upload failed ${res.status}`);
  return res.json();
}

export function semanticSearch(q, limit = 10) {
  const params = new URLSearchParams({ q, limit });
  return fetchJSON(`/semantic-search?${params}`);
}

export function videoURL(s3Path) {
  return `${BASE}/video?path=${encodeURIComponent(s3Path)}`;
}

// ── AI Clipper view (Phase 1+2 read API) ─────────────────────────────

export function listSources() {
  return fetchJSON("/sources");
}

export function getSource(sourceId) {
  return fetchJSON(`/sources/${encodeURIComponent(sourceId)}`);
}


// ── Delivery packages (Phase 3) + C2PA ───────────────────────────────

export function listPackages() {
  return fetchJSON("/packages");
}

export function getPackage(packageId) {
  return fetchJSON(`/packages/${encodeURIComponent(packageId)}`);
}

export function getPackageManifest(packageId) {
  return fetchJSON(`/packages/${encodeURIComponent(packageId)}/manifest`);
}

// Live C2PA verify: hits c2patool server-side, returns the parsed
// report including the active manifest, every assertion, signature
// info, cert details, and the AI-disclosure assertion extracted for
// quick UI access.
export function getRenditionC2pa(packageId, renditionId) {
  return fetchJSON(
    `/packages/${encodeURIComponent(packageId)}/renditions/${encodeURIComponent(renditionId)}/c2pa`,
  );
}


// ── Runtime function configs ──────────────────────────────────────────

export function listConfigScopes() {
  return fetchJSON("/configs");
}

export function getConfigScope(scope) {
  return fetchJSON(`/configs/${encodeURIComponent(scope)}`);
}

export async function updateConfigValue(scope, key, value, updatedBy = "webapp") {
  const res = await fetch(
    `${BASE}/configs/${encodeURIComponent(scope)}/${encodeURIComponent(key)}`,
    {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ value, updated_by: updatedBy }),
    },
  );
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(err.error || `HTTP ${res.status}`);
  }
  return res.json();
}

export async function resetConfigValue(scope, key) {
  const res = await fetch(
    `${BASE}/configs/${encodeURIComponent(scope)}/${encodeURIComponent(key)}/reset`,
    { method: "POST" },
  );
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(err.error || `HTTP ${res.status}`);
  }
  return res.json();
}

// Bulk update many keys in one scope. `updates` = [{key, value}, ...].
export async function bulkUpdateScope(scope, updates, updatedBy = "webapp") {
  const res = await fetch(
    `${BASE}/configs/${encodeURIComponent(scope)}`,
    {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ updates, updated_by: updatedBy }),
    },
  );
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(err.error || `HTTP ${res.status}`);
  }
  return res.json();
}

// Reset every setting in a scope to its default_value.
export async function resetScope(scope) {
  const res = await fetch(
    `${BASE}/configs/${encodeURIComponent(scope)}/reset`,
    { method: "POST" },
  );
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: `HTTP ${res.status}` }));
    throw new Error(err.error || `HTTP ${res.status}`);
  }
  return res.json();
}
