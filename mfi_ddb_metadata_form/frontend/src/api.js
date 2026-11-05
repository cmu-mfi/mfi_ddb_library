const API_BASE_URL = import.meta.env.VITE_API_URL || "/api";

async function request(path, options = {}) {
  let response;
  try {
    response = await fetch(`${API_BASE_URL}${path}`, options);
  } catch (error) {
    return { ok: false, status: 0, data: { message: `Cannot reach the server: ${error.message}` } };
  }
  let data = null;
  try {
    data = await response.json();
  } catch {
    data = { message: `Unexpected response from server (${response.status})` };
  }
  return { ok: response.ok, status: response.status, data };
}

export const fetchConfig = () => request("/config");
export const fetchHealth = () => request("/health");
export const fetchSubmissions = (limit = 20) => request(`/submissions?limit=${limit}`);
export const fetchUsers = (refresh = false) => request(`/users${refresh ? "?refresh=true" : ""}`);

function post(path, values, revises) {
  return request(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(revises ? { values, revises } : { values }),
  });
}

// `revises` ({entry_id, version, trial_uuid}) saves the values as the next version of that entry.
export const previewForm = (formId, values, revises) =>
  post(`/forms/${encodeURIComponent(formId)}/preview`, values, revises);
export const submitForm = (formId, values, revises) =>
  post(`/forms/${encodeURIComponent(formId)}/submit`, values, revises);
/** Label PDF for entries -> {ok: true, blob} or {ok: false, message}. */
export async function printLabels(formId, size, entries) {
  let response;
  try {
    response = await fetch(`${API_BASE_URL}/labels`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        form_id: formId,
        size,
        entries: entries.map((e) => ({
          entry_id: e.entry_id,
          version: e.version,
          trial_id: e.trial_id,
          values: e.values,
        })),
      }),
    });
  } catch (error) {
    return { ok: false, message: `Cannot reach the server: ${error.message}` };
  }
  if (response.ok) return { ok: true, blob: await response.blob() };
  try {
    return { ok: false, message: (await response.json()).message };
  } catch {
    return { ok: false, message: `Label printing failed (${response.status})` };
  }
}

export const searchEntries = (formId, query) =>
  request(`/forms/${encodeURIComponent(formId)}/entries`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(query),
  });

// ---- form builder ----------------------------------------------------------

const TOKEN_KEY = "mfi-form-builder-token";

export function getAdminToken() {
  try {
    return sessionStorage.getItem(TOKEN_KEY) || "";
  } catch {
    return "";
  }
}

export function setAdminToken(token) {
  try {
    if (token) sessionStorage.setItem(TOKEN_KEY, token);
    else sessionStorage.removeItem(TOKEN_KEY);
  } catch {
    /* storage unavailable: token just isn't remembered */
  }
}

function builderRequest(path, method, body) {
  const headers = { "Content-Type": "application/json" };
  const token = getAdminToken();
  if (token) headers["X-Admin-Token"] = token;
  return request(path, { method, headers, body: body ? JSON.stringify(body) : undefined });
}

export const fetchBuilderForms = () => request("/builder/forms");
export const createBuilderForm = (form) => builderRequest("/builder/forms", "POST", form);
export const updateBuilderForm = (id, form) =>
  builderRequest(`/builder/forms/${encodeURIComponent(id)}`, "PUT", form);
export const deleteBuilderForm = (id) => builderRequest(`/builder/forms/${encodeURIComponent(id)}`, "DELETE");

// ---- settings page -----------------------------------------------------------

export const fetchSettings = () => request("/settings");
export const saveSettings = (body) => builderRequest("/settings", "PUT", body);
export const resetSettings = () => builderRequest("/settings", "DELETE");
export const testConnection = (mqtt) => builderRequest("/settings/test", "POST", { mqtt });
