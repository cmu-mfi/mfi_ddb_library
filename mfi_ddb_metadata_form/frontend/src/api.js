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

function post(path, values) {
  const headers = { "Content-Type": "application/json" };
  // Platform admins (admin token) may use reserved accounts such as superadmin.
  const token = getAdminToken();
  if (token) headers["X-Admin-Token"] = token;
  return request(path, { method: "POST", headers, body: JSON.stringify({ values }) });
}

// ---- user directory (registered users, via the Retrieval API /users) -------

export const searchUsers = (search, limit = 8) =>
  request(`/directory/users?${new URLSearchParams({ search: search || "", limit: String(limit) })}`);
export const checkUser = (domain, userId) =>
  request(`/directory/users/${encodeURIComponent(domain || "-")}/${encodeURIComponent(userId)}`);

export const previewForm = (formId, values) => post(`/forms/${encodeURIComponent(formId)}/preview`, values);
export const submitForm = (formId, values) => post(`/forms/${encodeURIComponent(formId)}/submit`, values);

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
