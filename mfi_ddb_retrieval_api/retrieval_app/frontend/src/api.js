// All calls go through the nginx proxy: /api/<x> -> <RWS>/mfi-ddb/<x>
const API_BASE_URL = import.meta.env.VITE_API_URL || "/api";

function errorMessage(status, data, text) {
  if (data?.detail) {
    if (typeof data.detail === "string") return data.detail;
    if (Array.isArray(data.detail)) {
      return data.detail
        .map((d) => `${(d.loc || []).filter((p) => p !== "body").join(".") || "request"}: ${d.msg}`)
        .join("; ");
    }
  }
  if (data?.message) return data.message;
  if (status === 0) return text;
  if (status === 502 || status === 504) return `The Retrieval API did not respond (${status}). Is it running?`;
  if (status >= 500) return `The Retrieval API failed (${status}). Check the RWS logs: docker logs mfi-rws-app`;
  return text || `Request failed (${status})`;
}

export async function request(path, { method = "GET", body } = {}) {
  const started = performance.now();
  let response;
  try {
    response = await fetch(`${API_BASE_URL}${path}`, {
      method,
      headers: body ? { "Content-Type": "application/json" } : undefined,
      body: body ? JSON.stringify(body) : undefined,
    });
  } catch (error) {
    return {
      ok: false,
      status: 0,
      ms: performance.now() - started,
      data: null,
      error: `Cannot reach the server: ${error.message}`,
    };
  }
  const text = await response.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = null;
  }
  const ms = performance.now() - started;
  const ok = response.ok && (!data || data.status !== "error");
  return {
    ok,
    status: response.status,
    ms,
    bytes: text.length,
    data,
    raw: text,
    error: ok ? null : errorMessage(response.status, data, text.slice(0, 300)),
  };
}

export const fetchHealth = () => request("/health");

export const listEndpoints = () => request("/type0");
export const searchTrials = (body) => request("/type1", { method: "POST", body });

/** File name from a Content-Disposition header (plain or RFC 5987 `filename*=`). */
function fileNameFrom(header) {
  const encoded = /filename\*\s*=\s*[^']*''([^;]+)/i.exec(header || "");
  if (encoded) {
    try {
      return decodeURIComponent(encoded[1].trim().replace(/^"|"$/g, ""));
    } catch {
      /* fall through to the plain name */
    }
  }
  const plain = /filename\s*=\s*"?([^";]+)"?/i.exec(header || "");
  return plain ? plain[1].trim() : null;
}

/** Type 2 as a CSV/JSON file download. See downloadFile. */
export const downloadTrialData = (body, fallbackName, signal) => downloadFile("/type2", body, fallbackName, signal);

/**
 * Type 4: the trial's files (one file as is, several as a .zip). The file
 * list comes back in the X-File-Metadata header, as `metadata`.
 */
export const downloadTrialFiles = (body, fallbackName, signal) => downloadFile("/type4", body, fallbackName, signal);

/**
 * POST to `path` and save the response as a file, never parsed or rendered.
 * Name from the API's Content-Disposition, else `fallbackName`. Pass an
 * AbortSignal to cancel. Returns { ok, bytes, name, metadata } or { ok: false, error }.
 */
async function downloadFile(path, body, fallbackName, signal) {
  let response;
  try {
    response = await fetch(`${API_BASE_URL}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal,
    });
  } catch (error) {
    if (error.name === "AbortError") return { ok: false, cancelled: true, error: "Download cancelled." };
    return { ok: false, error: `Cannot reach the server: ${error.message}` };
  }
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    let data = null;
    try {
      data = text ? JSON.parse(text) : null;
    } catch {
      data = null;
    }
    return { ok: false, status: response.status, error: errorMessage(response.status, data, text.slice(0, 300)) };
  }
  let blob;
  try {
    blob = await response.blob();
  } catch (error) {
    if (error.name === "AbortError") return { ok: false, cancelled: true, error: "Download cancelled." };
    return { ok: false, error: `Download interrupted: ${error.message}` };
  }
  const name = fileNameFrom(response.headers.get("Content-Disposition")) || fallbackName;
  let metadata = null;
  try {
    metadata = JSON.parse(response.headers.get("X-File-Metadata") || "null");
  } catch {
    metadata = null;
  }
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = name;
  document.body.appendChild(link);
  link.click();
  link.remove();
  setTimeout(() => URL.revokeObjectURL(url), 60e3);
  return { ok: true, bytes: blob.size, name, metadata };
}
