import { API_BASE_URL } from "./data/defaults.js";
import { request, isRequestInterruption } from "./lib/request.js";

async function readActionResponse(response, action) {
  let data;
  try { data = await response.json(); } catch (error) {
    if (isRequestInterruption(error)) throw error;
  }
  if (!response.ok) {
    const detail = typeof data?.detail === "string" ? ` ${data.detail}` : "";
    const error = new Error(`Could not ${action} (HTTP ${response.status}).${detail}`);
    error.status = response.status;
    throw error;
  }
  if (!data || typeof data !== "object") {
    throw new Error(`Could not ${action}: the server returned an invalid response. Please retry.`);
  }
  return data;
}

function actionRequest(path, init, action, consume, options) {
  return request(`${API_BASE_URL}/connections/${path}`, init, consume, options).catch(error => {
    if (isRequestInterruption(error) || error?.status || !(error instanceof TypeError)) throw error;
    throw new Error(`Could not ${action}. Check the server connection and try again.`);
  });
}

export async function pauseConnection(connectionId, options) {
  return actionRequest(`pause/${connectionId}`, { method: "POST" }, "pause the connection", async response => {
    const data = await readActionResponse(response, "pause the connection");
    if (data.is_connected !== true || data.is_streaming !== false) {
      throw new Error("The backend did not confirm that the connection paused. Please retry.");
    }
    return true;
  }, options);
}

export async function resumeConnection(connectionId, options) {
  return actionRequest(`resume/${connectionId}`, { method: "POST" }, "resume the connection", async response => {
    const data = await readActionResponse(response, "resume the connection");
    if (data?.is_connected !== true || data?.is_streaming !== true) {
      throw new Error("The backend did not confirm that the connection resumed.");
    }
    return data;
  }, options);
}

export async function connectConnection(
  connectionId,
  adapter_name,
  adapter_cfg,
  streamer_cfg,
  is_polling,
  polling_rate_hz,
  options
) {
  const form = new FormData();
  form.append("adapter_name", adapter_name);
  form.append("adapter_text", adapter_cfg);
  form.append("streamer_text", streamer_cfg);
  form.append("is_polling", String(is_polling));
  form.append("polling_rate_hz", String(polling_rate_hz));

  return actionRequest(`connect/${connectionId}`, {
    method: "POST",
    body: form,
  }, "connect the adapter", async response => {
    const data = await readActionResponse(response, "connect the adapter");
    // Return parsed data so callers cannot read a body outside the deadline.
    return { ok: response.ok, status: response.status, data };
  }, options);
}

export async function disconnectConnection(connectionId, options) {
  return actionRequest(`disconnect/${connectionId}`, { method: "POST" }, "disconnect the connection", async response => {
    if (response.status === 404) return true;
    const data = await readActionResponse(response, "disconnect the connection");
    if (data.disconnected !== true) {
      throw new Error("The backend did not confirm disconnection. Please retry.");
    }
    return true;
  }, options);
}

export function fetchStreamingStatus(connectionId, options) {
  return actionRequest(`streaming-status/${connectionId}`, {}, "check connection status", async response => {
    const data = await readActionResponse(response, "check connection status");
    if (typeof data?.is_connected !== "boolean" || typeof data?.is_streaming !== "boolean") {
      throw new Error("The server returned an invalid streaming status response.");
    }
    return data;
  }, options);
}

export function fetchAdapters(options) {
  return actionRequest("adapters", {}, "load data adapters", async response => {
    const data = await readActionResponse(response, "load data adapters");
    if (!Array.isArray(data) || data.some(adapter => !adapter || typeof adapter.name !== "string" || !adapter.name.trim() ||
        (adapter.configExample?.configuration != null && typeof adapter.configExample.configuration !== "string"))) {
      throw new Error("The server returned an invalid adapter list. Please retry loading adapters.");
    }
    return data;
  }, options);
}

export function fetchServerHealth(options) {
  return request(`${API_BASE_URL}/connections/health`, {}, response => response.ok,
    { timeoutMs: 3000, ...options });
}

function validationRequestError(message, status) {
  const error = new Error(message);
  error.kind = "request";
  error.status = status;
  return error;
}

function validationResult(data) {
  const errors = Array.isArray(data.errors) && data.errors.length
    ? data.errors : [{ loc: [], message: "Configuration is invalid.", type: "configuration" }];
  if (!data.is_valid && errors.some(issue => !issue || typeof issue !== "object" ||
      typeof (issue.message ?? issue.msg) !== "string" ||
      (issue.loc !== undefined && (!Array.isArray(issue.loc) ||
        issue.loc.some(key => typeof key !== "string" && typeof key !== "number"))))) {
    throw validationRequestError("The validation service returned invalid error details. Please retry.");
  }
  return {
    is_valid: data.is_valid,
    errors: data.is_valid ? [] : errors.map(issue => ({
      loc: Array.isArray(issue.loc) ? issue.loc : [],
      message: issue.message || issue.msg || "Invalid value.",
      type: issue.type || "configuration",
      line: issue.line,
      column: issue.column,
    })),
  };
}

async function validateConfig(target, form, options) {
  try {
    return await request(`${API_BASE_URL}/connections/validate/${target}`, { method: "POST", body: form }, async response => {
      let data;
      try { data = await response.json(); } catch (error) {
        if (isRequestInterruption(error)) throw error;
        // Report invalid or non-JSON responses below.
      }
      if (!response.ok) {
        if (response.status === 400 || response.status === 422) {
          if (data?.detail?.is_valid === false) return validationResult(data.detail);
          if (typeof data?.detail === "string") {
            return validationResult({ is_valid: false, errors: [{ message: data.detail }] });
          }
          if (Array.isArray(data?.detail)) return validationResult({ is_valid: false, errors: data.detail });
        }
        throw validationRequestError(`Validation service request failed (HTTP ${response.status}). Please retry.`, response.status);
      }
      if (typeof data?.is_valid !== "boolean") {
        throw validationRequestError("The validation service returned an invalid response. Please retry.", response.status);
      }
      return validationResult(data);
    }, options);
  } catch (error) {
    if (error.name === "AbortError") throw error;
    if (error.name === "TimeoutError") {
      error.kind = "request";
      throw error;
    }
    if (error.kind === "request") throw error;
    throw validationRequestError("Could not reach the validation service. Check the connection and retry.");
  }
}

export async function validateAdapterConfig(adapter_name, adapter_cfg, options) {
  const form = new FormData();
  form.append("adapter_name", adapter_name);
  form.append("text", adapter_cfg);
  return validateConfig("adapter", form, options);
}

export async function validateStreamerConfig(streamer_cfg, options) {
  const form = new FormData();
  form.append("text", streamer_cfg);
  return validateConfig("streamer", form, options);
}
