import { API_BASE_URL } from "./data/defaults.js";

export async function pauseConnection(connectionId) {
  try {
    const response = await fetch(`${API_BASE_URL}/connections/pause/${connectionId}`, {
      method: "POST",
    });
    const response_data = await response.json();
    if (response.ok) {
      const paused = response_data.is_connected === true && response_data.is_streaming === false;
      if (!paused) console.error(`Pause succeeded but ${connectionId} is still streaming`);
      return paused;
    }
    console.error(`Pause ${connectionId} failed (${response.status}):`, response_data);
    return false;
  } catch (error) {
    console.error(`Failed to pause ${connectionId}:`, error);
    return false;
  }
}

export async function resumeConnection(connectionId) {
  const response = await fetch(`${API_BASE_URL}/connections/resume/${connectionId}`, {
    method: "POST",
  });
  let data;
  try {
    data = await response.json();
  } catch {
    if (response.ok) throw new Error("The server returned an invalid resume response.");
  }
  if (!response.ok) {
    const error = new Error(typeof data?.detail === "string"
      ? data.detail : `Could not resume connection (HTTP ${response.status}).`);
    error.status = response.status;
    throw error;
  }
  if (data?.is_connected !== true || data?.is_streaming !== true) {
    throw new Error("The backend did not confirm that the connection resumed.");
  }
  return data;
}

export async function connectConnection(
  connectionId,
  adapter_name,
  adapter_cfg,
  streamer_cfg,
  is_polling,
  polling_rate_hz
) {
  const form = new FormData();
  form.append("adapter_name", adapter_name);
  form.append("adapter_text", adapter_cfg);
  form.append("streamer_text", streamer_cfg);
  form.append("is_polling", String(is_polling));
  form.append("polling_rate_hz", String(polling_rate_hz));

  console.log(`DEBUG: Calling connection endpoint ${API_BASE_URL}/connections/connect/${connectionId}`);
  const response = await fetch(`${API_BASE_URL}/connections/connect/${connectionId}`, {
    method: "POST",
    body: form,
  });

  return response;
}

export async function disconnectConnection(connectionId) {
  let response;
  try {
    response = await fetch(`${API_BASE_URL}/connections/disconnect/${connectionId}`, {
      method: "POST",
    });
  } catch (error) {
    console.error(`Failed to disconnect ${connectionId} on backend:`, error);
    return false;
  }
  console.log(`DEBUG: Connection ${connectionId} disconnected successfully`);

  if (response.status === 404) {
    console.error(`Connection ${connectionId} not found on backend.`);
    return true;
  } else if (response.ok) {
    const data = await response.json();
    return data.disconnected;
  } else {
    console.error(`Failed to disconnect ${connectionId} on backend.`);
    return false;
  }
}

export async function fetchStreamingStatus(connectionId) {
  try {
    const response = await fetch(`${API_BASE_URL}/connections/streaming-status/${connectionId}`);
    if (!response.ok) {
      const error = new Error(`HTTP error! status: ${response.status}`);
      error.status = response.status;
      throw error;
    }
    const data = await response.json();
    if (typeof data?.is_connected !== "boolean" || typeof data?.is_streaming !== "boolean") {
      throw new Error("The server returned an invalid streaming status response.");
    }
    return data;
  } catch (error) {
    const failure = new Error(`Failed to fetch streaming status for ${connectionId}: ${error.message}`);
    failure.status = error.status;
    throw failure;
  }
}

export async function fetchAdapters() {
  try {
    const res = await fetch(`${API_BASE_URL}/connections/adapters`);
    if (!res.ok) throw new Error("Failed to fetch adapters");
    return await res.json();
  } catch (e) {
    throw new Error(`Failed to fetch adapters: ${e.message}`);
  }
}

export async function validateAdapterConfig(adapter_name, adapter_cfg) {
  try {
    const adp_form = new FormData();
    adp_form.append("adapter_name", adapter_name);
    adp_form.append("text", adapter_cfg);
    const adp_resp = await fetch(`${API_BASE_URL}/connections/validate/adapter`, {
      method: "POST",
      body: adp_form,
    });
    const adp_data = await adp_resp.json();
    return adp_data.is_valid;
  } catch (error) {
    console.error(`Failed to validate adapter configuration:`, error);
    return false;
  }
}

export async function validateStreamerConfig(streamer_cfg) {
  try {
    const streamer_form = new FormData();
    streamer_form.append("text", streamer_cfg);
    const streamer_resp = await fetch(`${API_BASE_URL}/connections/validate/streamer`, {
      method: "POST",
      body: streamer_form,
    });
    const streamer_data = await streamer_resp.json();
    return streamer_data.is_valid;
  } catch (error) {
    console.error(`Failed to validate streamer configuration:`, error);
    return false;
  }
}
