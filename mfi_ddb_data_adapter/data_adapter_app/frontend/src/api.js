import { API_BASE_URL } from "./data/defaults";

export async function pauseConnection(connectionId) {
  try {
    const response = await fetch(`${API_BASE_URL}/connections/pause/${connectionId}`, {
      method: "POST",
    });
    const response_data = await response.json();
    if (response.ok) {
      const paused = !response_data.is_streaming;
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
  let response;
  try {
    response = await fetch(`${API_BASE_URL}/connections/resume/${connectionId}`, {
      method: "POST",
    });
  } catch (error) {
    console.error(`Failed to resume ${connectionId} on backend:`, error);
    return false;
  }
  const response_data = await response.json();
  if (response.ok && response_data.is_streaming) {
    console.log(`System resumed ${connectionId} on backend (not saved to localStorage)`);
    return true;
  } else if (response.ok) {
    console.error(`Failed to resume ${connectionId} on backend`);
    return false;
  }
}

// Creates a brand new connection - the backend generates its id (returned
// as `id` in the response body) rather than the client guessing one.
export async function createConnection(
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

  const response = await fetch(`${API_BASE_URL}/connections/connect`, {
    method: "POST",
    body: form,
  });

  return response;
}

// Reconnects an existing connection (e.g. after it was Stopped) using its
// already-stored config. Does not create new connections - see createConnection.
export async function connectConnection(connectionId, adapter_name) {
  const form = new FormData();
  form.append("adapter_name", adapter_name);

  const response = await fetch(`${API_BASE_URL}/connections/connect/${connectionId}`, {
    method: "POST",
    body: form,
  });

  return response;
}

// Applies an updated config to an existing connection. Adapter type can't
// change here - only adp_cfg/streamer_cfg/is_polling/polling_rate_hz.
export async function updateConnectionConfig(
  connectionId,
  adapter_cfg,
  streamer_cfg,
  is_polling,
  polling_rate_hz
) {
  const form = new FormData();
  form.append("adapter_text", adapter_cfg);
  form.append("streamer_text", streamer_cfg);
  form.append("is_polling", String(is_polling));
  form.append("polling_rate_hz", String(polling_rate_hz));

  const response = await fetch(`${API_BASE_URL}/connections/update/${connectionId}`, {
    method: "POST",
    body: form,
  });

  return response;
}

export async function stopConnection(connectionId) {
  let response;
  try {
    response = await fetch(`${API_BASE_URL}/connections/stop/${connectionId}`, {
      method: "POST",
    });
  } catch (error) {
    console.error(`Failed to stop ${connectionId} on backend:`, error);
    return false;
  }
  console.log(`DEBUG: Connection ${connectionId} stopped successfully`);

  if (response.status === 404) {
    console.error(`Connection ${connectionId} not found on backend.`);
    return true;
  } else if (response.ok) {
    const data = await response.json();
    return data.stopped;
  } else {
    console.error(`Failed to stop ${connectionId} on backend.`);
    return false;
  }
}

export async function deleteConnection(connectionId) {
  let response;
  try {
    response = await fetch(`${API_BASE_URL}/connections/delete/${connectionId}`, {
      method: "POST",
    });
  } catch (error) {
    console.error(`Failed to delete ${connectionId} on backend:`, error);
    return false;
  }

  if (response.status === 404) {
    console.error(`Connection ${connectionId} not found on backend.`);
    return true;
  } else if (response.ok) {
    const data = await response.json();
    return Boolean(data.deleted);
  } else {
    console.error(`Failed to delete ${connectionId} on backend.`);
    return false;
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

export async function fetchAllConnections() {
  try {
    const res = await fetch(`${API_BASE_URL}/connections/all`);
    if (!res.ok) throw new Error("Failed to fetch connections");
    return await res.json();
  } catch (e) {
    throw new Error(`Failed to fetch connections: ${e.message}`);
  }
}

export async function fetchStreamerConfig() {
  try {
    const res = await fetch(`${API_BASE_URL}/connections/streamer`);
    if (!res.ok) throw new Error("Failed to fetch streamer config");
    return await res.json();
  } catch (e) {
    throw new Error(`Failed to fetch streamer config: ${e.message}`);
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
