/*--------------------------------------------------------------------------------
Provides functions for calling the backend API.

TODO: Add stored return value descriptions
...

--------------------------------------------------------------------------------*/

import { API_BASE_URL } from "./data/defaults";

export async function pauseConnection(connectionId) {
  try {
    const response = await fetch(
      `${API_BASE_URL}/connections/pause/${connectionId}`,
      {
        method: "POST",
      }
    );
    const response_data = await response.json();
    if (response.ok && !response_data.is_streaming) {
      console.log(
        `System paused ${connectionId} on backend (not saved to localStorage)`
      );
      return true;
    } else if (response.ok) {
      console.error(`Failed to pause ${connectionId} on backend`);
      return false;
    }
  } catch (error) {
    console.error(`Failed to pause ${connectionId} on backend:`, error);
    return false;
  }
}

export async function resumeConnection(connectionId) {
  let response;
  try {
    response = await fetch(
      `${API_BASE_URL}/connections/resume/${connectionId}`,
      {
        method: "POST",
      }
    );
  } catch (error) {
    console.error(`Failed to resume ${connectionId} on backend:`, error);
    return false;
  }
  const response_data = await response.json();
  if (response.ok && response_data.is_streaming) {
    console.log(
      `System resumed ${connectionId} on backend (not saved to localStorage)`
    );
    return true;
  } else if (response.ok) {
    console.error(`Failed to resume ${connectionId} on backend`);
    return false;
  }
}

export async function connectConnection(
  connectionId,
  adapter_name,
  adapter_cfg,
  streamer_cfg) {

  const form = new FormData();
  form.append("adapter_name", adapter_name);
  form.append("adapter_text", adapter_cfg);
  form.append("streamer_text", streamer_cfg);

  console.log(
    `DEBUG: Calling connection endpoint ${API_BASE_URL}/config/connect/${connectionId}`
  );
  const response = await fetch(
    `${API_BASE_URL}/connections/connect/${connectionId}`, {
    method: "POST",
    body: form,
  });

  return response;
}

export async function disconnectConnection(connectionId) {
  let response;
  try {
    response = await fetch(
      `${API_BASE_URL}/connections/disconnect/${connectionId}`, {
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
    const response = await fetch(
      `${API_BASE_URL}/connections/streaming-status/${connectionId}`);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    const return_data = await response.json();
    return return_data;
  } catch (error) {
    throw new Error(`Failed to fetch streaming status for ${connectionId}: ${error.message}`);
  }

}

export async function fetchAdapters() {
  try {
    const res = await fetch(`${API_BASE_URL}/connections/adapters`);
    if (!res.ok) throw new Error("Failed to fetch adapters");
    const data = await res.json();
    return data;
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
  try{
    const streamer_form = new FormData();
    streamer_form.append("text", streamer_cfg);
    const streamer_resp = await fetch(`${API_BASE_URL}/connections/validate/streamer`, {
      method: "POST",
      body: streamer_form
    });
    const streamer_data = await streamer_resp.json();
    return streamer_data.is_valid;
  } catch (error) {
    console.error(`Failed to validate streamer configuration:`, error);
    return false;
  }
}