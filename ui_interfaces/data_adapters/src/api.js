// src/api.js

// Reads from .env or falls back to localhost:8000
const API_BASE = process.env.REACT_APP_API_URL || "http://localhost:8000";

/**
 * callConfig
 * POST a multipart‐form to /config/validate,/config/connect, etc.
 * @param {string} path         path under the API (e.g. "/config/validate")
 * @param {object} options      { file, text, topicFamily }
 */
export async function callConfig(path, { file, text, topicFamily, id } = {}) {
  const url = `${API_BASE}${path}`;
  const form = new FormData();

  if (file) form.append("file", file);
  else if (text) form.append("text", text);

  if (topicFamily) form.append("topic_family", topicFamily);
  if (id) form.append("id", id);

  const res = await fetch(url, {
    method: "POST",
    body: form,
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(body);
  }

  return res.json();
}
