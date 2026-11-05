// Registered users (metadata store) shared by every 'user' dropdown on the page.
// One request serves all fields; reload() re-reads the database.

import { useEffect, useState } from "react";
import { fetchUsers } from "../api";

let state = { status: "idle", users: [], error: null };
let inflight = null;
const listeners = new Set();

function set(next) {
  state = next;
  listeners.forEach((fn) => fn(state));
}

function load(refresh = false) {
  if (inflight) return inflight;
  set({ ...state, status: "loading", error: null });
  inflight = fetchUsers(refresh)
    .then(({ ok, data }) => {
      if (ok && data?.ok) set({ status: "ready", users: data.users || [], error: null });
      else set({ status: "error", users: state.users, error: data?.message || "Could not load users." });
    })
    .finally(() => {
      inflight = null;
    });
  return inflight;
}

export function useUsers() {
  const [snapshot, setSnapshot] = useState(state);
  useEffect(() => {
    listeners.add(setSnapshot);
    if (state.status === "idle") load();
    else setSnapshot(state);
    return () => listeners.delete(setSnapshot);
  }, []);
  return { ...snapshot, reload: () => load(true) };
}

export const userKey = (u) => (u ? JSON.stringify([u.user_id, u.domain]) : "");

export function userLabel(u) {
  return u.name ? `${u.name} (${u.user_id} · ${u.domain})` : `${u.user_id} · ${u.domain}`;
}
