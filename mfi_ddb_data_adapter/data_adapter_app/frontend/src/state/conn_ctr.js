const STORAGE_KEY = "adapter_connections";

export function getConnCtr() {
  try {
    const data = localStorage.getItem(STORAGE_KEY);
    if (!data) return 100;
    const connections = JSON.parse(data);
    const ids = Object.keys(connections).map(Number).filter((n) => !isNaN(n) && n > 0);
    return ids.length > 0 ? Math.max(...ids) : 100;
  } catch {
    return 100;
  }
}

// No-op — ID is always derived from localStorage, no in-memory state to update
export function setConnCtr(_value) {}
