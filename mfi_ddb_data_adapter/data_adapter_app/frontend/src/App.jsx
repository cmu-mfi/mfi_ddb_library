import { useState, useEffect, useCallback, useMemo } from "react";
import ConnectionList from "./components/ConnectionList";
import ConnectionModal from "./components/ConnectionModal";
import logoMfi from "./images/logo_mfi.png";
import { API_BASE_URL } from "./data/defaults";
import { fetchAllConnections, deleteConnection } from "./api";

const serverStatusCls = {
  online:   "bg-green-600 text-white",
  offline:  "bg-red-600 text-white",
  checking: "bg-gray-500 text-white",
};

function App() {
  const [connections, setConnections] = useState([]);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [editingConnection, setEditingConnection] = useState(null);
  const [serverStatus, setServerStatus] = useState("checking");

  const checkServerHealth = useCallback(async () => {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 3000);
      const response = await fetch(`${API_BASE_URL}/connections/health`, { signal: controller.signal });
      clearTimeout(timeoutId);
      return response.ok;
    } catch (error) {
      console.error("Health check failed:", error);
      return false;
    }
  }, []);

  // Backend is the sole source of truth for connections: no localStorage cache,
  // no client-side restore/reconciliation. Whatever the backend reports here is
  // the list, refreshed on an interval below.
  const loadConnections = useCallback(async () => {
    try {
      const all = await fetchAllConnections();
      setConnections(all);
    } catch (error) {
      console.error("Failed to load connections:", error);
    }
  }, []);

  const handleSaveConnection = useCallback(() => {
    loadConnections();
    setIsModalOpen(false);
    setEditingConnection(null);
  }, [loadConnections]);

  const handleEditConnection = useCallback((connection) => {
    setEditingConnection(connection);
    setIsModalOpen(true);
  }, []);

  const handleTerminateConnection = useCallback(async (connectionId) => {
    const result = await deleteConnection(connectionId);
    if (!result) return;
    loadConnections();
  }, [loadConnections]);

  const handlePauseConnection = useCallback(() => {
    loadConnections();
  }, [loadConnections]);

  const handleResumeConnection = useCallback(() => {
    loadConnections();
  }, [loadConnections]);

  const handleNewConnection = useCallback(() => setIsModalOpen(true), []);

  // Stable reference: editingConnection only actually changes when the user
  // picks a different connection to edit, not on every periodic re-render
  // from loadConnections()/checkServerHealth polling. Without this, a new {}
  // literal every render would retrigger ConnectionModal's reset-on-open
  // effect (keyed on this object) every ~5s and wipe in-progress form input.
  const modalInitialData = useMemo(() => editingConnection || {}, [editingConnection]);

  // Server health monitor
  useEffect(() => {
    const monitorServer = async () => {
      const isOnline = await checkServerHealth();
      setServerStatus(isOnline ? "online" : "offline");
    };
    monitorServer();
    const interval = setInterval(monitorServer, 5000);
    return () => clearInterval(interval);
  }, [checkServerHealth]);

  // Load active connections on mount, then keep them fresh on an interval.
  useEffect(() => {
    loadConnections();
    const interval = setInterval(loadConnections, 5000);
    return () => clearInterval(interval);
  }, [loadConnections]);

  return (
    <div className="min-h-screen bg-gray-100">
      <div className="flex items-center gap-4 px-8 pt-8 justify-between">
        <div className="w-[250px] flex items-center gap-3">
          <a href="https://cmu-mfi.github.io/ddb/" target="_blank" rel="noreferrer">
            <img src={logoMfi} alt="Manufacturing Futures Institute Logo" width={200} />
          </a>
        </div>
        <div
          className={`relative flex w-[140px] items-center gap-1.5 px-3 py-2 rounded-full text-xs font-medium shadow-md backdrop-blur-sm ${serverStatusCls[serverStatus] || serverStatusCls.checking}`}
        >
          <span className="w-2 h-2 rounded-full bg-current shrink-0" />
          Server: {serverStatus}
        </div>
      </div>

      <main>
        <ConnectionList
          connections={connections}
          onNewConnection={handleNewConnection}
          onEditConnection={handleEditConnection}
          onTerminateConnection={handleTerminateConnection}
          onPauseConnection={handlePauseConnection}
          onResumeConnection={handleResumeConnection}
        />
      </main>

      {isModalOpen && (
        <ConnectionModal
          isOpen={isModalOpen}
          onClose={() => { setIsModalOpen(false); setEditingConnection(null); }}
          onSave={handleSaveConnection}
          initialData={modalInitialData}
        />
      )}
    </div>
  );
}

export default App;
