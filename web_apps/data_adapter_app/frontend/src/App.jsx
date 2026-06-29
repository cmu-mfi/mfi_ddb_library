import { useState, useEffect, useRef, useCallback } from "react";
import ConnectionList from "./components/ConnectionList";
import ConnectionModal from "./components/ConnectionModal";
import { ConnectionManager } from "./components/ConnectionManager";
import logoMfi from "./images/logo_mfi.png";
import { API_BASE_URL } from "./data/defaults";
import { disconnectConnection, fetchStreamingStatus, connectConnection, resumeConnection } from "./api";

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
  const [isRestoring, setIsRestoring] = useState(false);
  const hasAutoRestoredRef = useRef(false);

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

  const loadConnections = useCallback(() => {
    const savedConnections = ConnectionManager.getSavedConnections();
    const connectionsList = Object.values(savedConnections).map((conn) => ({
      id: conn.id,
      adapter: conn.adapter,
      adapterConfig: conn.adapterConfig,
      streamerConfig: conn.streamerConfig,
      isPolling: conn.isPolling !== false,
      pollingRateHz: conn.pollingRateHz ?? 1,
    }));
    setConnections(connectionsList);
  }, []);

  const performStartupRestore = useCallback(async () => {
    if (hasAutoRestoredRef.current) return;

    const shouldRestore = ConnectionManager.shouldRestoreOnStartup();
    if (!shouldRestore) return;

    setIsRestoring(true);

    const savedConnections = ConnectionManager.getSavedConnections();
    const savedStates = ConnectionManager.getConnectionStates();
    const connectionIds = Object.keys(savedConnections);

    if (connectionIds.length === 0) {
      setIsRestoring(false);
      return;
    }

    let successCount = 0;
    let errorCount = 0;

    for (const connectionId of connectionIds) {
      const connectionData = savedConnections[connectionId];
      const savedState = savedStates[connectionId];
      const targetState = savedState?.state || "streaming";

      // Determine current backend state, treating any error as "not_found"
      let currentStatus;
      try {
        const statusResponse = await fetchStreamingStatus(connectionId);
        currentStatus = statusResponse.is_streaming ? "streaming" : "paused";
      } catch {
        currentStatus = "not_found";
      }

      if (currentStatus === targetState) {
        successCount++;
        continue;
      }

      try {
        if (targetState === "paused") {
          // Connection should be paused — if it doesn't exist on backend that's fine,
          // we just leave it as-is and keep the saved "paused" state.
          successCount++;
        } else {
          // targetState === "streaming"
          if (currentStatus === "not_found") {
            const connectResponse = await connectConnection(
              connectionId,
              connectionData.adapter,
              connectionData.adapterConfig,
              connectionData.streamerConfig,
              connectionData.isPolling !== false,
              connectionData.pollingRateHz ?? 1
            );
            if (!connectResponse.ok) {
              console.error(`Failed to reconnect ${connectionId}`);
              errorCount++;
              continue;
            }
            successCount++;
          } else if (currentStatus === "paused") {
            const resumeSuccess = await resumeConnection(connectionId);
            if (resumeSuccess) { successCount++; } else { errorCount++; }
          }
        }
      } catch (error) {
        console.error(`ERROR: Exception restoring ${connectionId}:`, error);
        errorCount++;
      }
    }

    console.log(`Restore complete — success: ${successCount}, error: ${errorCount}`);
    hasAutoRestoredRef.current = true;
    ConnectionManager.markRestorationComplete();
    setIsRestoring(false);
    loadConnections();
  }, [loadConnections, isRestoring, serverStatus]);

  const verifyAndRestoreConnections = useCallback(async () => {
    if (serverStatus !== "online") return;
    try {
      const connectionStatus = await ConnectionManager.verifyConnections();
      const savedStates = ConnectionManager.getConnectionStates();
      const needsRestoration = Object.entries(connectionStatus).some(([id, status]) => {
        const savedState = savedStates[id]?.state || "streaming";
        return savedState === "streaming" && (status.status === "not_found" || status.status === "error" || !status.is_streaming);
      });
      if (needsRestoration) {
        ConnectionManager.resetRestorationState();
        hasAutoRestoredRef.current = false;
        await performStartupRestore();
      }
    } catch (error) {
      console.error("Error verifying connections:", error);
    }
  }, [serverStatus, performStartupRestore]);

  const handleRestoreAdapter = useCallback(async () => {
    ConnectionManager.resetRestorationState();
    hasAutoRestoredRef.current = false;
    await performStartupRestore();
  }, [performStartupRestore]);

  const handleSaveConnection = useCallback(async () => {
    try {
      loadConnections();
      setIsModalOpen(false);
      setEditingConnection(null);
    } catch (error) {
      console.error("Error saving connection:", error);
    }
  }, [loadConnections]);

  const handleEditConnection = useCallback((connection) => {
    setEditingConnection(connection);
    setIsModalOpen(true);
  }, []);

  const handleTerminateConnection = useCallback(async (connectionId) => {
    const result = await disconnectConnection(connectionId);
    if (!result) return;
    ConnectionManager.removeConnection(connectionId);
    loadConnections();
  }, [loadConnections]);

  const handlePauseConnection = useCallback((connectionId) => {
    ConnectionManager.saveConnectionState(connectionId, "paused", "user");
  }, []);

  const handleResumeConnection = useCallback((connectionId) => {
    ConnectionManager.saveConnectionState(connectionId, "streaming", "user");
  }, []);

  const handleNewConnection = useCallback(() => setIsModalOpen(true), []);

  // Server health monitor
  useEffect(() => {
    const monitorServer = async () => {
      const isOnline = await checkServerHealth();
      const newStatus = isOnline ? "online" : "offline";
      if (newStatus !== serverStatus) {
        setServerStatus(newStatus);
        if (newStatus === "online" && serverStatus === "offline") {
          hasAutoRestoredRef.current = false;
          ConnectionManager.resetRestorationState();
        }
      }
    };
    monitorServer();
    const interval = setInterval(monitorServer, 5000);
    return () => clearInterval(interval);
  }, [checkServerHealth, serverStatus]);

  // Auto-restore when server comes online
  useEffect(() => {
    if (serverStatus === "online" && !hasAutoRestoredRef.current && !isRestoring) {
      const restoreTimer = setTimeout(() => performStartupRestore(), 1000);
      return () => clearTimeout(restoreTimer);
    }
  }, [serverStatus, isRestoring, performStartupRestore]);

  // Periodic verification every 30 seconds
  useEffect(() => {
    if (serverStatus === "online") {
      const interval = setInterval(verifyAndRestoreConnections, 30000);
      return () => clearInterval(interval);
    }
  }, [serverStatus, verifyAndRestoreConnections]);

  // Load connections on startup
  useEffect(() => { loadConnections(); }, [loadConnections]);

  // Debug helpers on window
  useEffect(() => {
    window.debugRestore = () => { hasAutoRestoredRef.current = false; performStartupRestore(); };
    window.debugStorage = () => ConnectionManager.debugStorage();
    window.resetRestore = () => { hasAutoRestoredRef.current = false; ConnectionManager.resetRestorationState(); };
    window.forceRestore = () => { ConnectionManager.forceRestore(); hasAutoRestoredRef.current = false; performStartupRestore(); };
    window.verifyConnections = async () => { const s = await ConnectionManager.verifyConnections(); console.log("Verification:", s); return s; };
    window.checkStates = () => { const states = ConnectionManager.getConnectionStates(); console.log("Saved states:", states); };
  }, [performStartupRestore]);

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
          {isRestoring && " (Restoring...)"}
        </div>
      </div>

      <main>
        <ConnectionList
          connections={connections}
          onNewConnection={handleNewConnection}
          onEditConnection={handleEditConnection}
          onTerminateConnection={handleTerminateConnection}
          onRestoreConnections={handleRestoreAdapter}
          onPauseConnection={handlePauseConnection}
          onResumeConnection={handleResumeConnection}
          isRestoring={isRestoring}
        />
      </main>

      {isModalOpen && (
        <ConnectionModal
          isOpen={isModalOpen}
          onClose={() => { setIsModalOpen(false); setEditingConnection(null); }}
          onSave={handleSaveConnection}
          initialData={editingConnection || {}}
        />
      )}
    </div>
  );
}

export default App;
