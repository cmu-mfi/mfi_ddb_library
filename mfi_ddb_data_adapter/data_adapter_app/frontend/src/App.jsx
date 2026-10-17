import { useState, useEffect, useRef, useCallback } from "react";
import ConnectionList from "./components/ConnectionList";
import ConnectionModal from "./components/ConnectionModal";
import { ConnectionManager } from "./components/ConnectionManager";
import logoMfi from "./images/logo_mfi.png";
import { disconnectConnection, fetchServerHealth } from "./api";

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
  const restorationInProgressRef = useRef(false);
  const verificationInProgressRef = useRef(false);
  const lifetimeRef = useRef(null);

  useEffect(() => {
    const controller = new AbortController();
    lifetimeRef.current = controller;
    return () => controller.abort();
  }, []);

  const checkServerHealth = useCallback(async (signal) => {
    try {
      return await fetchServerHealth({ signal });
    } catch (error) {
      if (signal?.aborted) throw error;
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
    const signal = lifetimeRef.current?.signal;
    if (signal?.aborted) return;
    if (hasAutoRestoredRef.current || restorationInProgressRef.current) return;
    if (!ConnectionManager.shouldRestoreOnStartup()) return;
    restorationInProgressRef.current = true;
    setIsRestoring(true);
    try {
      const connectionIds = Object.keys(ConnectionManager.getSavedConnections());
      let successCount = 0;
      let errorCount = 0;
      for (const connectionId of connectionIds) {
        if (signal?.aborted) return;
        try {
          const result = await ConnectionManager.restoreConnection(connectionId, { signal });
          if (result !== "skipped") successCount++;
        } catch (error) {
          if (signal?.aborted) return;
          console.error(`Error restoring ${connectionId}:`, error);
          errorCount++;
        }
      }

      if (signal?.aborted) return;
      console.log(`Restore complete — success: ${successCount}, error: ${errorCount}`);
      hasAutoRestoredRef.current = true;
      ConnectionManager.markRestorationComplete();
      loadConnections();
    } finally {
      restorationInProgressRef.current = false;
      if (!signal?.aborted) setIsRestoring(false);
    }
  }, [loadConnections]);

  const verifyAndRestoreConnections = useCallback(async ({ signal = lifetimeRef.current?.signal } = {}) => {
    if (serverStatus !== "online" || signal?.aborted || verificationInProgressRef.current) return;
    verificationInProgressRef.current = true;
    try {
      const connectionStatus = await ConnectionManager.verifyConnections({ signal });
      if (signal?.aborted) return;
      const needsRestoration = Object.entries(connectionStatus).some(([id, status]) => {
        if (ConnectionManager.isUpdatingConnection(id)) return false;
        const savedState = ConnectionManager.getConnectionState(id).state;
        return savedState === "streaming" && status.status !== "error" &&
          (status.status === "not_found" || !status.is_connected || !status.is_streaming);
      });
      if (needsRestoration) {
        ConnectionManager.resetRestorationState();
        hasAutoRestoredRef.current = false;
        await performStartupRestore();
      }
    } catch (error) {
      if (!signal?.aborted) console.error("Error verifying connections:", error);
    } finally {
      verificationInProgressRef.current = false;
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
    if (!result) throw new Error("Could not remove the connection. Please retry.");
    ConnectionManager.removeConnection(connectionId);
    if (!lifetimeRef.current?.signal.aborted) loadConnections();
  }, [loadConnections]);

  const handleNewConnection = useCallback(() => setIsModalOpen(true), []);

  // Server health monitor
  useEffect(() => {
    const controller = new AbortController();
    let pending = false;
    const monitorServer = async () => {
      if (pending || controller.signal.aborted) return;
      pending = true;
      try {
        const isOnline = await checkServerHealth(controller.signal);
        if (controller.signal.aborted) return;
        const newStatus = isOnline ? "online" : "offline";
        if (newStatus !== serverStatus) {
          setServerStatus(newStatus);
          if (newStatus === "online" && serverStatus === "offline") {
            hasAutoRestoredRef.current = false;
            ConnectionManager.resetRestorationState();
          }
        }
      } catch (error) {
        if (!controller.signal.aborted) console.error("Health check failed:", error);
      } finally {
        pending = false;
      }
    };
    monitorServer();
    const interval = setInterval(monitorServer, 5000);
    return () => {
      clearInterval(interval);
      controller.abort();
    };
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
      const controller = new AbortController();
      const interval = setInterval(() => verifyAndRestoreConnections({ signal: controller.signal }), 30000);
      return () => {
        clearInterval(interval);
        controller.abort();
      };
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
