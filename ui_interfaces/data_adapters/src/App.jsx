import { useState, useEffect, useRef, useCallback } from "react";
import ConnectionList from "./components/ConnectionList";
import ConnectionModal from "./components/ConnectionModal";
import { ConnectionManager } from "./components/ConnectionManager";
import logoMfi from "./images/logo_mfi.png";
import "./App.css";

const API_BASE_URL = process.env.REACT_APP_API_URL || "http://localhost:8000";

function App() {
  const [connections, setConnections] = useState([]);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [editingConnection, setEditingConnection] = useState(null);
  const [serverStatus, setServerStatus] = useState("checking");
  const [isRestoring, setIsRestoring] = useState(false);
  const hasAutoRestoredRef = useRef(false);

  // Check server health
  const checkServerHealth = useCallback(async () => {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 3000);

      const response = await fetch(`${API_BASE_URL}/config/health`, {
        signal: controller.signal,
      });

      clearTimeout(timeoutId);
      return response.ok;
    } catch (error) {
      console.error("Health check failed:", error);
      return false;
    }
  }, []);

  // Load connections for UI
  const loadConnections = useCallback(() => {
    const savedConnections = ConnectionManager.getSavedConnections();
    const connectionsList = Object.values(savedConnections).map((conn) => ({
      id: conn.id,
      name: conn.name,
      type: conn.type,
      topicFamily: conn.topicFamily,
      configuration: conn.configuration,
      mqttConfig: conn.mqttConfig,
    }));
    setConnections(connectionsList);
    console.log(`DEBUG: Loaded ${connectionsList.length} connections for UI`);
  }, []);

  // FIXED: Don't auto-pause connections on restoration failures
  const performStartupRestore = useCallback(async () => {
    console.log("=== RESTORATION DEBUG START ===");
    console.log("hasAutoRestoredRef.current:", hasAutoRestoredRef.current);
    console.log("isRestoring:", isRestoring);
    console.log("serverStatus:", serverStatus);

    if (hasAutoRestoredRef.current) {
      console.log("DEBUG: Auto-restore already performed, skipping...");
      return;
    }

    const shouldRestore = ConnectionManager.shouldRestoreOnStartup();
    console.log("DEBUG: Should restore on startup:", shouldRestore);

    if (!shouldRestore) {
      console.log("No restoration needed");
      return;
    }

    console.log("=== STARTUP RESTORE BEGINNING ===");
    setIsRestoring(true);

    const savedConnections = ConnectionManager.getSavedConnections();
    const savedStates = ConnectionManager.getConnectionStates();

    console.log("DEBUG: Saved connections:", savedConnections);
    console.log("DEBUG: Saved states:", savedStates);

    const connectionIds = Object.keys(savedConnections);
    console.log(
      `Found ${connectionIds.length} saved connections:`,
      connectionIds
    );

    if (connectionIds.length === 0) {
      console.log("No saved connections to restore");
      setIsRestoring(false);
      return;
    }

    let successCount = 0;
    let errorCount = 0;

    for (const connectionId of connectionIds) {
      const connectionData = savedConnections[connectionId];
      const savedState = savedStates[connectionId];
      const targetState = savedState?.state || "streaming"; // Default to streaming if no saved state

      console.log(`\n--- Processing connection ${connectionId} ---`);
      console.log(`Name: ${connectionData.name}`);
      console.log(`Type: ${connectionData.type}`);
      console.log(
        `Target state: ${targetState} (source: ${
          savedState?.source || "default"
        })`
      );

      try {
        const yamlConfig = ConnectionManager.getYamlConfig(connectionId);
        if (!yamlConfig) {
          console.error(`ERROR: No YAML config found for ${connectionId}`);
          errorCount++;
          continue;
        }

        console.log(
          `DEBUG: YAML config length: ${yamlConfig.length} characters`
        );
        console.log(
          `DEBUG: YAML config preview:`,
          yamlConfig.substring(0, 200)
        );

        // ONLY restore streaming connections automatically
        if (targetState === "streaming") {
          console.log(
            `DEBUG: Auto-restoring STREAMING connection ${connectionId}...`
          );

          const formData = new FormData();
          formData.append("text", yamlConfig);

          console.log(
            `DEBUG: Calling connect endpoint: ${API_BASE_URL}/config/connect/${connectionId}`
          );

          const connectResponse = await fetch(
            `${API_BASE_URL}/config/connect/${connectionId}`,
            {
              method: "POST",
              body: formData,
            }
          );

          console.log(
            `DEBUG: Connect response status: ${connectResponse.status}`
          );

          if (!connectResponse.ok) {
            const errorText = await connectResponse.text();
            console.error(
              `ERROR: Failed to connect ${connectionId}:`,
              errorText
            );

            // FIXED: Don't auto-pause on connection failures
            // Just mark as temporarily failed, keep original state
            ConnectionManager.markConnectionAsFailedTemporarily(
              connectionId,
              `Connection failed during restoration: ${errorText}`
            );

            // If it's a broker connection error, still count as "attempted" for UI purposes
            if (
              errorText.includes("broker unreachable") ||
              errorText.includes("MQTT") ||
              errorText.includes("Configuration error")
            ) {
              console.log(
                `WARNING: Connection ${connectionId} failed due to broker/config error - will show in UI but remain as '${targetState}' in localStorage`
              );
              successCount++; // Count as success for restoration completion
            } else {
              errorCount++;
            }
            continue;
          } else {
            const responseData = await connectResponse.json();
            console.log(
              `SUCCESS: Successfully auto-restored streaming connection ${connectionId}:`,
              responseData
            );
            successCount++;
          }
        } else if (targetState === "paused") {
          console.log(
            `DEBUG: Skipping PAUSED connection ${connectionId} - will remain paused until manually resumed`
          );

          // For paused connections, we DON'T restore them to the backend
          // They stay paused and will only be restored when user clicks Resume
          successCount++;
        } else {
          console.log(
            `DEBUG: Unknown target state '${targetState}' for ${connectionId}, defaulting to streaming`
          );

          // Unknown state, treat as streaming
          const formData = new FormData();
          formData.append("text", yamlConfig);

          const connectResponse = await fetch(
            `${API_BASE_URL}/config/connect/${connectionId}`,
            {
              method: "POST",
              body: formData,
            }
          );

          if (connectResponse.ok) {
            console.log(
              `SUCCESS: Successfully restored connection ${connectionId} (unknown state -> streaming)`
            );
            successCount++;
          } else {
            console.error(
              `ERROR: Failed to restore connection ${connectionId}`
            );
            // Don't auto-pause, just mark as failed
            ConnectionManager.markConnectionAsFailedTemporarily(
              connectionId,
              "Failed to restore connection with unknown state"
            );
            errorCount++;
          }
        }
      } catch (error) {
        console.error(`ERROR: Exception restoring ${connectionId}:`, error);
        // Don't auto-pause on exceptions
        ConnectionManager.markConnectionAsFailedTemporarily(
          connectionId,
          `Exception during restoration: ${error.message}`
        );
        errorCount++;
      }
    }

    console.log(`\n=== RESTORE COMPLETE ===`);
    console.log(`SUCCESS: ${successCount}`);
    console.log(`ERROR: ${errorCount}`);
    console.log(
      `INFO: Only streaming connections were auto-restored. Paused connections remain paused until manually resumed.`
    );
    console.log(
      `INFO: Failed connections keep their original state - no auto-pausing!`
    );

    hasAutoRestoredRef.current = true;
    ConnectionManager.markRestorationComplete();
    setIsRestoring(false);

    // Refresh connections list
    loadConnections();
  }, [loadConnections]);

  // Verify and restore connections periodically (permanent fix for server restarts)
  const verifyAndRestoreConnections = useCallback(async () => {
    if (serverStatus !== "online") return;

    try {
      const connectionStatus = await ConnectionManager.verifyConnections();
      const savedStates = ConnectionManager.getConnectionStates();

      // Only restore connections that SHOULD be streaming but aren't found on backend
      const needsRestoration = Object.entries(connectionStatus).some(
        ([id, status]) => {
          const savedState = savedStates[id]?.state || "streaming";
          return (
            savedState === "streaming" &&
            (status.status === "not_found" || status.status === "error")
          );
        }
      );

      if (needsRestoration) {
        console.log(
          "DEBUG: Detected missing streaming connections, triggering restoration..."
        );
        ConnectionManager.resetRestorationState();
        hasAutoRestoredRef.current = false;
        await performStartupRestore();
      }
    } catch (error) {
      console.error("Error verifying connections:", error);
    }
  }, [serverStatus, performStartupRestore]);

  // Monitor server status
  useEffect(() => {
    const monitorServer = async () => {
      const isOnline = await checkServerHealth();
      const newStatus = isOnline ? "online" : "offline";

      if (newStatus !== serverStatus) {
        console.log(
          `DEBUG: Server status changed: ${serverStatus} -> ${newStatus}`
        );
        setServerStatus(newStatus);

        // Reset restoration flag when server comes back online
        if (newStatus === "online" && serverStatus === "offline") {
          console.log(
            "DEBUG: Server came back online, resetting restoration flag"
          );
          hasAutoRestoredRef.current = false;
          ConnectionManager.resetRestorationState();
        }
      }
    };

    monitorServer();
    const interval = setInterval(monitorServer, 5000);
    return () => clearInterval(interval);
  }, [checkServerHealth, serverStatus]);

  // Auto-restoration when server comes online
  useEffect(() => {
    console.log("DEBUG: Checking auto-restoration conditions:");
    console.log("- serverStatus:", serverStatus);
    console.log("- hasAutoRestoredRef.current:", hasAutoRestoredRef.current);
    console.log("- isRestoring:", isRestoring);

    if (
      serverStatus === "online" &&
      !hasAutoRestoredRef.current &&
      !isRestoring
    ) {
      console.log(
        "DEBUG: Server is online, triggering state-aware restoration..."
      );
      const restoreTimer = setTimeout(() => {
        performStartupRestore();
      }, 1000);

      return () => clearTimeout(restoreTimer);
    } else {
      console.log("DEBUG: Skipping auto-restoration (conditions not met)");
    }
  }, [serverStatus, isRestoring, performStartupRestore]);

  // Periodic verification (every 30 seconds) - permanent fix for missed restorations
  useEffect(() => {
    if (serverStatus === "online") {
      const interval = setInterval(verifyAndRestoreConnections, 30000);
      return () => clearInterval(interval);
    }
  }, [serverStatus, verifyAndRestoreConnections]);

  // Load connections on startup
  useEffect(() => {
    console.log("DEBUG: Loading connections on startup...");
    loadConnections();
  }, [loadConnections]);

  // Enhanced manual restore function with permanent fix logic
  const handleRestoreAdapter = useCallback(async () => {
    console.log("DEBUG: Manual restore triggered");

    // Reset restoration state to allow fresh restoration
    ConnectionManager.resetRestorationState();
    hasAutoRestoredRef.current = false;

    await performStartupRestore();
  }, [performStartupRestore]);

  const handleSaveConnection = useCallback(
    async (connectionData) => {
      try {
        loadConnections();
        setIsModalOpen(false);
        setEditingConnection(null);
      } catch (error) {
        console.error("Error saving connection:", error);
      }
    },
    [loadConnections]
  );

  const handleEditConnection = useCallback((connection) => {
    setEditingConnection(connection);
    setIsModalOpen(true);
  }, []);

  const handleTerminateConnection = useCallback(
    async (connectionId) => {
      try {
        // Disconnect from backend
        await fetch(`${API_BASE_URL}/config/disconnect/${connectionId}`, {
          method: "POST",
        });
      } catch (error) {
        console.error("Failed to disconnect from backend:", error);
      }

      // Remove from localStorage
      ConnectionManager.removeConnection(connectionId);
      loadConnections();
    },
    [loadConnections]
  );

  // FIXED: Only save pause state when user explicitly pauses
  const handlePauseConnection = useCallback((connectionId) => {
    console.log(`DEBUG: User paused connection ${connectionId}`);
    ConnectionManager.saveConnectionState(connectionId, "paused", "user");
  }, []);

  // FIXED: Only save streaming state when user explicitly resumes
  const handleResumeConnection = useCallback((connectionId) => {
    console.log(`DEBUG: User resumed connection ${connectionId}`);
    ConnectionManager.saveConnectionState(connectionId, "streaming", "user");
  }, []);

  const handleNewConnection = useCallback(() => {
    setIsModalOpen(true);
  }, []);

  // DEBUG: Add window functions for manual testing and debugging
  useEffect(() => {
    window.debugRestore = () => {
      console.log("DEBUG: Manual debug restore triggered");
      hasAutoRestoredRef.current = false;
      performStartupRestore();
    };

    window.debugStorage = () => {
      ConnectionManager.debugStorage();
    };

    window.resetRestore = () => {
      console.log("DEBUG: Resetting restore flag");
      hasAutoRestoredRef.current = false;
      ConnectionManager.resetRestorationState();
    };

    window.forceRestore = () => {
      console.log("DEBUG: Force restore triggered");
      ConnectionManager.forceRestore();
      hasAutoRestoredRef.current = false;
      performStartupRestore();
    };

    window.verifyConnections = async () => {
      const status = await ConnectionManager.verifyConnections();
      console.log("Connection verification results:", status);
      return status;
    };

    window.checkStates = () => {
      const states = ConnectionManager.getConnectionStates();
      console.log("Current saved states:", states);
      Object.entries(states).forEach(([id, state]) => {
        console.log(
          `${id}: ${state.state} (source: ${state.source}, updated: ${state.updated_at})`
        );
      });
    };
  }, [performStartupRestore]);

  return (
    <div className="app">
      <div className="app-header">
        <div className="logo">
          <img
            src={logoMfi}
            className="app-logo"
            alt="Manufacturing Futures Institute Logo"
            width={200}
          />
        </div>
        <div className={`server-status ${serverStatus}`}>
          Server: {serverStatus}
          {isRestoring && " (Restoring...)"}
        </div>
      </div>
      <main className="app-main">
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
          onClose={() => {
            setIsModalOpen(false);
            setEditingConnection(null);
          }}
          onSave={handleSaveConnection}
          initialData={editingConnection || {}}
        />
      )}
    </div>
  );
}

export default App;
