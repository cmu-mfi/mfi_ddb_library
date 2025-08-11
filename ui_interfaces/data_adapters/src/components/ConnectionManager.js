// ConnectionManager.js - Adapter-agnostic, no protocol maps, preserves exact YAML
export class ConnectionManager {
  static STORAGE_KEY = "adapter_connections";
  static STATE_KEY = "adapter_states";
  static SESSION_KEY = "session_info";

  // Save complete connection data
  static saveConnection(connectionId, connectionData) {
    const connections = this.getSavedConnections();

    connections[connectionId] = {
      id: connectionId,
      name: connectionData.name || connectionData.type,
      type: connectionData.type, // display label only
      topicFamily: connectionData.topicFamily, // kv | blob | historian
      configuration: connectionData.configuration, // inner text shown in UI
      mqttConfig: connectionData.mqttConfig || "",
      protocol: connectionData.protocol || null, // not used anymore
      yaml: connectionData.yaml || null, // exact YAML posted to backend
      adapterKey: connectionData.adapterKey || null, // backend-detected adapter key
      saved_at: connectionData.savedAt || new Date().toISOString(),
      updated_at: connectionData.updatedAt || null,
    };

    localStorage.setItem(this.STORAGE_KEY, JSON.stringify(connections));
    console.log(
      `Saved connection data for ${connectionId}:`,
      connections[connectionId]
    );
  }

  static getSavedConnections() {
    try {
      const data = localStorage.getItem(this.STORAGE_KEY);
      return data ? JSON.parse(data) : {};
    } catch (error) {
      console.error("Error loading saved connections:", error);
      return {};
    }
  }

  // Only save state when explicitly called by user actions
  static saveConnectionState(connectionId, state, source = "user") {
    const states = this.getConnectionStates();

    if (source === "user") {
      states[connectionId] = {
        state, // "streaming" | "paused"
        updated_at: new Date().toISOString(),
        source,
      };
      localStorage.setItem(this.STATE_KEY, JSON.stringify(states));
      console.log(
        `Saved state for ${connectionId}: ${state} (source: ${source})`
      );
    } else {
      console.log(
        `IGNORED state change for ${connectionId}: ${state} (source: ${source}) - only user actions are saved`
      );
    }
  }

  // Don't auto-save paused state on connection failures
  static markConnectionAsFailedTemporarily(connectionId, reason) {
    console.log(`Connection ${connectionId} temporarily failed: ${reason}`);
    console.log("State will remain as saved in localStorage, not auto-paused");
  }

  static getConnectionState(connectionId) {
    const states = this.getConnectionStates();
    return states[connectionId] || { state: "streaming", source: "default" };
  }

  static getConnectionStates() {
    try {
      const data = localStorage.getItem(this.STATE_KEY);
      return data ? JSON.parse(data) : {};
    } catch (error) {
      console.error("Error loading connection states:", error);
      return {};
    }
  }

  static removeConnection(connectionId) {
    const connections = this.getSavedConnections();
    const states = this.getConnectionStates();

    delete connections[connectionId];
    delete states[connectionId];

    localStorage.setItem(this.STORAGE_KEY, JSON.stringify(connections));
    localStorage.setItem(this.STATE_KEY, JSON.stringify(states));
  }

  // Restoration policy for the current session
  static shouldRestoreOnStartup() {
    console.log("DEBUG: Checking shouldRestoreOnStartup conditions...");

    const connections = this.getSavedConnections();
    const hasConnections = Object.keys(connections).length > 0;

    console.log(
      `DEBUG: Has connections: ${hasConnections} (${
        Object.keys(connections).length
      } connections)`
    );

    if (!hasConnections) {
      console.log("DEBUG: No connections to restore");
      return false;
    }

    const sessionInfo = sessionStorage.getItem(this.SESSION_KEY);
    console.log("DEBUG: Session info:", sessionInfo);

    if (!sessionInfo) {
      console.log("DEBUG: New session detected - creating session info");
      this._createNewSession();
      return true;
    }

    try {
      const session = JSON.parse(sessionInfo);
      console.log("DEBUG: Parsed session:", session);

      if (!session.restored) {
        console.log(
          "DEBUG: Never restored in this session, allowing restoration"
        );
        return true;
      }

      if (session.completedAt) {
        const lastRestore = new Date(session.completedAt);
        const now = new Date();
        const minutesSinceRestore = (now - lastRestore) / (1000 * 60);

        if (minutesSinceRestore > 5) {
          console.log(
            `DEBUG: Last restoration was ${minutesSinceRestore.toFixed(
              1
            )} minutes ago, allowing re-restoration`
          );
          return true;
        } else {
          console.log(
            `DEBUG: Last restoration was only ${minutesSinceRestore.toFixed(
              1
            )} minutes ago, skipping`
          );
        }
      }

      console.log("DEBUG: Should restore based on session.restored: false");
      return false;
    } catch (e) {
      console.error("DEBUG: Failed to parse session info:", e);
      this._createNewSession();
      return true;
    }
  }

  static _createNewSession() {
    sessionStorage.setItem(
      this.SESSION_KEY,
      JSON.stringify({
        startTime: new Date().toISOString(),
        restored: false,
      })
    );
  }

  static markRestorationComplete() {
    console.log("DEBUG: Marking restoration as complete");

    try {
      const sessionInfo = sessionStorage.getItem(this.SESSION_KEY);
      const session = sessionInfo ? JSON.parse(sessionInfo) : {};

      session.restored = true;
      session.completedAt = new Date().toISOString();

      sessionStorage.setItem(this.SESSION_KEY, JSON.stringify(session));
      console.log("DEBUG: Restoration marked complete:", session);
    } catch (e) {
      console.error("DEBUG: Failed to mark restoration complete:", e);
    }
  }

  static resetRestorationState() {
    sessionStorage.removeItem(this.SESSION_KEY);
    console.log(
      "DEBUG: Restoration state reset - next check will trigger restoration"
    );
  }

  static async verifyConnections() {
    const connections = this.getSavedConnections();
    const results = {};

    for (const [id, conn] of Object.entries(connections)) {
      try {
        const response = await fetch(
          `${
            process.env.REACT_APP_API_URL || "http://localhost:8000"
          }/config/streaming-status/${id}`
        );
        if (response.ok) {
          const status = await response.json();
          results[id] = {
            name: conn.name,
            connected: status.adapter_connected,
            streaming: status.is_streaming,
            status: status.status,
          };
        } else {
          results[id] = {
            name: conn.name,
            connected: false,
            streaming: false,
            status: "not_found",
          };
        }
      } catch (error) {
        results[id] = {
          name: conn.name,
          connected: false,
          streaming: false,
          status: "error",
          error: error.message,
        };
      }
    }

    return results;
  }

  static clearAll() {
    localStorage.removeItem(this.STORAGE_KEY);
    localStorage.removeItem(this.STATE_KEY);
    sessionStorage.removeItem(this.SESSION_KEY);
  }

  static debugStorage() {
    console.log("=== LOCALSTORAGE DEBUG ===");

    const connections = this.getSavedConnections();
    const states = this.getConnectionStates();
    const sessionInfo = sessionStorage.getItem(this.SESSION_KEY);

    console.log("Saved connections:", connections);
    console.log("Connection states:", states);
    console.log("Session info raw:", sessionInfo);

    try {
      const parsedSession = sessionInfo ? JSON.parse(sessionInfo) : null;
      console.log("Session info parsed:", parsedSession);
    } catch (e) {
      console.log("Session info parse error:", e);
    }

    Object.keys(connections).forEach((id) => {
      const conn = connections[id];
      const state = states[id];
      console.log(`\nConnection ${id}:`);
      console.log(`  Name: ${conn.name}`);
      console.log(`  Type: ${conn.type}`);
      console.log(
        `  State: ${state?.state || "streaming"} (updated: ${
          state?.updated_at || "never"
        }, source: ${state?.source || "unknown"})`
      );

      const config = this.getYamlConfig(id);
      console.log(`  Has YAML config: ${!!config}`);
      if (config) console.log(`  YAML length: ${config.length} chars`);
    });

    console.log("Should restore on startup:", this.shouldRestoreOnStartup());
    console.log("=== END DEBUG ===");
  }

  static forceRestore() {
    console.log("DEBUG: Forcing restoration by clearing session");
    sessionStorage.removeItem(this.SESSION_KEY);
    return this.shouldRestoreOnStartup();
  }

  // Get YAML config for restoration (adapter-agnostic)
  static getYamlConfig(connectionId) {
    console.log(`DEBUG: Getting YAML config for ${connectionId}`);

    const connections = this.getSavedConnections();
    const connection = connections[connectionId];

    if (!connection) {
      console.error(`DEBUG: No connection found for ${connectionId}`);
      console.log(`DEBUG: Available connections:`, Object.keys(connections));
      return null;
    }

    // 1) Preferred: exact YAML saved at connect time
    if (connection.yaml && connection.yaml.trim()) {
      return connection.yaml.trim();
    }

    console.log(
      "DEBUG: No saved 'yaml', building from fields (legacy fallback).",
      {
        id: connection.id,
        name: connection.name,
        type: connection.type,
        topicFamily: connection.topicFamily,
        hasConfiguration: !!connection.configuration,
        hasMqttConfig: !!connection.mqttConfig,
        adapterKey: connection.adapterKey || null,
      }
    );

    // 2) Adapter-agnostic fallback using backend-provided adapterKey, if present
    let inner = (connection.configuration || "").trim();
    const mqttPart =
      connection.mqttConfig && connection.mqttConfig.trim()
        ? `\n\n${connection.mqttConfig.trim()}`
        : "";

    const key = (connection.adapterKey || "").trim();
    if (key && inner && !inner.startsWith(key + ":")) {
      inner = `${key}:\n` + inner.replace(/^/gm, "  ");
    }

    if (!inner) {
      console.warn(
        `DEBUG: No configuration text to build YAML for ${connectionId}.`
      );
      return null;
    }

    const combined = `topic_family: ${connection.topicFamily}\n\n${inner}${mqttPart}`;
    console.log(
      "DEBUG: Generated YAML config (first 200 chars):",
      combined.substring(0, 200)
    );
    return combined;
  }
}
