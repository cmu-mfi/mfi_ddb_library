// ConnectionManager.js - Fixed to prevent auto-pausing on connection failures
export class ConnectionManager {
  static STORAGE_KEY = "adapter_connections";
  static STATE_KEY = "adapter_states";
  static SESSION_KEY = "session_info";

  // Save complete connection data in correct format
  static saveConnection(connectionId, connectionData) {
    const connections = this.getSavedConnections();

    // Store in format that restoration expects
    connections[connectionId] = {
      id: connectionId,
      name: connectionData.name || connectionData.type,
      type: connectionData.type,
      topicFamily: connectionData.topicFamily,
      configuration: connectionData.configuration, // Raw YAML config
      mqttConfig: connectionData.mqttConfig || "",
      protocol:
        connectionData.protocol ||
        this.getProtocolFromType(connectionData.type),
      saved_at: connectionData.savedAt || new Date().toISOString(),
      updated_at: connectionData.updatedAt || null,
    };

    localStorage.setItem(this.STORAGE_KEY, JSON.stringify(connections));
    console.log(
      `Saved connection data for ${connectionId}:`,
      connections[connectionId]
    );
  }

  // Add helper method:
  static getProtocolFromType(type) {
    const mapping = {
      ROS: "ros",
      "ROS Files": "ros_files",
      "Local Files": "file",
      MTConnect: "mtconnect",
      "MQTT-ADP": "mqtt_adp",
    };
    return mapping[type] || "unknown";
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

  // FIXED: Only save state when explicitly called by user actions
  static saveConnectionState(connectionId, state, source = "user") {
    const states = this.getConnectionStates();

    // Only save state changes that come from user actions, not system failures
    if (source === "user") {
      states[connectionId] = {
        state: state, // "streaming" or "paused"
        updated_at: new Date().toISOString(),
        source: source, // Track what caused the state change
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

  // FIXED: Don't auto-save paused state on connection failures
  static markConnectionAsFailedTemporarily(connectionId, reason) {
    // This is for logging only - don't save to localStorage
    console.log(`Connection ${connectionId} temporarily failed: ${reason}`);
    console.log(`State will remain as saved in localStorage, not auto-paused`);
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

  // PERMANENT FIX: More robust restoration logic
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

    // More lenient restoration logic:
    // 1. Always restore if no session info exists (new session)
    // 2. Restore if restoration was never completed successfully
    // 3. Allow restoration if it's been more than 5 minutes since last attempt

    const sessionInfo = sessionStorage.getItem(this.SESSION_KEY);
    console.log("DEBUG: Session info:", sessionInfo);

    if (!sessionInfo) {
      // New session - definitely restore
      console.log("DEBUG: New session detected - creating session info");
      this._createNewSession();
      return true;
    }

    try {
      const session = JSON.parse(sessionInfo);
      console.log("DEBUG: Parsed session:", session);

      // If never restored successfully, allow restoration
      if (!session.restored) {
        console.log(
          "DEBUG: Never restored in this session, allowing restoration"
        );
        return true;
      }

      // If last restoration was more than 5 minutes ago, allow re-restoration
      // (handles cases where server restarted after successful frontend restoration)
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

      const shouldRestore = !session.restored;
      console.log(
        `DEBUG: Should restore based on session.restored: ${shouldRestore}`
      );
      return false;
    } catch (e) {
      console.error("DEBUG: Failed to parse session info:", e);
      // If we can't parse session, assume we should restore
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

  // Add method to force restoration (useful for manual restore button)
  static resetRestorationState() {
    sessionStorage.removeItem(this.SESSION_KEY);
    console.log(
      "DEBUG: Restoration state reset - next check will trigger restoration"
    );
  }

  // Enhanced method to check if connections are actually working
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

  // Enhanced debugging method
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
      if (config) {
        console.log(`  YAML length: ${config.length} chars`);
      }
    });

    console.log("Should restore on startup:", this.shouldRestoreOnStartup());
    console.log("=== END DEBUG ===");
  }

  // Force restoration method for debugging
  static forceRestore() {
    console.log("DEBUG: Forcing restoration by clearing session");
    sessionStorage.removeItem(this.SESSION_KEY);
    return this.shouldRestoreOnStartup();
  }

  // Get YAML config for restoration (creates complete config from saved data)
  static getYamlConfig(connectionId) {
    console.log(`DEBUG: Getting YAML config for ${connectionId}`);

    const connections = this.getSavedConnections();
    const connection = connections[connectionId];

    if (!connection) {
      console.error(`DEBUG: No connection found for ${connectionId}`);
      console.log(`DEBUG: Available connections:`, Object.keys(connections));
      return null;
    }

    console.log(`DEBUG: Found connection:`, {
      id: connection.id,
      name: connection.name,
      type: connection.type,
      topicFamily: connection.topicFamily,
      hasConfiguration: !!connection.configuration,
      hasMqttConfig: !!connection.mqttConfig,
    });

    const protocolMapping = {
      ROS: "ros",
      "ROS Files": "ros_files",
      "Local Files": "file",
      MTConnect: "mtconnect",
      "MQTT-ADP": "mqtt_adp",
    };

    const protocolKey = protocolMapping[connection.type];
    let wrappedConfig = connection.configuration;

    console.log(
      `DEBUG: Protocol mapping: ${connection.type} -> ${protocolKey}`
    );

    // Wrap configuration with protocol key if needed
    if (
      protocolKey &&
      !connection.configuration.trim().startsWith(protocolKey + ":")
    ) {
      wrappedConfig = `${protocolKey}:\n${connection.configuration.replace(
        /^/gm,
        "  "
      )}`;
      console.log(`DEBUG: Wrapped config with protocol key`);
    }

    let combinedConfig = `topic_family: ${connection.topicFamily}\n\n${wrappedConfig}`;

    // Add MQTT config if present
    if (connection.mqttConfig && connection.mqttConfig.trim()) {
      combinedConfig += `\n\n${connection.mqttConfig}`;
      console.log(`DEBUG: Added MQTT config`);
    }

    console.log(
      `DEBUG: Generated YAML config (first 200 chars):`,
      combinedConfig.substring(0, 200)
    );
    return combinedConfig;
  }
}
