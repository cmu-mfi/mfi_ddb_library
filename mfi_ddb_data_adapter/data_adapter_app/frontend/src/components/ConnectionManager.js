import { connectConnection, disconnectConnection, fetchStreamingStatus, pauseConnection } from "../api.js";

export class ConnectionManager {
  static STORAGE_KEY = "adapter_connections";
  static STATE_KEY = "adapter_states";
  static SESSION_KEY = "session_info";
  static updatingConnections = new Set();

  static isUpdatingConnection(connectionId) {
    return this.updatingConnections.has(String(connectionId));
  }

  static async connectWithState(connectionId, config, state) {
    const response = await connectConnection(
      connectionId,
      config.adapter,
      config.adapterConfig,
      config.streamerConfig,
      config.isPolling !== false,
      config.pollingRateHz ?? 1
    );
    const result = await response.json();
    if (!response.ok || !result.is_connected || !result.is_streaming) {
      const detail = typeof result.detail === "string" ? result.detail : null;
      throw new Error(detail || `Connection did not start (HTTP ${response.status}).`);
    }
    if (state === "paused" && !(await pauseConnection(connectionId))) {
      throw new Error("Could not pause the updated connection.");
    }
  }

  static async updateConnection(connectionId, updated) {
    const previous = this.getSavedConnections()[connectionId];
    if (!previous) throw new Error("The saved connection could not be found.");
    if (this.isUpdatingConnection(connectionId)) {
      throw new Error("This connection is already being updated.");
    }

    this.updatingConnections.add(String(connectionId));
    let previousStatus = null;
    let disconnected = false;
    let replacementAttempted = false;
    try {
      try {
        previousStatus = await fetchStreamingStatus(connectionId);
      } catch (error) {
        if (error.status !== 404) throw error;
      }

      const targetState = this.getConnectionState(connectionId).state;
      if (targetState === "paused" && updated.isPolling === false) {
        throw new Error("Resume this connection before switching to Callback mode.");
      }

      // The connect endpoint reuses existing IDs, so remove the old instance
      // before sending new settings. Keep intentionally inactive adapters idle.
      if (previousStatus) {
        if (!(await disconnectConnection(connectionId))) {
          throw new Error("Could not disconnect the existing connection. Settings were not saved.");
        }
        disconnected = true;
      }
      if (targetState !== "stopped" && (previousStatus || targetState === "streaming")) {
        replacementAttempted = true;
        await this.connectWithState(connectionId, updated, targetState);
      }

      this.saveConnection(connectionId, updated);
      return updated;
    } catch (error) {
      if (disconnected || replacementAttempted) {
        try {
          // A failed or lost connect response may still have created an instance.
          if (replacementAttempted && !(await disconnectConnection(connectionId))) {
            throw new Error("Could not remove the replacement connection.");
          }
          if (previousStatus) {
            await this.connectWithState(
              connectionId,
              previous,
              previousStatus.is_streaming ? "streaming" : "paused"
            );
          }
        } catch (restoreError) {
          throw new Error(`${error.message} Previous settings remain saved, but restoring the connection failed: ${restoreError.message}`);
        }
        const recovery = previousStatus
          ? "The previous connection was restored."
          : "The saved settings were not changed.";
        throw new Error(`${error.message} ${recovery}`);
      }
      throw error;
    } finally {
      this.updatingConnections.delete(String(connectionId));
    }
  }

  static saveConnection(connectionId, connectionData) {
    const connections = this.getSavedConnections();
    connections[connectionId] = {
      id: connectionId,
      adapter: connectionData.adapter,
      adapterConfig: connectionData.adapterConfig,
      streamerConfig: connectionData.streamerConfig || "",
      isPolling: connectionData.isPolling !== false, // default true unless explicitly false
      pollingRateHz: connectionData.pollingRateHz ?? 1,
      saved_at: connectionData.savedAt || new Date().toISOString(),
      updated_at: connectionData.updatedAt || null,
    };
    localStorage.setItem(this.STORAGE_KEY, JSON.stringify(connections));
    console.log(`Saved connection data for ${connectionId}:`, connections[connectionId]);
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

  static saveConnectionState(connectionId, state, source = "user") {
    const states = this.getConnectionStates();
    if (source === "user") {
      states[connectionId] = {
        state,
        updated_at: new Date().toISOString(),
        source,
      };
      localStorage.setItem(this.STATE_KEY, JSON.stringify(states));
      console.log(`Saved state for ${connectionId}: ${state} (source: ${source})`);
    } else {
      console.log(`IGNORED state change for ${connectionId}: ${state} (source: ${source}) - only user actions are saved`);
    }
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

  static shouldRestoreOnStartup() {
    console.log("DEBUG: Checking shouldRestoreOnStartup conditions...");
    const connections = this.getSavedConnections();
    const hasConnections = Object.keys(connections).length > 0;
    console.log(`DEBUG: Has connections: ${hasConnections} (${Object.keys(connections).length} connections)`);

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
        console.log("DEBUG: Never restored in this session, allowing restoration");
        return true;
      }

      if (session.completedAt) {
        const lastRestore = new Date(session.completedAt);
        const now = new Date();
        const minutesSinceRestore = (now - lastRestore) / (1000 * 60);

        if (minutesSinceRestore > 5) {
          console.log(`DEBUG: Last restoration was ${minutesSinceRestore.toFixed(1)} minutes ago, allowing re-restoration`);
          return true;
        } else {
          console.log(`DEBUG: Last restoration was only ${minutesSinceRestore.toFixed(1)} minutes ago, skipping`);
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
      JSON.stringify({ startTime: new Date().toISOString(), restored: false })
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
    console.log("DEBUG: Restoration state reset - next check will trigger restoration");
  }

  static async verifyConnections() {
    const connections = this.getSavedConnections();
    const results = Object.fromEntries(
      await Promise.all(
        Object.entries(connections)
          .filter(([id]) => !this.isUpdatingConnection(id))
          .map(async ([id]) => [id, await fetchStreamingStatus(id)])
      )
    );
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
      console.log(`  Adapter: ${conn.adapter}`);
      console.log(`  State: ${state?.state || "streaming"} (updated: ${state?.updated_at || "never"}, source: ${state?.source || "unknown"})`);
    });
    console.log("Should restore on startup:", this.shouldRestoreOnStartup());
    console.log("=== END DEBUG ===");
  }

  static forceRestore() {
    console.log("DEBUG: Forcing restoration by clearing session");
    sessionStorage.removeItem(this.SESSION_KEY);
    return this.shouldRestoreOnStartup();
  }
}
