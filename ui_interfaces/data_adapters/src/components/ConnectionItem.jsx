import React, { useState, useEffect, useRef, useCallback } from "react";
import { ConnectionManager } from "./ConnectionManager";

const API_BASE_URL = process.env.REACT_APP_API_URL || "http://localhost:8000";

export default function ConnectionItem({
  connection,
  onEdit,
  onTerminate,
  onPause,
  onResume,
}) {
  const [streamingStatus, setStreamingStatus] = useState(null);
  const [isProcessing, setIsProcessing] = useState(false);
  const [localState, setLocalState] = useState("streaming");
  const [autoReconnectAttempted, setAutoReconnectAttempted] = useState(false);
  const statusCheckIntervalRef = useRef(null);

  // Load saved connection state from localStorage
  useEffect(() => {
    const savedState = ConnectionManager.getConnectionState(connection.id);
    const initialState = savedState?.state || "streaming";
    setLocalState(initialState);
    console.log(`Loaded initial state for ${connection.id}: ${initialState}`);
  }, [connection.id]);

  // FIXED: Helper to pause connection on backend - DON'T save to localStorage (system action)
  const pauseConnectionOnBackend = useCallback(async () => {
    try {
      const response = await fetch(
        `${API_BASE_URL}/config/pause/${connection.id}`,
        {
          method: "POST",
        }
      );
      if (response.ok) {
        setLocalState("paused");
        // FIXED: Don't save system-triggered pause to localStorage
        console.log(
          `System paused ${connection.id} on backend (not saved to localStorage)`
        );
      }
    } catch (error) {
      console.error(`Failed to pause ${connection.id} on backend:`, error);
    }
  }, [connection.id]);

  // Auto-reconnection logic
  const attemptAutoReconnect = useCallback(async () => {
    if (autoReconnectAttempted) return;
    setAutoReconnectAttempted(true);

    const savedState = ConnectionManager.getConnectionState(connection.id);
    const targetState = savedState?.state || "streaming";
    console.log(`Auto-reconnecting ${connection.id} to ${targetState} state`);

    try {
      const yamlConfig = ConnectionManager.getYamlConfig(connection.id);
      if (!yamlConfig) return;

      const formData = new FormData();
      formData.append("text", yamlConfig);

      const connectResponse = await fetch(
        `${API_BASE_URL}/config/connect/${connection.id}`,
        {
          method: "POST",
          body: formData,
        }
      );

      if (!connectResponse.ok) {
        console.error(`Failed to auto-reconnect ${connection.id}`);
        return;
      }

      if (targetState === "paused") {
        const pauseResponse = await fetch(
          `${API_BASE_URL}/config/pause/${connection.id}`,
          {
            method: "POST",
          }
        );

        if (pauseResponse.ok) {
          console.log(`Auto-paused ${connection.id} after reconnection`);
          setLocalState("paused");
          // FIXED: Don't save auto-pause to localStorage
        }
      } else {
        setLocalState("streaming");
      }
    } catch (error) {
      console.error(`Auto-reconnect failed for ${connection.id}:`, error);
    }
  }, [connection.id, autoReconnectAttempted]);

  // FIXED: Enhanced status checking - don't auto-save state changes
  const checkStatus = useCallback(async () => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000);

    try {
      const response = await fetch(
        `${API_BASE_URL}/config/streaming-status/${connection.id}`,
        {
          signal: controller.signal,
          headers: { "Content-Type": "application/json" },
        }
      );
      clearTimeout(timeoutId);

      if (!response.ok) {
        // If 404, connection doesn't exist on backend, try to restore it
        if (response.status === 404 && !autoReconnectAttempted) {
          console.log(
            `Connection ${connection.id} not found on backend, attempting auto-reconnect`
          );
          await attemptAutoReconnect();
          return;
        }
        throw new Error(`HTTP ${response.status}`);
      }

      const status = await response.json();
      setStreamingStatus(status);

      // Reset auto-reconnect flag on successful status check
      setAutoReconnectAttempted(false);

      // FIXED: Sync local state with backend state, but DON'T save to localStorage
      const savedState = ConnectionManager.getConnectionState(connection.id);
      const expectedState = savedState?.state || "streaming";

      if (status.is_paused && expectedState === "streaming") {
        // Backend says paused but user expects streaming - update local display only
        setLocalState("paused");
        console.log(
          `System detected backend pause for ${connection.id} (not saved to localStorage)`
        );
      } else if (
        !status.is_paused &&
        status.status === "active" &&
        expectedState === "paused"
      ) {
        // Backend is active but user wants paused - pause it
        await pauseConnectionOnBackend();
      } else if (status.is_paused) {
        setLocalState("paused");
      } else if (status.status === "active") {
        setLocalState("streaming");
      }
    } catch (error) {
      console.error(
        `Failed to check streaming status for ${connection.id}:`,
        error
      );
      setStreamingStatus({
        status: "inactive",
        adapter_connected: false,
        is_streaming: false,
        network_error: true,
        reason: "Network error or server unreachable",
      });
    }
  }, [
    connection.id,
    autoReconnectAttempted,
    attemptAutoReconnect,
    pauseConnectionOnBackend,
  ]);

  useEffect(() => {
    checkStatus();
    const intervalRef = setInterval(checkStatus, 5000);
    statusCheckIntervalRef.current = intervalRef;

    return () => {
      if (statusCheckIntervalRef.current) {
        clearInterval(statusCheckIntervalRef.current);
      }
    };
  }, [checkStatus]);

  // FIXED: Pause handler - ONLY user action saves to localStorage
  const handlePause = async (e) => {
    e.stopPropagation();
    setIsProcessing(true);

    try {
      const response = await fetch(
        `${API_BASE_URL}/config/pause/${connection.id}`,
        {
          method: "POST",
        }
      );

      if (response.ok) {
        setLocalState("paused");
        // FIXED: Only save user-triggered pause
        ConnectionManager.saveConnectionState(connection.id, "paused", "user");
        if (onPause) onPause(connection.id);
        console.log(`User successfully paused ${connection.id}`);
      } else {
        console.error(`Failed to pause connection ${connection.id}`);
      }
    } catch (error) {
      console.error(`Error pausing connection ${connection.id}:`, error);
    } finally {
      setIsProcessing(false);
    }
  };

  // FIXED: Resume handler - ONLY user action saves to localStorage
  const handleResume = async (e) => {
    e.stopPropagation();
    setIsProcessing(true);

    try {
      console.log(`User resuming paused connection ${connection.id}...`);

      // Get config from localStorage
      const yamlConfig = ConnectionManager.getYamlConfig(connection.id);
      if (!yamlConfig) {
        throw new Error("No configuration found for this connection");
      }

      // Send config data to resume endpoint
      const formData = new FormData();
      formData.append("text", yamlConfig);

      const resumeResponse = await fetch(
        `${API_BASE_URL}/config/resume/${connection.id}`,
        {
          method: "POST",
          body: formData,
        }
      );

      if (resumeResponse.ok) {
        setLocalState("streaming");
        // FIXED: Only save user-triggered resume
        ConnectionManager.saveConnectionState(
          connection.id,
          "streaming",
          "user"
        );
        if (onResume) onResume(connection.id);
        console.log(
          `User successfully resumed ${connection.id} and started streaming`
        );
      } else {
        const errorText = await resumeResponse.text();
        console.error(`Resume failed for ${connection.id}:`, errorText);
        throw new Error(`Resume failed: ${errorText}`);
      }
    } catch (error) {
      console.error(`Error resuming connection ${connection.id}:`, error);
      alert(
        `Failed to resume connection: ${error.message}\n\nYou may need to edit and save the connection to fix configuration issues.`
      );
    } finally {
      setIsProcessing(false);
    }
  };

  // Terminate connection
  const handleDelete = async (e) => {
    e.stopPropagation();

    if (
      window.confirm(
        `Are you sure you want to remove "${
          connection.name || connection.type
        }"?`
      )
    ) {
      try {
        await fetch(`${API_BASE_URL}/config/disconnect/${connection.id}`, {
          method: "POST",
        });
        console.log(`Successfully disconnected ${connection.id}`);
      } catch (error) {
        console.error(`Disconnect failed for ${connection.id}:`, error);
      }

      onTerminate(connection.id);
    }
  };

  const getBannerStatus = () => {
    if (!streamingStatus) {
      return {
        color: "checking",
        text: "Checking status...",
        detail: "Initializing connection",
      };
    }

    const {
      status,
      reason,
      network_error,
      connection_error,
      protocol,
      is_paused,
      adapter_connected,
    } = streamingStatus;

    if (network_error) {
      return {
        color: "red",
        text: protocol || connection.type || "Server Unreachable",
        detail: "Backend server is down or unreachable",
      };
    }

    if (connection_error) {
      return {
        color: "red",
        text: protocol || connection.type || "Connection Error",
        detail: connection_error,
      };
    }

    // Use local state as the primary source of truth for pause status
    if (localState === "paused" || is_paused) {
      return {
        color: "yellow",
        text: protocol || connection.type || "Paused",
        detail: "Streaming paused - click resume to continue",
      };
    }

    if (status === "active" && adapter_connected) {
      return {
        color: "green",
        text: protocol || connection.type || "Connected",
        detail: reason || "Active streaming",
      };
    }

    if (status === "inactive") {
      return {
        color: "red",
        text: protocol || connection.type || "Inactive",
        detail: reason || "Not streaming",
      };
    }

    if (status === "starting") {
      return {
        color: "blue",
        text: protocol || connection.type || "Starting",
        detail: reason || "Initializing stream",
      };
    }

    return {
      color: "orange",
      text: protocol || connection.type || "Unknown",
      detail: reason || "Status unknown",
    };
  };

  const bannerStatus = getBannerStatus();
  const isPaused = localState === "paused";
  const isActive =
    streamingStatus?.status === "active" &&
    streamingStatus?.adapter_connected &&
    !isPaused;
  const canPause = isActive && !isProcessing;
  const canResume = isPaused && !isProcessing;

  return (
    <div className={`connection-item ${bannerStatus.color}`}>
      <div className={`connection-banner ${bannerStatus.color}`}>
        <div className="banner-content">
          <div className="banner-main">
            <div className="banner-left">
              <span className="banner-title">{bannerStatus.text}</span>
              <span className="banner-detail">{bannerStatus.detail}</span>
              {autoReconnectAttempted && (
                <span className="banner-reconnect">
                  Auto-reconnection attempted
                </span>
              )}
            </div>
            <div className="banner-actions">
              <button
                className="action-button edit-button"
                onClick={(e) => {
                  e.stopPropagation();
                  onEdit(connection);
                }}
                title="Edit Connection"
                disabled={isProcessing}
              >
                <svg
                  className="edit-icon"
                  viewBox="0 0 20 20"
                  xmlns="http://www.w3.org/2000/svg"
                >
                  <path d="M13.586 3.586a2 2 0 112.828 2.828l-.793.793-2.828-2.828.793-.793zM11.379 5.793L3 14.172V17h2.828l8.38-8.379-2.83-2.828z" />
                </svg>
              </button>

              {isPaused ? (
                <button
                  className="action-button play-button"
                  onClick={handleResume}
                  title="Resume Streaming"
                  disabled={!canResume}
                >
                  <svg
                    className="play-icon"
                    viewBox="0 0 20 20"
                    xmlns="http://www.w3.org/2000/svg"
                  >
                    <path
                      fillRule="evenodd"
                      d="M10 18a8 8 0 100-16 8 8 0 000 16zM9.555 7.168A1 1 0 008 8v4a1 1 0 001.555.832l3-2a1 1 0 000-1.664l-3-2z"
                      clipRule="evenodd"
                    />
                  </svg>
                  {isProcessing ? "Resuming..." : ""}
                </button>
              ) : (
                <button
                  className="action-button pause-button"
                  onClick={handlePause}
                  title="Pause Streaming"
                  disabled={!canPause}
                >
                  <svg
                    className="pause-icon"
                    viewBox="0 0 20 20"
                    xmlns="http://www.w3.org/2000/svg"
                  >
                    <path
                      fillRule="evenodd"
                      d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zM7 8a1 1 0 012 0v4a1 1 0 11-2 0V8zm5-1a1 1 0 00-1 1v4a1 1 0 102 0V8a1 1 0 00-1-1z"
                      clipRule="evenodd"
                    />
                  </svg>
                  {isProcessing ? "Pausing..." : ""}
                </button>
              )}

              <button
                className="action-button terminate-button"
                onClick={handleDelete}
                title="Remove Connection"
                disabled={isProcessing}
              >
                <svg
                  className="delete-icon"
                  viewBox="0 0 20 20"
                  xmlns="http://www.w3.org/2000/svg"
                >
                  <path
                    fillRule="evenodd"
                    d="M9 2a1 1 0 00-.894.553L7.382 4H4a1 1 0 000 2v10a2 2 0 002 2h8a2 2 0 002-2V6a1 1 0 100-2h-3.382l-.724-1.447A1 1 0 0011 2H9zM7 8a1 1 0 012 0v6a1 1 0 11-2 0V8zm5-1a1 1 0 00-1 1v6a1 1 0 102 0V8a1 1 0 00-1-1z"
                    clipRule="evenodd"
                  />
                </svg>
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
