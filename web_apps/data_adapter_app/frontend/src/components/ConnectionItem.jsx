import React, { useState, useEffect, useRef, useCallback } from "react";
import { ConnectionManager } from "./ConnectionManager";
import { pauseConnection, resumeConnection } from "../api";
import { connectConnection, disconnectConnection } from "../api";
import { fetchStreamingStatus } from "../api";

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

  // Auto-reconnection logic
  const attemptAutoReconnect = useCallback(async () => {
    if (autoReconnectAttempted) return;

    const savedState = ConnectionManager.getConnectionState(connection.id);
    const targetState = savedState?.state || "streaming";

    if (localState !== targetState) {
      console.log(`No need to auto-reconnect ${connection.id} - already in ${localState}`);
      return;
    }
    
    console.log(`Auto-reconnecting ${connection.id} to ${targetState} state`);

    try {
      if (localState === "inactive") {
        const connectResponse = await connectConnection(
          connection.id,
          connection.adapter,
          connection.adapterConfig,
          connection.streamerConfig
        );

        if (!connectResponse.ok) {
          console.error(`Failed to auto-reconnect ${connection.id}`);
          return;
        }
      } else if (localState === "paused" && targetState === "streaming") {
        const success = await resumeConnection(connection.id);
        if (!success) {
          console.error(`Failed to auto-resume ${connection.id}`);
          return;
        } else {
          console.log(`Auto-resumed ${connection.id} after reconnection`);
        }
      } else if (localState === "streaming" && targetState === "paused") {
        const is_paused = await pauseConnection(connection.id);
        if (!is_paused) {
          console.error(`Failed to auto-pause ${connection.id}`);
          return;
        } else {
          console.log(`Auto-paused ${connection.id} after reconnection`);
        }
      } else {
        console.log(`Unknown state ${localState} for ${connection.id}, or ${targetState} target state`);
      }

    } catch (error) {
      console.error(`Auto-reconnect failed for ${connection.id}:`, error);
    }

    setAutoReconnectAttempted(true);
  }, [connection, autoReconnectAttempted, localState]);

  // FIXED: Enhanced status checking - don't auto-save state changes
  const checkStatus = useCallback(async () => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000);

    try {
      const response = await fetchStreamingStatus(connection.id);
      clearTimeout(timeoutId);
      
      setStreamingStatus(response);

      // Reset auto-reconnect flag on successful status check
      setAutoReconnectAttempted(false);

      if (!response.ok){
        setLocalState("inactive");
      } else if (response.is_streaming) {
        setLocalState("streaming");
      } else {
        setLocalState("paused");
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
  }, [connection.id, autoReconnectAttempted, attemptAutoReconnect]);

  // FIXED: Pause handler - ONLY user action saves to localStorage
  const handlePause = async (e) => {
    e.stopPropagation();
    setIsProcessing(true);

    const is_paused = await pauseConnection(connection.id);

    if (is_paused) {
      setLocalState("paused");
      // FIXED: Only save user-triggered pause
      ConnectionManager.saveConnectionState(connection.id, "paused", "user");
      if (onPause) onPause(connection.id);
      console.log(`User successfully paused ${connection.id}`);
    }
    setIsProcessing(false);
  };

  // FIXED: Resume handler - ONLY user action saves to localStorage
  const handleResume = async (e) => {
    e.stopPropagation();
    setIsProcessing(true);

    const is_resumed = await resumeConnection(connection.id);

    if (is_resumed) {
      setLocalState("streaming");
      // FIXED: Only save user-triggered resume
      ConnectionManager.saveConnectionState(connection.id, "streaming", "user");
      if (onResume) onResume(connection.id);
      console.log(`User successfully resumed ${connection.id}`);
    }
    setIsProcessing(false);
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
      await disconnectConnection(connection.id);
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

  // Load saved connection state from localStorage
  useEffect(() => {
    const savedState = ConnectionManager.getConnectionState(connection.id);
    const initialState = savedState?.state || "streaming";
    setLocalState(initialState);
    console.log(`Loaded initial state for ${connection.id}: ${initialState}`);
  }, [connection.id]);


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
