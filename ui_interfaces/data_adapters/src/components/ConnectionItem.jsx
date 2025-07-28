/**
 * Displays an individual adapter connection in a list, showing its name, status,
 * and real-time streaming indicator. Polls the server for streaming status every 5 seconds,
 * opens a Server-Sent Events stream when active, and provides edit and terminate actions.
 */

import React, { useState, useEffect } from "react";

// Add API base URL configuration
const API_BASE_URL = process.env.REACT_APP_API_URL || "http://localhost:8000";

// Note: You'll also need to update ConnectionModal.jsx to use the same API_BASE_URL
// for its callConfig function calls.

/**
 * ConnectionItem component props:
 * @param {object} connection - Connection object with id, name, and status fields
 * @param {function} onEdit - Callback invoked when edit button is clicked
 * @param {function} onTerminate - Callback invoked after successful termination
 */
export default function ConnectionItem({ connection, onEdit, onTerminate }) {
  // Local state for current streaming status fetched from server
  const [streamingStatus, setStreamingStatus] = useState(null);

  // Effect: poll /config/streaming-status/{id} every 5 seconds
  useEffect(() => {
    // Async function to fetch streaming status with timeout and error detection
    const checkStatus = async () => {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 5000); // 5 second timeout

      try {
        const response = await fetch(
          `${API_BASE_URL}/config/streaming-status/${connection.id}`, // Use API_BASE_URL
          {
            signal: controller.signal,
            headers: { "Content-Type": "application/json" },
          }
        );
        clearTimeout(timeoutId);

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }

        const status = await response.json();
        console.log(`Status for ${connection.id}:`, status); // Add debug logging
        setStreamingStatus(status);
      } catch (error) {
        clearTimeout(timeoutId);
        console.error("Failed to check streaming status:", error);

        // Detect different types of network errors
        const isNetworkError =
          error.name === "AbortError" ||
          error.message.includes("fetch") ||
          error.message.includes("NetworkError") ||
          error.message.includes("Failed to fetch");

        // Set appropriate status based on error type
        setStreamingStatus({
          status: "inactive",
          adapter_connected: false,
          is_streaming: false,
          stream_rate: null,
          network_error: isNetworkError, // Add this flag
        });
      }
    };
    // Perform initial check immediately
    checkStatus();
    // Schedule repeated checks every 5000ms
    const interval = setInterval(checkStatus, 5000);
    // Cleanup interval on unmount or connection.id change
    return () => clearInterval(interval);
  }, [connection.id]);

  // Determine full banner status and color based on new API response format
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
      adapter_connected,
      is_streaming,
      network_error,
      connection_error,
      protocol,
    } = streamingStatus;

    if (network_error) {
      return {
        color: "red",
        text: protocol || connection.type || "Server Unreachable",
        detail: "Backend server is down or unreachable",
      };
    }

    if (connection_error) {
      // Categorize different types of errors for better user experience
      if (
        connection_error.includes("Broker unreachable") ||
        connection_error.includes("refused") ||
        connection_error.includes("timeout")
      ) {
        return {
          color: "red",
          text: protocol || connection.type || "Broker Unreachable",
          detail: connection_error,
        };
      }
      if (
        connection_error.includes("disconnected") ||
        connection_error.includes("not connected")
      ) {
        return {
          color: "red",
          text: protocol || connection.type || "Broker Disconnected",
          detail: connection_error,
        };
      }
      if (
        connection_error.includes("authentication") ||
        connection_error.includes("auth")
      ) {
        return {
          color: "red",
          text: protocol || connection.type || "Auth Failed",
          detail: connection_error,
        };
      }
      // Generic connection error
      return {
        color: "red",
        text: protocol || connection.type || "Connection Error",
        detail: connection_error,
      };
    }

    // Use the status field from the new API response
    if (status === "active") {
      return {
        color: "green",
        text: protocol || connection.type || "Connected",
        detail: reason || "Active Streaming",
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
        color: "yellow",
        text: protocol || connection.type || "Starting",
        detail: reason || "Initializing stream",
      };
    }

    // Fallback to old logic if status field not present
    if (adapter_connected && is_streaming) {
      return {
        color: "green",
        text: protocol || connection.type || "Connected",
        detail: "Active Streaming",
      };
    }

    if (!adapter_connected) {
      return {
        color: "red",
        text: protocol || connection.type || "Adapter Error",
        detail: "Adapter not connected",
      };
    }

    if (!is_streaming) {
      return {
        color: "red",
        text: protocol || connection.type || "Stream Error",
        detail: "Inactive - No data streaming",
      };
    }

    return {
      color: "red",
      text: protocol || connection.type || "Connection Issues",
      detail: "Unknown error state",
    };
  };

  const bannerStatus = getBannerStatus();

  return (
    <div className={`connection-item ${bannerStatus.color}`}>
      {/* Full-width status banner */}
      <div className={`connection-banner ${bannerStatus.color}`}>
        <div className="banner-content">
          <div className="banner-main">
            <div className="banner-left">
              <span className="banner-title">{bannerStatus.text}</span>
              <span className="banner-detail">{bannerStatus.detail}</span>
            </div>
            <div className="banner-actions">
              <button
                className="edit-button"
                onClick={(e) => {
                  e.stopPropagation();
                  onEdit(connection);
                }}
                title="Edit Connection"
              >
                ✎
              </button>

              <button
                className="terminate-button"
                onClick={async (e) => {
                  e.stopPropagation();
                  if (
                    window.confirm(
                      `Are you sure you want to remove "${
                        connection.name || connection.type
                      }"?`
                    )
                  ) {
                    try {
                      await fetch(
                        `${API_BASE_URL}/config/disconnect/${connection.id}`,
                        {
                          // Use API_BASE_URL
                          method: "POST",
                        }
                      );
                      console.log("Disconnected connection:", connection.id);
                    } catch (error) {
                      console.error("Disconnect failed:", error);
                    }
                    onTerminate(connection.id);
                  }
                }}
                title="Remove Connection"
              >
                ✖
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
