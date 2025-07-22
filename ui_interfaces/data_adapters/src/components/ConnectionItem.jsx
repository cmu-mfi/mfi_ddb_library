/**
 * Displays an individual adapter connection in a list, showing its name, status,
 * and real-time streaming indicator. Polls the server for streaming status every 5 seconds,
 * opens a Server-Sent Events stream when active, and provides edit and terminate actions.
 */

import React, { useState, useEffect } from "react";

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
          `/config/streaming-status/${connection.id}`,
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
          is_streaming: false,
          adapter_connected: false,
          broker_connected: false,
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

  // Effect: open SSE stream when streamingStatus.is_streaming becomes true
  useEffect(() => {
    if (!streamingStatus?.is_streaming) return;

    // Create EventSource for real-time stream events
    const es = new EventSource(`/config/stream/${connection.id}`);
    es.onmessage = (e) => {
      // Optionally process real-time data here
      // const msg = JSON.parse(e.data);
      // console.log(`Data for ${connection.name}:`, msg);
    };

    // Close SSE connection on cleanup
    return () => es.close();
  }, [streamingStatus, connection.id, connection.name]);

  // Create a CSS-friendly class name from connection.status
  // Determine full banner status and color
  const getBannerStatus = () => {
    if (!streamingStatus) {
      return {
        color: "checking",
        text: "Checking status...",
        detail: "Initializing connection",
      };
    }

    const {
      adapter_connected,
      broker_connected,
      is_streaming,
      network_error,
      connection_error,
    } = streamingStatus;
    if (network_error) {
      return {
        color: "red",
        text: connection.type || "Server Unreachable",
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
          text: connection.type || "Broker Unreachable",
          detail: connection_error,
        };
      }
      if (
        connection_error.includes("disconnected") ||
        connection_error.includes("not connected")
      ) {
        return {
          color: "red",
          text: connection.type || "Broker Disconnected",
          detail: connection_error,
        };
      }
      if (
        connection_error.includes("authentication") ||
        connection_error.includes("auth")
      ) {
        return {
          color: "red",
          text: connection.type || "Auth Failed",
          detail: connection_error,
        };
      }
      // Generic connection error
      return {
        color: "red",
        text: connection.type || "Connection Error",
        detail: connection_error,
      };
    }
    if (adapter_connected && broker_connected && is_streaming) {
      return {
        color: "green",
        text: connection.type || "Connected",
        detail: `Active Streaming`,
      };
    }

    // Any issue = RED banner with specific message
    if (!adapter_connected) {
      return {
        color: "red",
        text: connection.type || "Adapter Error",
        detail: "Adapter Not streaming",
      };
    }

    if (!broker_connected) {
      return {
        color: "red",
        text: connection.type || "Broker Error",
        detail: "MQTT broker unreachable",
      };
    }

    if (!is_streaming) {
      return {
        color: "red",
        text: connection.type || "Stream Error",
        detail: "Inactive - No data streaming",
      };
    }

    return {
      color: "red",
      text: connection.type || "Connection Issues",
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
                      `Are you sure you want to remove "${connection.name}"?`
                    )
                  ) {
                    try {
                      await fetch(`/config/disconnect/${connection.id}`, {
                        method: "POST",
                      });
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
