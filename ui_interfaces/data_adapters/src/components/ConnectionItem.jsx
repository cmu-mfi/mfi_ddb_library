import React, { useState, useEffect } from "react";

export default function ConnectionItem({ connection, onEdit, onTerminate }) {
  const [streamingStatus, setStreamingStatus] = useState(null);

  // Poll streaming status every 5 seconds for this adapter
  useEffect(() => {
    const checkStatus = async () => {
      try {
        const response = await fetch(
          `/config/streaming-status/${connection.id}`
        );
        const status = await response.json();
        setStreamingStatus(status);
      } catch (error) {
        console.error("Failed to check streaming status:", error);
        setStreamingStatus({
          is_streaming: false,
          adapter_connected: false,
          stream_rate: null,
        });
      }
    };

    // Initial check
    checkStatus();
    // Poll interval
    const interval = setInterval(checkStatus, 5000);
    return () => clearInterval(interval);
  }, [connection.id]);
  // Open SSE when streaming starts
  useEffect(() => {
    if (!streamingStatus?.is_streaming) return;

    const es = new EventSource(`/config/stream/${connection.id}`);
    es.onmessage = (e) => {
      // Optionally, handle real-time data here:
      // const msg = JSON.parse(e.data);
      // console.log(`Data for ${connection.name}:`, msg);
    };

    return () => es.close();
  }, [streamingStatus, connection.id]);

  // Normalize status for CSS
  const statusClass = (connection.status || "pending")
    .toLowerCase()
    .replace(/\s+/g, "");

  return (
    <div className={`connection-item ${statusClass}`}>
      <div className="connection-main">
        <span className="connection-name">{connection.name}</span>

        <div className="connection-actions">
          <span className={`connection-status ${statusClass}`}>
            {connection.status || "Pending"}
          </span>

          <button
            className="edit-button"
            onClick={(e) => {
              e.stopPropagation();
              onEdit(connection);
            }}
          >
            ✎
          </button>

          <button
            className="terminate-button"
            onClick={(e) => {
              e.stopPropagation();
              if (
                window.confirm(
                  `Are you sure you want to remove "${connection.name}"?`
                )
              ) {
                onTerminate(connection.id);
              }
            }}
            title="Remove Connection"
          >
            ✖
          </button>
        </div>
      </div>

      {/* Streaming status UI */}
      <div className="stream-data">
        {streamingStatus === null ? (
          <div className="streaming-status-checking">
            <div className="loading-spinner"></div>
            Checking status...
          </div>
        ) : (
          <div className="streaming-status-container">
            <div
              className={`status-indicator ${
                streamingStatus.is_streaming ? "active" : "inactive"
              }`}
            ></div>
            {streamingStatus.is_streaming ? (
              <div className="streaming-status-text">
                Active - Data streaming to broker
              </div>
            ) : (
              <div className="streaming-status-text inactive">
                Inactive - No data streaming
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
