import { useState, useEffect, useRef, useCallback } from "react";
import { Pencil, Play, Pause, Trash2, Square } from "lucide-react";
import { ConnectionManager } from "./ConnectionManager";
import { pauseConnection, resumeConnection, connectConnection, disconnectConnection, fetchStreamingStatus } from "../api";

const bannerBg = {
  green:    "bg-gradient-to-br from-green-100 to-green-200 border-l-4 border-green-500",
  yellow:   "bg-gradient-to-br from-yellow-100 to-yellow-200 border-l-4 border-yellow-400",
  red:      "bg-gradient-to-br from-red-100 to-red-200 border-l-4 border-red-500",
  checking: "bg-gradient-to-br from-gray-100 to-gray-200 border-l-4 border-gray-500",
  grey:     "bg-gradient-to-br from-gray-100 to-gray-200 border-l-4 border-gray-400",
};

const titleColor = {
  green:    "text-green-800",
  yellow:   "text-yellow-700",
  red:      "text-red-800",
  checking: "text-gray-700",
  grey:     "text-gray-600",
};

const detailColor = {
  green:    "text-green-700",
  yellow:   "text-yellow-600",
  red:      "text-red-700",
  checking: "text-gray-600",
  grey:     "text-gray-500",
};

export default function ConnectionItem({ connection, onEdit, onTerminate, onPause, onResume }) {
  const [streamingStatus, setStreamingStatus] = useState(null);
  const [isProcessing, setIsProcessing] = useState(false);
  const [localState, setLocalState] = useState("streaming");
  const statusCheckIntervalRef = useRef(null);

  const isCallback = connection.isPolling === false;

  // ─── Status polling ───────────────────────────────────────────────────────

  const checkStatus = useCallback(async () => {
    // Don't poll the backend if we know the connection is intentionally stopped
    if (localState === "stopped") return;
    try {
      const response = await fetchStreamingStatus(connection.id);
      setStreamingStatus(response);
      setLocalState(response.is_streaming ? "streaming" : "paused");
    } catch (error) {
      console.error(`Failed to check streaming status for ${connection.id}:`, error);
      setStreamingStatus({ streaming_mode: "inactive", is_connected: false, is_streaming: false });
    }
  }, [connection.id, localState]);

  useEffect(() => {
    const savedState = ConnectionManager.getConnectionState(connection.id);
    setLocalState(savedState?.state || "streaming");
  }, [connection.id]);

  useEffect(() => {
    checkStatus();
    const intervalRef = setInterval(checkStatus, 5000);
    statusCheckIntervalRef.current = intervalRef;
    return () => clearInterval(statusCheckIntervalRef.current);
  }, [checkStatus]);

  // ─── Actions ──────────────────────────────────────────────────────────────

  const handlePause = async (e) => {
    e.stopPropagation();
    setIsProcessing(true);
    const ok = await pauseConnection(connection.id);
    if (ok) {
      setLocalState("paused");
      ConnectionManager.saveConnectionState(connection.id, "paused", "user");
      if (onPause) onPause(connection.id);
    }
    setIsProcessing(false);
  };

  const handleResume = async (e) => {
    e.stopPropagation();
    setIsProcessing(true);
    const ok = await resumeConnection(connection.id);
    if (ok) {
      setLocalState("streaming");
      ConnectionManager.saveConnectionState(connection.id, "streaming", "user");
      if (onResume) onResume(connection.id);
    }
    setIsProcessing(false);
  };

  // Stop: disconnect from backend but keep config in localStorage
  const handleStop = async (e) => {
    e.stopPropagation();
    setIsProcessing(true);
    const ok = await disconnectConnection(connection.id);
    if (ok) {
      setStreamingStatus({ streaming_mode: "inactive", is_connected: false, is_streaming: false });
      setLocalState("stopped");
      ConnectionManager.saveConnectionState(connection.id, "stopped", "user");
      if (onPause) onPause(connection.id);
    }
    setIsProcessing(false);
  };

  // Reconnect: re-establish using saved config, no localStorage change
  const handleReconnect = async (e) => {
    e.stopPropagation();
    setIsProcessing(true);
    try {
      const response = await connectConnection(
        connection.id,
        connection.adapter,
        connection.adapterConfig,
        connection.streamerConfig,
        connection.isPolling !== false,
        connection.pollingRateHz ?? 1
      );
      if (response.ok) {
        setLocalState("streaming");
        ConnectionManager.saveConnectionState(connection.id, "streaming", "user");
        if (onResume) onResume(connection.id);
        // Let checkStatus pick up the real state
        setTimeout(checkStatus, 500);
      } else {
        console.error(`Reconnect failed for ${connection.id}: ${response.status}`);
      }
    } catch (err) {
      console.error(`Reconnect error for ${connection.id}:`, err);
    }
    setIsProcessing(false);
  };

  const handleDelete = async (e) => {
    e.stopPropagation();
    if (window.confirm(`Are you sure you want to remove connection?\n${connection.id} - ${connection.adapter}`)) {
      onTerminate(connection.id);
    }
  };

  // ─── Derived state ────────────────────────────────────────────────────────

  const isPaused  = localState === "paused";
  const isStopped = localState === "stopped";
  const isActive  = streamingStatus?.is_streaming && streamingStatus?.is_connected;

  const canPause     = isActive && !isPaused && !isCallback && !isProcessing;
  const canResume    = isPaused && !isCallback && !isProcessing;
  const canStop      = isActive && isCallback && !isProcessing;
  const canReconnect = isStopped && isCallback && !isProcessing;

  // ─── Banner ───────────────────────────────────────────────────────────────

  const getBannerStatus = () => {
    if (isStopped) {
      return { color: "grey", text: `Connection ${connection.id} - ${connection.adapter}`, detail: "Disconnected — click Reconnect to resume" };
    }
    if (!streamingStatus) {
      return { color: "grey", text: "Checking status...", detail: "Initializing connection" };
    }
    const { streaming_mode, is_connected, is_streaming } = streamingStatus;
    if (is_connected && is_streaming) {
      return { color: "green", text: `Connection ${connection.id} - ${connection.adapter}`, detail: `Streaming in ${streaming_mode} mode` };
    } else if (is_connected && !is_streaming) {
      return { color: "yellow", text: `Connection ${connection.id} - ${connection.adapter}`, detail: "Connected but not streaming" };
    } else if (!is_connected) {
      return { color: "red", text: `Connection ${connection.id} - ${connection.adapter}`, detail: "Not connected" };
    }
    return { color: "checking", text: `Connection ${connection.id} - ${connection.adapter}`, detail: "Status unknown. Trying..." };
  };

  const bannerStatus = getBannerStatus();

  const actionBtn = "bg-white/90 border border-black/10 p-2 rounded-md cursor-pointer text-gray-500 transition-all min-w-[40px] h-10 flex items-center justify-center backdrop-blur-sm hover:scale-105 disabled:opacity-50 disabled:cursor-not-allowed disabled:transform-none";

  // ─── Render ───────────────────────────────────────────────────────────────

  return (
    <div className={`rounded-lg overflow-hidden mb-3 shadow transition-all hover:-translate-y-px hover:shadow-md ${bannerBg[bannerStatus.color] || bannerBg.grey}`}>
      <div className="w-full px-5 py-4">
        <div className="flex justify-between items-center">
          <div className="flex flex-col gap-0.5">
            <span className={`text-base font-semibold ${titleColor[bannerStatus.color] || titleColor.grey}`}>
              {bannerStatus.text}
            </span>
            <span className={`text-[13px] font-medium ${detailColor[bannerStatus.color] || detailColor.grey}`}>
              {bannerStatus.detail}
            </span>
          </div>

          <div className="flex gap-3 items-center ml-5">
            <button
              className={actionBtn}
              onClick={(e) => { e.stopPropagation(); onEdit(connection); }}
              title="Edit Connection"
              disabled={isProcessing}
            >
              <Pencil size={16} />
            </button>

            {/* Polling: Pause / Resume */}
            {!isCallback && (
              isPaused ? (
                <button className={actionBtn} onClick={handleResume} title="Resume Streaming" disabled={!canResume}>
                  <Play size={16} />
                </button>
              ) : (
                <button className={actionBtn} onClick={handlePause} title="Pause Streaming" disabled={!canPause}>
                  <Pause size={16} />
                </button>
              )
            )}

            {/* Callback: Stop / Reconnect */}
            {isCallback && (
              isStopped ? (
                <button className={actionBtn} onClick={handleReconnect} title="Reconnect" disabled={!canReconnect}>
                  <Play size={16} />
                </button>
              ) : (
                <button className={actionBtn} onClick={handleStop} title="Stop (disconnect, keep config)" disabled={!canStop}>
                  <Square size={16} />
                </button>
              )
            )}

            <button
              className={`${actionBtn} bg-red-50/90 border-red-500 text-red-500 hover:bg-red-500 hover:text-white`}
              onClick={handleDelete}
              title="Remove Connection"
              disabled={isProcessing}
            >
              <Trash2 size={16} />
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
