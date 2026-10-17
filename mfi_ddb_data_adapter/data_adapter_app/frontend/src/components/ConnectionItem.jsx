import { useState, useEffect, useRef, useCallback } from "react";
import { Pencil, Play, Pause, Trash2, Square } from "lucide-react";
import { ConnectionManager } from "./ConnectionManager";
import { fetchStreamingStatus } from "../api";

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

export default function ConnectionItem({ connection, onEdit, onTerminate }) {
  const [streamingStatus, setStreamingStatus] = useState(null);
  const [isProcessing, setIsProcessing] = useState(false);
  const [localState, setLocalState] = useState(() => ConnectionManager.getConnectionState(connection.id).state);
  const [actionError, setActionError] = useState("");
  const [failedAction, setFailedAction] = useState("");
  const [statusError, setStatusError] = useState("");
  const statusVersionRef = useRef(0);
  const statusRequestRef = useRef(null);
  const mountedRef = useRef(true);
  const processingRef = useRef(false);

  const isCallback = connection.isPolling === false;

  // ─── Status polling ───────────────────────────────────────────────────────

  const invalidateStatus = useCallback(() => {
    statusVersionRef.current++;
    statusRequestRef.current?.abort();
    statusRequestRef.current = null;
  }, []);

  const checkStatus = useCallback(async () => {
    if (!mountedRef.current || processingRef.current || statusRequestRef.current || ConnectionManager.isUpdatingConnection(connection.id) ||
        ConnectionManager.getConnectionState(connection.id).state === "stopped") return;
    const controller = new AbortController();
    statusRequestRef.current = controller;
    const version = ++statusVersionRef.current;
    const canApplyStatus = () => !controller.signal.aborted && version === statusVersionRef.current &&
      !ConnectionManager.isUpdatingConnection(connection.id) &&
      ConnectionManager.getConnectionState(connection.id).state !== "stopped";
    try {
      const response = await fetchStreamingStatus(connection.id, { signal: controller.signal });
      if (!canApplyStatus()) return;
      setStatusError("");
      setStreamingStatus(response);
      setLocalState(response.is_streaming ? "streaming" : "paused");
    } catch (error) {
      if (!canApplyStatus()) return;
      setStatusError(error?.message || "Could not check connection status. Please retry.");
    } finally {
      if (statusRequestRef.current === controller) statusRequestRef.current = null;
    }
  }, [connection.id]);

  useEffect(() => {
    const savedState = ConnectionManager.getConnectionState(connection.id);
    setLocalState(savedState?.state || "streaming");
  }, [connection.id]);

  useEffect(() => {
    mountedRef.current = true;
    checkStatus();
    const interval = setInterval(checkStatus, 5000);
    return () => {
      clearInterval(interval);
      mountedRef.current = false;
      invalidateStatus();
    };
  }, [checkStatus, invalidateStatus]);

  // ─── Actions ──────────────────────────────────────────────────────────────

  const runAction = async (name, operation, nextState) => {
    if (processingRef.current || !mountedRef.current) return;
    processingRef.current = true;
    setIsProcessing(true);
    setActionError("");
    setFailedAction("");
    invalidateStatus();
    try {
      const status = await operation();
      if (!mountedRef.current) return;
      if (nextState) {
        setStatusError("");
        setStreamingStatus(status);
        setLocalState(nextState);
      }
    } catch (error) {
      if (mountedRef.current) {
        const labels = { pause: "pause", resume: "resume", stop: "stop", reconnect: "reconnect", delete: "remove" };
        setActionError(`Could not ${labels[name]} connection ${connection.id}. ${error?.message || "Please try again."}`);
        setFailedAction(name);
      }
    } finally {
      processingRef.current = false;
      if (mountedRef.current) setIsProcessing(false);
    }
  };

  const handlePause = (e) => {
    e.stopPropagation();
    return runAction("pause", () => ConnectionManager.pauseConnection(connection.id), "paused");
  };

  const handleResume = (e) => {
    e.stopPropagation();
    return runAction("resume", () => ConnectionManager.resumeConnection(connection.id, connection), "streaming");
  };

  // Stop: disconnect from backend but keep config in localStorage
  const handleStop = (e) => {
    e.stopPropagation();
    return runAction("stop", () => ConnectionManager.stopConnection(connection.id), "stopped");
  };

  // Reconnect: keep the config and persist streaming only after confirmation.
  const handleReconnect = (e) => {
    e.stopPropagation();
    return runAction("reconnect", () => ConnectionManager.reconnectConnection(connection.id, connection), "streaming");
  };

  const handleDelete = async (e) => {
    e.stopPropagation();
    if (!processingRef.current && window.confirm(`Are you sure you want to remove connection?\n${connection.id} - ${connection.adapter}`)) {
      return runAction("delete", () => onTerminate(connection.id));
    }
  };

  const handleRetryAction = (e) => {
    const handlers = { pause: handlePause, resume: handleResume, stop: handleStop, reconnect: handleReconnect, delete: handleDelete };
    return handlers[failedAction]?.(e);
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
    if (statusError) {
      return { color: "checking", text: `Connection ${connection.id} - ${connection.adapter}`, detail: "Unable to confirm current status" };
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
        {statusError && (
          <div role="alert" className="mt-2 text-sm text-red-700">
            <p>{statusError}</p>
            <button type="button" className="mt-1 font-medium underline disabled:opacity-50" onClick={checkStatus} disabled={isProcessing}>Retry status check</button>
          </div>
        )}
        {actionError && (
          <div role="alert" className="mt-2 text-sm text-red-700">
            <p>{actionError}</p>
            <button type="button" className="mt-1 font-medium underline disabled:opacity-50" onClick={handleRetryAction} disabled={isProcessing}>Retry {failedAction === "delete" ? "remove" : failedAction}</button>
          </div>
        )}
      </div>
    </div>
  );
}
