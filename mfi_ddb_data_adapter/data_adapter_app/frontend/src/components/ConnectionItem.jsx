import { useState } from "react";
import { Pencil, Play, Pause, Trash2, Square } from "lucide-react";
import { pauseConnection, resumeConnection, connectConnection, stopConnection } from "../api";

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
  const [isProcessing, setIsProcessing] = useState(false);

  const isCallback = connection.isPolling === false;

  // Status comes straight from the backend's /all response (via the
  // connection prop) - no per-item polling, no localStorage.
  const isConnected = Boolean(connection.isConnected);
  const isStreaming = Boolean(connection.isStreaming);
  const isActive = isConnected && isStreaming;
  const isPaused = isConnected && !isStreaming;
  const isStopped = !isConnected;

  const canPause     = isActive && !isCallback && !isProcessing;
  const canResume    = isPaused && !isCallback && !isProcessing;
  // Stop/Reconnect apply to both modes - the backend's disconnect()/
  // connect_and_stream() already handle the polling-loop teardown/restart
  // correctly regardless of is_polling, this was only ever a UI gap.
  const canStop      = isConnected && !isProcessing;
  const canReconnect = isStopped && !isProcessing;

  // ─── Actions ──────────────────────────────────────────────────────────────

  const handlePause = async (e) => {
    e.stopPropagation();
    setIsProcessing(true);
    const ok = await pauseConnection(connection.id);
    if (ok && onPause) onPause(connection.id);
    setIsProcessing(false);
  };

  const handleResume = async (e) => {
    e.stopPropagation();
    setIsProcessing(true);
    const ok = await resumeConnection(connection.id);
    if (ok && onResume) onResume(connection.id);
    setIsProcessing(false);
  };

  // Stop: the backend keeps the connection (and its config) known, so
  // Reconnect below still works with no client-side memory needed.
  const handleStop = async (e) => {
    e.stopPropagation();
    setIsProcessing(true);
    const ok = await stopConnection(connection.id);
    if (ok && onPause) onPause(connection.id);
    setIsProcessing(false);
  };

  const handleReconnect = async (e) => {
    e.stopPropagation();
    setIsProcessing(true);
    try {
      const response = await connectConnection(connection.id, connection.adapter);
      if (response.ok) {
        if (onResume) onResume(connection.id);
      } else {
        console.error(`Reconnect failed for ${connection.id}: ${response.status}`);
      }
    } catch (err) {
      console.error(`Reconnect error for ${connection.id}:`, err);
    }
    setIsProcessing(false);
  };

  const handleDelete = (e) => {
    e.stopPropagation();
    if (window.confirm(`Are you sure you want to remove connection?\n${connection.id} - ${connection.adapter}`)) {
      onTerminate(connection.id);
    }
  };

  // ─── Banner ───────────────────────────────────────────────────────────────

  const getBannerStatus = () => {
    if (isStopped) {
      return { color: "grey", text: `Connection ${connection.id} - ${connection.adapter}`, detail: "Disconnected — click Reconnect to resume" };
    }
    if (isActive) {
      return { color: "green", text: `Connection ${connection.id} - ${connection.adapter}`, detail: `Streaming in ${isCallback ? "callback" : "polling"} mode` };
    }
    return { color: "yellow", text: `Connection ${connection.id} - ${connection.adapter}`, detail: "Connected but not streaming" };
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

            {/* Stopped (either mode): only Reconnect is available */}
            {isStopped && (
              <button className={actionBtn} onClick={handleReconnect} title="Reconnect" disabled={!canReconnect}>
                <Play size={16} />
              </button>
            )}

            {/* Polling, not stopped: Pause / Resume */}
            {!isCallback && !isStopped && (
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

            {/* Not stopped (either mode): Stop is always available alongside
                Pause/Resume for polling, or on its own for callback */}
            {!isStopped && (
              <button className={actionBtn} onClick={handleStop} title="Stop (disconnect, keep config)" disabled={!canStop}>
                <Square size={16} />
              </button>
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
