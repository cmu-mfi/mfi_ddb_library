import { useState, useRef, useEffect, useCallback, useMemo } from "react";
import Modal from "./Modal";
import { ConnectionManager } from "./ConnectionManager";
import { getConnCtr, setConnCtr } from "../state/conn_ctr";
import {
  connectConnection,
  disconnectConnection,
  fetchAdapters,
  fetchStreamerConfig,
  validateAdapterConfig,
  validateStreamerConfig,
} from "../api";

// ─── YAML helpers ─────────────────────────────────────────────────────────────

const wrapConfigForProtocol = (adapters, connectionType, innerYaml) => {
  const adapter = adapters.find((a) => a.name === connectionType);
  if (!adapter) return (innerYaml || "").trim();
  const key = adapter.key;
  const configExample = adapter.configExample?.raw || {};
  const needsWrapping = key && key in configExample;
  if (!needsWrapping) return (innerYaml || "").trim();
  const inner = (innerYaml || "").trim();
  if (!inner || inner.startsWith(`${key}:`)) return inner;
  const indented = inner.split("\n").map((l) => `  ${l}`).join("\n");
  return `${key}:\n${indented}`;
};

const generateHelpNodes = (helpData) => {
  if (!helpData || typeof helpData !== "string") return null;
  return helpData.split("\n").map((line, i) => {
    const leadingSpaces = (line.match(/^(\s*)/) || ["", ""])[1].length;
    const indent = Math.floor(leadingSpaces / 2) * 16;
    const colonIdx = line.indexOf(":");
    if (line.trim() === "") return null;
    if (colonIdx > -1) {
      const key = line.substring(0, colonIdx).trim();
      const value = line.substring(colonIdx + 1).trim();
      if (value) {
        return (
          <div key={i} style={{ marginLeft: indent }} className="text-xs leading-5">
            <span className="font-medium text-gray-700">{key}:</span>{" "}
            <span className="text-gray-500">{value}</span>
          </div>
        );
      }
      return (
        <div key={i} style={{ marginLeft: indent }} className="text-xs font-semibold text-gray-800 mt-1.5">
          {key}:
        </div>
      );
    }
    return (
      <div key={i} style={{ marginLeft: indent }} className="text-xs font-semibold text-gray-800 mt-1.5">
        {line.trim()}
      </div>
    );
  }).filter(Boolean);
};

// ─── Component ────────────────────────────────────────────────────────────────

export default function ConnectionModal({ isOpen, onClose, onSave, initialData = {} }) {
  const [connectionType, setConnectionType] = useState("");
  const [configuration, setConfiguration] = useState("");
  const [streamerExample, setStreamerExample] = useState("");
  const [streamerConfig, setStreamerConfig] = useState("");
  const [adapters, setAdapters] = useState([]);
  const [step, setStep] = useState("");
  const [validationError, setValidationError] = useState("");
  const [selectedFile, setSelectedFile] = useState(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isValidating, setIsValidating] = useState(false);
  const [validationWarnings, setValidationWarnings] = useState([]);
  const [showHelp, setShowHelp] = useState(false);
  const [activeConnectionId, setActiveConnectionId] = useState(null);
  const [isPolling, setIsPolling] = useState(true);
  const [pollingRateHz, setPollingRateHz] = useState("1");
  const fileInputRef = useRef(null);
  const lastValidatedRef = useRef("");

  const connectionTypes = adapters.map((a) => a.name);

  const selectedAdapter = useMemo(
    () => adapters.find((a) => a.name === connectionType),
    [adapters, connectionType]
  );

  const supportsCallback = Boolean(selectedAdapter?.selfUpdate);

  const exampleInnerByName = useMemo(
    () => adapters.reduce((m, a) => { m[a.name] = a.configExample?.configuration?.trim() || ""; return m; }, {}),
    [adapters]
  );

  const exampleConfigMap = useMemo(
    () => adapters.reduce((m, a) => { m[a.name] = wrapConfigForProtocol(adapters, a.name, exampleInnerByName[a.name]); return m; }, {}),
    [adapters, exampleInnerByName]
  );

  const configHelpMap = useMemo(
    () => adapters.reduce((m, a) => { m[a.name] = a.configHelpText || ""; return m; }, {}),
    [adapters]
  );

  // ─── Handlers ─────────────────────────────────────────────────────────────

  const handleTypeChange = useCallback((e) => {
    const t = e.target.value;
    setConnectionType(t);
    setConfiguration(exampleConfigMap[t] || "");
    setStreamerConfig(streamerExample);
    setIsPolling(true);
    setPollingRateHz("1");
    setValidationError("");
    lastValidatedRef.current = "";
  }, [exampleConfigMap, streamerExample]);

  const handleFileUpload = useCallback((e) => {
    const f = e.target.files?.[0];
    if (!f) return setSelectedFile(null);
    setSelectedFile(f);
    const r = new FileReader();
    r.onload = ({ target }) => typeof target.result === "string" && setConfiguration(target.result);
    r.readAsText(f);
  }, []);

  const handleClose = useCallback(async () => {
    if (isSubmitting) return;
    if (activeConnectionId) {
      await disconnectConnection(activeConnectionId).catch(console.error);
      setActiveConnectionId(null);
    }
    onClose();
  }, [isSubmitting, activeConnectionId, onClose]);

  const validateConfiguration = useCallback(async () => {
    setValidationError("");
    setValidationWarnings([]);
    setIsValidating(true);
    try {
      if (!connectionType) throw new Error("Select connection type");
      if (!configuration.trim()) throw new Error("Enter YAML config");
      const [adp_valid, streamer_valid] = await Promise.all([
        validateAdapterConfig(connectionType, configuration),
        validateStreamerConfig(streamerConfig),
      ]);
      if (!adp_valid) setValidationError((p) => p + "Data Adapter configuration validation error.\n");
      if (!streamer_valid) setValidationError((p) => p + "Streamer configuration validation error.\n");
      return { valid: adp_valid && streamer_valid };
    } catch (e) {
      console.error("Validation error:", e);
      return { valid: false };
    } finally {
      setIsValidating(false);
    }
  }, [connectionType, configuration, streamerConfig]);

  const handleSave = useCallback(async () => {
    if (isSubmitting) return;
    setIsSubmitting(true);
    setValidationError("");
    try {
      const validation = await validateConfiguration();
      if (!validation?.valid) { setIsSubmitting(false); return; }

      const effectiveIsPolling = supportsCallback ? isPolling : true;
      const hzInt = Math.max(1, parseInt((pollingRateHz || "1").trim(), 10) || 1);

      if (initialData.id) {
        const updated = {
          ...initialData,
          adapter: connectionType,
          adapterConfig: configuration,
          streamerConfig,
          updatedAt: new Date().toISOString(),
        };
        ConnectionManager.saveConnection(initialData.id, updated);
        onSave(updated);
        onClose();
        return;
      }

      const id = getConnCtr() + 1;
      setConnCtr(id);
      setActiveConnectionId(id);
      setStep("Connecting to adapter...");

      await connectConnection(id, connectionType, configuration, streamerConfig, effectiveIsPolling, hzInt);

      const saved = {
        id,
        adapter: connectionType,
        adapterConfig: configuration,
        streamerConfig,
        isPolling: effectiveIsPolling,
        pollingRateHz: hzInt,
        savedAt: new Date().toISOString(),
      };
      ConnectionManager.saveConnection(id, saved);
      onSave(saved);
      onClose();
    } catch (e) {
      console.error(e);
    } finally {
      setStep("");
      setIsSubmitting(false);
    }
  }, [isSubmitting, validateConfiguration, configuration, connectionType, streamerConfig, initialData, onSave, onClose, supportsCallback, isPolling, pollingRateHz]);

  const canSave = Boolean(
    connectionType && configuration.trim() && !validationError && !isValidating && !isSubmitting
  );
  const isEditMode = Boolean(initialData.id);
  const helpData = configHelpMap[connectionType];

  // ─── Effects ──────────────────────────────────────────────────────────────

  useEffect(() => {
    fetchAdapters().then(setAdapters).catch(console.error);
    fetchStreamerConfig()
      .then((data) => setStreamerExample(data?.configExample?.configuration || ""))
      .catch(console.error);
  }, []);

  useEffect(() => {
    if (!isOpen) return;
    setConnectionType(initialData.adapter || "");
    setConfiguration(initialData.adapterConfig || "");
    setStreamerConfig(initialData.streamerConfig || streamerExample);
    setIsPolling(typeof initialData.isPolling === "boolean" ? initialData.isPolling : true);
    setPollingRateHz(initialData.pollingRateHz != null ? String(initialData.pollingRateHz) : "1");
    setStep("");
    setValidationError("");
    setSelectedFile(null);
    setIsSubmitting(false);
    lastValidatedRef.current = "";
    if (fileInputRef.current) fileInputRef.current.value = "";
  }, [isOpen, initialData, streamerExample]);

  useEffect(() => {
    if (!isOpen || !connectionType || !configuration.trim()) { setValidationError(""); return; }
    const id = setTimeout(() => {
      const combined = `adapter:${connectionType}||${configuration}||${streamerConfig}`;
      if (combined !== lastValidatedRef.current) {
        lastValidatedRef.current = combined;
        validateConfiguration();
      }
    }, 800);
    return () => clearTimeout(id);
  }, [isOpen, connectionType, configuration, streamerConfig, validateConfiguration]);

  // ─── Shared input classes ──────────────────────────────────────────────────

  const inputCls = "w-full border border-gray-300 rounded-md px-3 py-2 text-gray-700 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-colors disabled:bg-gray-100 disabled:cursor-not-allowed";
  const textareaCls = `${inputCls} resize-y font-mono text-xs`;

  // ─── Render ───────────────────────────────────────────────────────────────

  return (
    <Modal isOpen={isOpen} onClose={handleClose} title={isEditMode ? "Edit Adapter" : "New Adapter"}>
      <div className="p-6 space-y-5">

        {/* Adapter type */}
        <div>
          <label className="block mb-1.5 text-sm font-medium text-gray-800" htmlFor="connection-type">
            Data Adapter:
          </label>
          <select id="connection-type" value={connectionType} onChange={handleTypeChange} className={inputCls} disabled={isSubmitting}>
            <option value="">Select a type</option>
            {connectionTypes.map((type) => <option key={type} value={type}>{type}</option>)}
          </select>
        </div>

        {/* Configuration YAML */}
        <div>
          <div className="flex items-center justify-between mb-1.5">
            <label className="text-sm font-medium text-gray-800" htmlFor="configuration">
              Configuration (YAML):
            </label>
            {helpData && (
              <div className="relative">
                <button
                  type="button"
                  className="w-6 h-6 rounded-full bg-gray-200 hover:bg-gray-300 text-gray-600 text-xs font-bold leading-none flex items-center justify-center"
                  onMouseEnter={() => setShowHelp(true)}
                  onMouseLeave={() => setShowHelp(false)}
                  onFocus={() => setShowHelp(true)}
                  onBlur={() => setShowHelp(false)}
                  disabled={isSubmitting}
                >
                  ?
                </button>
                {showHelp && (
                  <div className="absolute right-0 top-8 z-50 w-80 max-h-64 overflow-y-auto bg-white border border-gray-200 rounded-lg shadow-lg p-3">
                    {generateHelpNodes(helpData)}
                  </div>
                )}
              </div>
            )}
          </div>
          <textarea
            id="configuration"
            rows={8}
            className={textareaCls}
            value={configuration}
            onChange={(e) => setConfiguration(e.target.value)}
            disabled={isSubmitting}
            placeholder={exampleConfigMap[connectionType] || "Enter your YAML configuration here..."}
          />
          {connectionType && (
            <p className="mt-1 text-xs text-gray-400">
              Hover over <strong>?</strong> to see configuration help
            </p>
          )}
        </div>

        {/* File upload */}
        <div className="flex items-center gap-3">
          <input type="file" accept=".yaml,.yml,.txt" onChange={handleFileUpload} ref={fileInputRef} className="hidden" disabled={isSubmitting} />
          <button
            type="button"
            className="px-3 py-1.5 text-sm border border-gray-300 rounded bg-white hover:bg-gray-100 disabled:cursor-not-allowed"
            onClick={() => fileInputRef.current.click()}
            disabled={isSubmitting}
          >
            Choose File
          </button>
          <span className="text-gray-500 text-sm">{selectedFile ? selectedFile.name : "No file chosen"}</span>
        </div>

        {/* Streamer config */}
        <div>
          <label className="block mb-1.5 text-sm font-medium text-gray-800" htmlFor="streamer-config">
            Streamer Configuration:
          </label>
          <textarea
            id="streamer-config"
            rows={7}
            className={textareaCls}
            value={streamerConfig}
            onChange={(e) => setStreamerConfig(e.target.value)}
            disabled={isSubmitting}
            placeholder="Streamer configuration..."
          />
        </div>

        {/* Streaming mode toggle */}
        <div>
          <label className="block mb-2 text-sm font-medium text-gray-800">Streaming Mode:</label>
          <div className={`relative inline-grid grid-cols-2 items-center p-1 rounded-full bg-gray-200 w-60 select-none ${isSubmitting ? "opacity-80 pointer-events-none" : ""}`}>
            <span
              className="absolute top-1 left-1 w-[calc(50%-4px)] h-[calc(100%-8px)] rounded-full bg-[#C41230] transition-transform duration-[220ms] z-0"
              style={{ transform: `translateX(${isPolling ? "0%" : "100%"})` }}
            />
            <button type="button" className={`relative z-10 border-0 bg-transparent rounded-full py-2 px-3 text-sm font-semibold cursor-pointer transition-colors ${isPolling ? "text-white" : "text-gray-700"}`} disabled={isSubmitting} onClick={() => setIsPolling(true)}>
              Polling
            </button>
            <button type="button" className={`relative z-10 border-0 bg-transparent rounded-full py-2 px-3 text-sm font-semibold transition-colors ${!isPolling ? "text-white" : "text-gray-700"} disabled:opacity-45 disabled:cursor-not-allowed`} disabled={!supportsCallback || isSubmitting} onClick={() => setIsPolling(false)} title={!supportsCallback ? "Callback not supported" : undefined}>
              Callback
            </button>
          </div>
          {!supportsCallback && connectionType && (
            <p className="mt-1.5 text-xs text-gray-500 font-mono">This adapter does not support callbacks. Polling is required.</p>
          )}
        </div>

        {/* Polling rate */}
        {isPolling && connectionType && (
          <div>
            <label className="block mb-1.5 text-sm font-medium text-gray-800" htmlFor="polling-hz">Polling Rate (Hz):</label>
            <input id="polling-hz" type="number" min="1" step="1" className={inputCls} value={pollingRateHz} disabled={isSubmitting} onChange={(e) => setPollingRateHz(e.target.value)} />
          </div>
        )}

        {/* Status / errors */}
        {isValidating && <div className="bg-yellow-100 text-yellow-800 text-sm italic text-center py-1 px-2 rounded">Validating configuration…</div>}
        {step && <div className="bg-yellow-100 text-yellow-800 text-sm italic text-center py-1 px-2 rounded">{step}</div>}
        {validationWarnings.length > 0 && (
          <div className="bg-yellow-50 border border-yellow-200 rounded p-3">
            <span className="text-yellow-800 font-semibold block mb-2 text-sm">Warnings:</span>
            <ul className="list-disc list-inside space-y-1">
              {validationWarnings.map((w, i) => <li key={i} className="text-yellow-700 text-sm">{w}</li>)}
            </ul>
          </div>
        )}
        {validationError && (
          <div className="bg-red-50 border border-red-200 rounded p-3">
            <span className="text-red-800 font-semibold block mb-2 text-sm">Error:</span>
            <pre className="text-red-800 text-xs font-mono whitespace-pre-wrap m-0">{validationError}</pre>
          </div>
        )}

        {/* Actions */}
        <div className="flex gap-2 justify-evenly pt-1">
          <button type="button" className="flex-none px-4 py-2 text-sm rounded min-w-[120px] bg-gray-200 text-gray-800 hover:bg-gray-300 disabled:cursor-not-allowed" onClick={handleClose} disabled={isSubmitting}>
            Cancel
          </button>
          <button type="button" className="flex-none px-4 py-2 text-sm rounded min-w-[120px] bg-green-600 text-white hover:bg-green-700 disabled:bg-green-300 disabled:cursor-not-allowed" onClick={handleSave} disabled={!canSave}>
            {isSubmitting ? "Saving…" : "Save"}
          </button>
        </div>

      </div>
    </Modal>
  );
}
