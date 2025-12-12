import React, {
  useState,
  useRef,
  useEffect,
  useCallback,
  useMemo,
} from "react";
import Modal from "./Modal";
import { ConnectionManager } from "./ConnectionManager";
import { makeDefaultStreamerConfig } from "../data/defaults";
import { getConnCtr, setConnCtr } from "../state/conn_ctr";
import { connectConnection, disconnectConnection, fetchAdapters } from "../api";
import { validateAdapterConfig, validateStreamerConfig } from "../api";


const generateNestedHelpText = (helpData, indentLevel = 0) => {
  if (!helpData || typeof helpData !== "string") return null;

  const lines = helpData.split("\n");
  return lines
    .map((line, index) => {
      const colonIndex = line.indexOf(":");

      // Check indentation level by counting leading spaces
      const leadingSpaces = line.match(/^(\s*)/)[1].length;
      const currentIndentLevel = Math.floor(leadingSpaces / 2);
      const indentStyle = {
        marginLeft: (indentLevel + currentIndentLevel) * 16,
      };

      if (colonIndex > -1) {
        const key = line.substring(0, colonIndex).trim();
        const value = line.substring(colonIndex + 1).trim();

        // Check if this looks like a section header (has nested content after it)
        const nextLine = lines[index + 1];
        const isGroupHeader = nextLine && nextLine.startsWith("  ") && !value;

        if (isGroupHeader) {
          // This is a group header
          return (
            <div key={index} className="help-field-group" style={indentStyle}>
              <div
                className="help-group-name"
                style={{ fontWeight: "bold", marginTop: 8 }}
              >
                {key}:
              </div>
            </div>
          );
        } else if (value) {
          // This is a regular field with a value
          return (
            <div key={index} className="help-field-line" style={indentStyle}>
              <span className="help-field-name">{key}:</span>{" "}
              <span className="help-field-desc">{value}</span>
            </div>
          );
        }
      }

      // Lines without colons or empty lines
      if (line.trim() === "") {
        return null;
      }

      // Section headers without colons
      if (!line.startsWith(" ") && line.trim()) {
        return (
          <div
            key={index}
            className="help-field-group"
            style={{ marginLeft: indentLevel * 16 }}
          >
            <div
              className="help-group-name"
              style={{ fontWeight: "bold", marginTop: 8 }}
            >
              {line.trim()}
            </div>
          </div>
        );
      }

      return null;
    })
    .filter(Boolean); // Filter out null values
};

export default function ConnectionModal({
  isOpen,
  onClose,
  onSave,
  initialData = {},
}) {
  const [connectionType, setConnectionType] = useState("");
  const [configuration, setConfiguration] = useState("");
  const [streamerConfig, setStreamerConfig] = useState(makeDefaultStreamerConfig());
  const [adapters, setAdapters] = useState([]);
  const [isEditingStreamer, setIsEditingStreamer] = useState(false);
  const [step, setStep] = useState("");
  const [validationError, setValidationError] = useState("");
  const [selectedFile, setSelectedFile] = useState(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isValidating, setIsValidating] = useState(false);
  const [validationWarnings, setValidationWarnings] = useState([]);
  const fileInputRef = useRef(null);
  const [showHelp, setShowHelp] = useState(false);
  const [activeConnectionId, setActiveConnectionId] = useState(null);

  // Track the last payload we validated to avoid re-validating identical text
  const lastValidatedRef = useRef("");

  const connectionTypes = adapters.map((a) => a.name);

  const configHelpMap = useMemo(
    () =>
      adapters.reduce((m, a) => {
        m[a.name] = a.configHelpText || "";
        return m;
      }, {}),
    [adapters]
  );

  const exampleInnerByName = useMemo(
    () =>
      adapters.reduce((m, a) => {
        m[a.name] = a.configExample?.configuration?.trim() || "";
        return m;
      }, {}),
    [adapters]
  );

  // Smarter wrapper that checks the config example format
  const wrapConfigForProtocol = useCallback(
    (connectionType, yaml) => {
      const adapter = adapters.find((a) => a.name === connectionType);
      if (!adapter) return (yaml || "").trim();

      const key = adapter.key;
      const configExample = adapter.configExample?.raw || {};

      // If example shows the key at top level, we should wrap
      const needsWrapping = key && key in configExample;

      if (!needsWrapping) {
        // Direct format (e.g., ROS/LocalFiles-style examples)
        return (yaml || "").trim();
      }

      // Keyed format (e.g., MTConnect-style examples)
      const inner = (yaml || "").trim();
      if (!key) return inner;
      if (inner.startsWith(`${key}:`)) return inner;

      const indented = inner
        .split("\n")
        .map((l) => `  ${l}`)
        .join("\n");
      return `${key}:\n${indented}`;
    }, [adapters]);

  // Build exampleConfigMap using the stable wrapper
  const exampleConfigMap = useMemo(() => {
    return adapters.reduce((m, a) => {
      m[a.name] = wrapConfigForProtocol(a.name, exampleInnerByName[a.name]);
      return m;
    }, {});
  }, [adapters, exampleInnerByName, wrapConfigForProtocol]);

  // Handlers
  const handleTypeChange = useCallback(
    (e) => {
      const t = e.target.value;
      setConnectionType(t);
      setConfiguration(exampleConfigMap[t] || "");
      setStreamerConfig(makeDefaultStreamerConfig());
      setValidationError("");
      lastValidatedRef.current = ""; // force a fresh validate on type change
    }, [exampleConfigMap]);

  const handleConfigurationChange = (e) => {
    setConfiguration(e.target.value);
    // no need to touch lastValidatedRef: the effect compares current payload
  };

  const handleStreamerConfigChange = (e) => {
    setStreamerConfig(e.target.value);
    // no need to touch lastValidatedRef: the effect compares current payload
  };

  const handleFileUpload = useCallback((e) => {
    const f = e.target.files?.[0];
    if (!f) return setSelectedFile(null);
    setSelectedFile(f);
    const r = new FileReader();
    r.onload = ({ target }) =>
      typeof target.result === "string" && setConfiguration(target.result);
    r.readAsText(f);
  }, []);

  const handleClose = useCallback(async () => {
    if (isSubmitting) return;
    if (activeConnectionId) {
      const is_disconnected = await disconnectConnection(activeConnectionId);
      if (is_disconnected) {
        setActiveConnectionId(null);
      } else {
        console.error("Failed to disconnect active connection");
      }
    }
    onClose();
  }, [isSubmitting, activeConnectionId, onClose]);

  const validateYAMLFormats = useCallback(
    (yaml) => (yaml.trim() ? [] : ["Configuration cannot be empty"]),
    []);

  const validateConfiguration = useCallback(async () => {
    setValidationError("");
    setValidationWarnings([]);
    setIsValidating(true);
    try {
      if (!connectionType) throw new Error("Select connection type");
      if (!configuration.trim()) throw new Error("Enter YAML config");
      const errs = validateYAMLFormats(configuration);
      if (errs.length) throw new Error(errs.join("\n"));

      const adp_valid = await validateAdapterConfig(connectionType, configuration);
      const streamer_valid = await validateStreamerConfig(streamerConfig);
      const is_valid = adp_valid && streamer_valid;

      if (!is_valid) {
        if (!adp_valid) {
          const error_msg = "Data Adapter configuration validation error.\n";
          setValidationError(error_msg);
        }
        if (!streamer_valid) {
          const error_msg = "Streamer configuration validation error.\n";
          setValidationError((prev) => prev + error_msg);
        }
      }

      console.log("ADP VALID:", adp_valid, "STREAMER VALID:", streamer_valid);
      return { valid: is_valid };
    } catch (e) {
      console.error("Validation error:", e);
      return { valid: false };
    } finally {
      setIsValidating(false);
    }
  }, [connectionType, configuration, streamerConfig, validateYAMLFormats]);

  const handleSave = useCallback(async () => {
    if (isSubmitting) return;
    setIsSubmitting(true);
    setValidationError("");

    try {
      const validation = await validateConfiguration();
      if (!validation?.valid) {
        setIsSubmitting(false);
        return;
      }

      if (initialData.id) {
        // EDIT
        const updated = {
          ...initialData,
          adapter: connectionType,
          adapterConfig: configuration,
          streamerConfig: streamerConfig,
          updatedAt: new Date().toISOString(),
        };
        ConnectionManager.saveConnection(initialData.id, updated);
        onSave(updated);
        onClose();
        return;
      }

      // NEW
      const id = getConnCtr() + 1;
      setConnCtr(id);
      setActiveConnectionId(id);
      setStep("Connecting to adapter...");

      console.log("HEYYO! Connecting adapter with ID:", id);

      await connectConnection(
        id,
        connectionType,
        configuration,
        streamerConfig
      );

      const saved = {
        id,
        adapter: connectionType,
        adapterConfig: configuration,
        streamerConfig: streamerConfig,
        savedAt: new Date().toISOString(),
      };

      ConnectionManager.saveConnection(id, saved);
      onSave(saved);
      onClose();
    } catch (e) {
    } finally {
      setStep("");
      setIsSubmitting(false);
    }
  }, [isSubmitting, validateConfiguration, configuration, connectionType, streamerConfig, initialData, onSave, onClose]);

  const canSave = Boolean(
    connectionType &&
    configuration.trim() &&
    streamerConfig.trim() &&
    !validationError &&
    !isValidating
  );
  const helpData = configHelpMap[connectionType];
  const isEditMode = Boolean(initialData.id);

  // Fetch adapter metadata
  useEffect(() => {
    fetchAdapters()
      .then(data => { setAdapters(data); })
      .catch(err => { console.error(err); });
  }, []);

  useEffect(() => {
    if (!isOpen) return;

    console.log("Modal opened with initial data:", initialData);

    setConnectionType(initialData.adapter || "");
    setConfiguration(initialData.adapterConfig || "");
    setStreamerConfig(initialData.streamerConfig || makeDefaultStreamerConfig());
    setIsEditingStreamer(false);
    setStep("");
    setValidationError("");
    setSelectedFile(null);
    setIsSubmitting(false);
    lastValidatedRef.current = ""; // reset on open
    if (fileInputRef.current) fileInputRef.current.value = "";
  }, [isOpen, initialData]);

  // Debounced validate on changes — but only when the combined payload actually changed
  useEffect(() => {
    if (!isOpen || !connectionType || !configuration.trim()) {
      setValidationError("");
      return;
    }

    const id = setTimeout(() => {
      const combined =
        `adapter: ${connectionType}\n\n${configuration}` +
        (streamerConfig.trim() ? `\n\n${streamerConfig.trim()}` : "");

      if (combined !== lastValidatedRef.current) {
        lastValidatedRef.current = combined;
        validateConfiguration();
      }
    }, 800);

    return () => clearTimeout(id);
  }, [isOpen, connectionType, configuration, streamerConfig, validateConfiguration]);

  return (
    <Modal
      isOpen={isOpen}
      onClose={handleClose}
      title={isEditMode ? "Edit Adapter" : "New Adapter"}
    >
      <div className="modal-content configuration-modal">
        <div className="form-group">
          <label htmlFor="connection-type">Data Adapter:</label>
          <select
            id="connection-type"
            value={connectionType}
            onChange={handleTypeChange}
            className="form-select"
            disabled={isSubmitting}
          >
            <option value="">Select a type</option>
            {connectionTypes.map((type) => (
              <option key={type} value={type}>
                {type}
              </option>
            ))}
          </select>
        </div>

        <div className="form-group">
          <label htmlFor="configuration">Configuration (YAML):</label>
          <div className="textarea-container">
            <textarea
              id="configuration"
              rows={10}
              className="form-textarea"
              value={configuration}
              onChange={handleConfigurationChange}
              disabled={isSubmitting}
              placeholder={
                exampleConfigMap[connectionType] ||
                "Enter your YAML configuration here..."
              }
            />
            {connectionType && (
              <span className="help-text">
                Hover over <strong>?</strong> to see configuration help
              </span>
            )}

            {helpData && (
              <button
                type="button"
                className="help-button-inside"
                onMouseEnter={() => setShowHelp(true)}
                onMouseLeave={() => setShowHelp(false)}
                onFocus={() => setShowHelp(true)}
                onBlur={() => setShowHelp(false)}
                disabled={isSubmitting}
              >
                ?
              </button>
            )}
            {showHelp && helpData && (
              <div className="help-tooltip">
                <div className="help-content">
                  {generateNestedHelpText(helpData)}
                </div>
              </div>
            )}
          </div>
        </div>

        <div className="file-upload-wrapper">
          <input
            type="file"
            accept=".yaml,.yml,.txt"
            onChange={handleFileUpload}
            ref={fileInputRef}
            style={{ display: "none" }}
            disabled={isSubmitting}
          />
          <button
            type="button"
            className="small-button file-upload-button"
            onClick={() => fileInputRef.current.click()}
            disabled={isSubmitting}
          >
            Choose File
          </button>
          <span className="file-name">
            {selectedFile ? selectedFile.name : "No file chosen"}
          </span>
        </div>

        <div className="form-group">
          <label htmlFor="streamer-config">Streamer Configuration:</label>
          <textarea
            id="streamer-config"
            rows={5}
            className={`form-textarea ${!isEditingStreamer ? "disabled" : ""}`}
            value={streamerConfig}
            onChange={handleStreamerConfigChange}
            disabled={isSubmitting || !isEditingStreamer}
            readOnly={!isEditingStreamer}
            placeholder="Streamer configuration..."
          />
          <button
            type="button"
            className="edit-streamer-button"
            onClick={() => setIsEditingStreamer(!isEditingStreamer)}
            disabled={isSubmitting}
          >
            {isEditingStreamer ? "Done Editing" : "Edit Broker"}
          </button>
        </div>

        {isValidating && (
          <div className="info-message">Validating configuration...</div>
        )}
        {step && <div className="info-message">{step}</div>}
        {validationWarnings.length > 0 && (
          <div className="warning-message-box">
            <span className="warning-title">Warnings:</span>
            <ul className="warning-list">
              {validationWarnings.map((warning, i) => (
                <li key={i}>{warning}</li>
              ))}
            </ul>
          </div>
        )}
        {validationError && (
          <div className="error-message-box">
            <span className="error-title">Error:</span>
            <pre className="error-details">{validationError}</pre>
          </div>
        )}

        <div className="button-group small">
          <button
            type="button"
            className="small-button cancel-button"
            onClick={handleClose}
            disabled={isSubmitting}
          >
            Cancel
          </button>
          <button
            type="button"
            className="small-button save-button"
            onClick={handleSave}
            disabled={!canSave || isSubmitting}
          >
            {isSubmitting ? "Saving..." : "Save"}
          </button>
        </div>
      </div>
    </Modal>
  );
}
