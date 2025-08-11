import React, {
  useState,
  useRef,
  useEffect,
  useCallback,
  useMemo,
} from "react";
import Modal from "./Modal";
import { ConnectionManager } from "./ConnectionManager";
import { callConfig } from "../api";

const TOPIC_FAMILIES = [
  { label: "Key-Value (kv)", value: "kv" },
  { label: "Blob", value: "blob" },
  { label: "Historian", value: "historian" },
];

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

const parseServerError = (error) => {
  try {
    const err = (error?.message || error?.toString() || "").toLowerCase();
    if (
      err.includes("mqtt broker unreachable") ||
      err.includes("mqtt connection failed") ||
      err.includes("connection timeout")
    ) {
      return error.message || err;
    }
    if (err.includes("validation error")) {
      const m = err.match(/validation failed for ([^:]+)/i);
      if (m) return `Configuration error: ${m[1].trim()}`;
    }
    if (err.includes("missing")) return err;
    if (err.includes("schema error"))
      return "Configuration schema error. Please ensure all required fields are correct.";
    return (
      err
        .replace(/\{.*?\}/g, "")
        .replace(/\\n/g, " ")
        .replace(/error:\s*/g, "")
        .trim() || "Configuration validation failed"
    );
  } catch {
    return "Configuration validation failed. Please check your settings.";
  }
};

const makeDefaultMqttConfig = () => `mqtt:
  broker_address: 128.237.92.30
  broker_port: 1883
  enterprise: Mill-19-test
  site: HAAS-UMC750
  username: admin
  password: password
  tls_enabled: False
  debug: True

`;

export default function ConnectionModal({
  isOpen,
  onClose,
  onSave,
  initialData = {},
}) {
  const [connectionType, setConnectionType] = useState("");
  const [topicFamily, setTopicFamily] = useState("");
  const [configuration, setConfiguration] = useState("");
  const [mqttConfig, setMqttConfig] = useState(makeDefaultMqttConfig());
  const [adapters, setAdapters] = useState([]);

  const [isEditingMqtt, setIsEditingMqtt] = useState(false);
  const [step, setStep] = useState("");
  const [validationError, setValidationError] = useState("");
  const [selectedFile, setSelectedFile] = useState(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isValidating, setIsValidating] = useState(false);
  const [validationWarnings, setValidationWarnings] = useState([]);
  const fileInputRef = useRef(null);
  const [showHelp, setShowHelp] = useState(false);
  const [activeConnectionId, setActiveConnectionId] = useState(null);
  const [detectedAdapter, setDetectedAdapter] = useState(null);

  // Track the last payload we validated to avoid re-validating identical text
  const lastValidatedRef = useRef("");

  // Fetch adapter metadata
  useEffect(() => {
    async function fetchAdapters() {
      try {
        const res = await fetch("/config/adapters");
        if (!res.ok) throw new Error("Failed to fetch adapters");
        const data = await res.json();
        setAdapters(data);
      } catch (e) {
        console.error(e);
      }
    }
    fetchAdapters();
  }, []);

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
    },
    [adapters]
  );

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
      const meta = adapters.find((a) => a.name === t);
      setTopicFamily(meta?.recommendedTopicFamily || "");
      setMqttConfig(makeDefaultMqttConfig());
      setValidationError("");
      lastValidatedRef.current = ""; // force a fresh validate on type change
    },
    [adapters, exampleConfigMap]
  );

  const handleTopicChange = (e) => {
    setTopicFamily(e.target.value);
    lastValidatedRef.current = ""; // ensure revalidate
  };
  const handleConfigurationChange = (e) => {
    setConfiguration(e.target.value);
    // no need to touch lastValidatedRef: the effect compares current payload
  };
  const handleMqttConfigChange = (e) => {
    setMqttConfig(e.target.value);
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
      await callConfig(`/config/disconnect/${activeConnectionId}`);
      setActiveConnectionId(null);
    }
    onClose();
  }, [isSubmitting, activeConnectionId, onClose]);

  useEffect(() => {
    if (!isOpen) return;
    setConnectionType(initialData.type || "");
    setTopicFamily(initialData.topicFamily || "");
    setConfiguration(initialData.configuration || "");
    setMqttConfig(initialData.mqttConfig || makeDefaultMqttConfig());
    setIsEditingMqtt(false);
    setStep("");
    setValidationError("");
    setSelectedFile(null);
    setIsSubmitting(false);
    lastValidatedRef.current = ""; // reset on open
    if (fileInputRef.current) fileInputRef.current.value = "";
  }, [isOpen, initialData]);

  const validateYAMLFormats = useCallback(
    (yaml) => (yaml.trim() ? [] : ["Configuration cannot be empty"]),
    []
  );

  const validateConfiguration = useCallback(async () => {
    setValidationError("");
    setValidationWarnings([]);
    setIsValidating(true);
    try {
      if (!connectionType) throw new Error("Select connection type");
      if (!topicFamily) throw new Error("Select topic family");
      if (!configuration.trim()) throw new Error("Enter YAML config");
      const errs = validateYAMLFormats(configuration);
      if (errs.length) throw new Error(errs.join("\n"));

      const wrapped = wrapConfigForProtocol(connectionType, configuration);
      const combined =
        `topic_family: ${topicFamily}\n\n${wrapped}` +
        (mqttConfig.trim() ? `\n\n${mqttConfig.trim()}` : "");

      const resp = await callConfig("/config/validate", {
        text: combined,
        topicFamily,
      });

      setDetectedAdapter(resp.detected_adapter || null);

      if (resp.warnings?.length) setValidationWarnings(resp.warnings);
      if (resp.errors?.length) {
        setValidationError(resp.errors.join("\n"));
      }

      return resp;
    } catch (e) {
      setValidationError(parseServerError(e));
      return { valid: false };
    } finally {
      setIsValidating(false);
    }
  }, [
    connectionType,
    topicFamily,
    configuration,
    mqttConfig,
    validateYAMLFormats,
    wrapConfigForProtocol,
  ]);

  // Debounced validate on changes — but only when the combined payload actually changed
  useEffect(() => {
    if (!isOpen || !connectionType || !topicFamily || !configuration.trim()) {
      setValidationError("");
      return;
    }

    const id = setTimeout(() => {
      const wrapped = wrapConfigForProtocol(connectionType, configuration);
      const combined =
        `topic_family: ${topicFamily}\n\n${wrapped}` +
        (mqttConfig.trim() ? `\n\n${mqttConfig.trim()}` : "");

      if (combined !== lastValidatedRef.current) {
        lastValidatedRef.current = combined;
        validateConfiguration();
      }
    }, 800);

    return () => clearTimeout(id);
  }, [
    isOpen,
    connectionType,
    topicFamily,
    configuration,
    mqttConfig,
    validateConfiguration,
    wrapConfigForProtocol,
  ]);

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

      const wrapped = wrapConfigForProtocol(connectionType, configuration);
      const combined =
        `topic_family: ${topicFamily}\n\n${wrapped}` +
        (mqttConfig.trim() ? `\n\n${mqttConfig.trim()}` : "");

      const adapterKey = validation.detected_adapter || detectedAdapter || null;

      if (initialData.id) {
        // EDIT
        const updated = {
          ...initialData,
          type: connectionType,
          topicFamily,
          configuration,
          mqttConfig,
          adapterKey,
          yaml: combined,
          updatedAt: new Date().toISOString(),
        };
        ConnectionManager.saveConnection(initialData.id, updated);
        onSave(updated);
        onClose();
        return;
      }

      // NEW
      const id = crypto.randomUUID();
      setActiveConnectionId(id);
      setStep("Connecting to adapter...");

      await callConfig(`/config/connect/${id}`, {
        text: combined,
        topicFamily,
      });

      const saved = {
        id,
        type: connectionType,
        topicFamily,
        configuration,
        mqttConfig,
        name: connectionType,
        savedAt: new Date().toISOString(),
        adapterKey,
        yaml: combined,
      };

      ConnectionManager.saveConnection(id, saved);
      onSave(saved);
      onClose();
    } catch (e) {
      setValidationError(parseServerError(e));
    } finally {
      setStep("");
      setIsSubmitting(false);
    }
  }, [
    isSubmitting,
    validateConfiguration,
    configuration,
    connectionType,
    topicFamily,
    mqttConfig,
    initialData,
    onSave,
    onClose,
    detectedAdapter,
    wrapConfigForProtocol,
  ]);

  const canSave = Boolean(
    connectionType &&
      topicFamily &&
      configuration.trim() &&
      !validationError &&
      !isValidating
  );
  const helpData = configHelpMap[connectionType];
  const isEditMode = Boolean(initialData.id);

  return (
    <Modal
      isOpen={isOpen}
      onClose={handleClose}
      title={isEditMode ? "Edit Adapter" : "New Adapter"}
    >
      <div className="modal-content configuration-modal">
        <div className="form-group">
          <label htmlFor="connection-type">Connection Type:</label>
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
          <label>Topic Family:</label>
          <div className="segmented-control">
            {TOPIC_FAMILIES.map(({ label, value }) => (
              <React.Fragment key={value}>
                <input
                  type="radio"
                  id={`tf-${value}`}
                  name="topicFamily"
                  value={value}
                  checked={topicFamily === value}
                  onChange={handleTopicChange}
                  disabled={isSubmitting}
                />
                <label htmlFor={`tf-${value}`}>{label}</label>
              </React.Fragment>
            ))}
          </div>
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
          <label htmlFor="mqtt-config">MQTT Broker Configuration:</label>
          <textarea
            id="mqtt-config"
            rows={5}
            className={`form-textarea ${!isEditingMqtt ? "disabled" : ""}`}
            value={mqttConfig}
            onChange={handleMqttConfigChange}
            disabled={isSubmitting || !isEditingMqtt}
            readOnly={!isEditingMqtt}
            placeholder="MQTT broker configuration..."
          />
          <button
            type="button"
            className="edit-mqtt-button"
            onClick={() => setIsEditingMqtt(!isEditingMqtt)}
            disabled={isSubmitting}
          >
            {isEditingMqtt ? "Done Editing" : "Edit Broker"}
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
