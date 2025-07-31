import React, { useState, useRef, useEffect, useCallback } from "react";
import Modal from "./Modal";
import { ConnectionManager } from "./ConnectionManager";
import { callConfig } from "../api";
import { EXAMPLE_CONFIGS } from "../config/exampleConfig";
export const CONNECTION_TYPES = Object.keys(EXAMPLE_CONFIGS);

// MQTT topic classifications for data routing strategy
const TOPIC_FAMILIES = [
  { label: "Key-Value (kv)", value: "kv" },
  { label: "Blob", value: "blob" },
  { label: "Historian", value: "historian" },
];

// Configuration help data (matching Pydantic models)
const CONFIG_HELP = {
  MTConnect: {
    fields: [
      { name: "agent_ip", description: "Agent IP address" },
      { name: "agent_url", description: "Agent URL" },
      { name: "stream_rate", description: "Polling rate in seconds (>0)" },
      { name: "device_name", description: "Optional device identifier" },
      { name: "trial_id", description: "Optional trial ID" },
    ],
  },
  ROS: {
    fields: [
      { name: "trial_id", description: "Trial/experiment identifier" },
      { name: "set_ros_callback", description: "Enable ROS callback mode" },
      { name: "devices", description: "Named device configurations" },
      {
        name: "namespace",
        description: "ROS namespace for the device",
      },
      {
        name: "rostopics",
        description: "List of ROS topics to subscribe to",
      },
      {
        name: "attributes-description",
        description: "Device description",
      },
      {
        name: "attributes-type",
        description: "Device type (e.g., Robot)",
      },
      { name: "attributes-version", description: "Device version" },
    ],
  },
  "ROS Files": {
    fields: [
      { name: "trial_id", description: "Trial/experiment identifier" },
      { name: "set_ros_callback", description: "Enable ROS callback mode" },
      { name: "devices", description: "Named device configurations" },
      {
        name: "namespace",
        description: "ROS namespace for the device",
      },
      {
        name: "rostopics",
        description:
          "List of ROS topics to subscribe to: [camera/color/image_raw]",
      },
      {
        name: "attributes-description",
        description: "Device description",
      },
      {
        name: "attributes-type",
        description: "Device type (e.g., Robot)",
      },
      { name: "attributes-version", description: "Device version" },
    ],
  },
  "Local Files": {
    fields: [
      { name: "watch_dir", description: "Directories to monitor" },
      { name: "wait_before_read", description: "Seconds after file creation" },
      { name: "buffer_size", description: "File buffer count" },
      { name: "name", description: "System name" },
      { name: "trial_id", description: "Trial/experiment ID of the system" },
    ],
  },
  "MQTT-ADP": {
    fields: [
      { name: "trial_id", description: "Trial/experiment identifier" },
      { name: "broker_address", description: "Adapter host/IP" },
      { name: "broker_port", description: "Adapter port" },
      { name: "username", description: "Auth username" },
      { name: "password", description: "Auth password" },
      { name: "trial_id", description: "Trial/experiment identifier" },
      { name: "queue_size", description: "Max number of messages to buffer" },
      { name: "topics", description: "List of topic configurations" },
      { name: "topic", description: "Topic name" },
      { name: "component_id", description: "Component identifier" },
    ],
  },
};

// Generate help text
const generateHelpText = (helpData) => {
  return helpData.fields.map((field, index) => (
    <div key={index} className="help-field-line">
      <span className="help-field-name">{field.name}:</span>
      <span className="help-field-desc"> {field.description}</span>
    </div>
  ));
};

// Factory function for default MQTT broker configuration
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
  isEditing = false,
}) {
  // Core form data state
  const [connectionType, setConnectionType] = useState("");
  const [topicFamily, setTopicFamily] = useState("");
  const [configuration, setConfiguration] = useState("");
  const [mqttConfig, setMqttConfig] = useState(makeDefaultMqttConfig());

  // UI interaction state
  const [isEditingMqtt, setIsEditingMqtt] = useState(false);
  const [step, setStep] = useState("");
  const [validationError, setValidationError] = useState("");
  const [selectedFile, setSelectedFile] = useState(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isValidating, setIsValidating] = useState(false);
  const fileInputRef = useRef(null);
  const [showHelp, setShowHelp] = useState(false);
  const [activeConnectionId, setActiveConnectionId] = useState(null);

  // Handle connection type selection and auto-populate configuration
  const handleTypeChange = useCallback((e) => {
    const selectedType = e.target.value;
    setConnectionType(selectedType);
    setConfiguration(EXAMPLE_CONFIGS[selectedType]?.configuration || "");
    setTopicFamily(
      selectedType === "MTConnect"
        ? "historian"
        : selectedType === "Local Files"
        ? "blob"
        : ""
    );
    setMqttConfig(makeDefaultMqttConfig());
    setValidationError("");
  }, []);

  // Handle topic family radio button selection
  const handleTopicChange = useCallback((e) => {
    setTopicFamily(e.target.value);
    setValidationError("");
  }, []);

  // Handle main configuration text area changes
  const handleConfigurationChange = useCallback((e) => {
    setConfiguration(e.target.value);
    setValidationError("");
  }, []);

  // Handle MQTT broker configuration changes
  const handleMqttConfigChange = useCallback((e) => {
    setMqttConfig(e.target.value);
    setValidationError("");
  }, []);

  // Handle configuration file uploads and read content
  const handleFileUpload = useCallback((e) => {
    const file = e.target.files?.[0];
    if (!file) {
      setSelectedFile(null);
      return;
    }
    setSelectedFile(file);
    const reader = new FileReader();
    reader.onload = ({ target }) =>
      typeof target.result === "string" &&
      setConfiguration(
        target.result.match(/^\w+:\s*\n/)
          ? target.result.replace(/^\w+:\s*\n/, "").replace(/^  /gm, "")
          : target.result
      );
    reader.onerror = () => setValidationError("Failed to read file");
    reader.readAsText(file);
  }, []);

  // Trigger hidden file input programmatically
  const handleFileButtonClick = useCallback(
    () => fileInputRef.current?.click(),
    []
  );

  // Handle modal close with submission state protection
  const handleClose = useCallback(async () => {
    if (isSubmitting) return;
    if (activeConnectionId) {
      try {
        await callConfig(`/config/disconnect/${activeConnectionId}`, {});
        setActiveConnectionId(null);
      } catch (error) {
        console.warn("Failed to disconnect adapter on modal close:", error);
      }
    }
    onClose();
  }, [isSubmitting, onClose, activeConnectionId]);

  // Reset form state when modal opens or when editing different connection
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
    if (fileInputRef.current) fileInputRef.current.value = "";
  }, [isOpen, initialData]);

  // Client-side YAML format validation with regex patterns
  const validateYAMLFormats = useCallback((yaml) => {
    if (!yaml?.trim()) return ["Configuration cannot be empty"];

    const errs = [];
    const ipRx =
      /^(25[0-5]|2[0-4]\d|[01]?\d\d?)(\.(25[0-5]|2[0-4]\d|[01]?\d\d?)){3}$/;
    const domainRx =
      /^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?)*$/;
    const urlRx = /^(https?:\/\/|mqtts?:\/\/|opc\.tcp:\/\/)[\w\d./:?=#-]+$/i;
    const portRx = /^\d{1,5}$/;

    yaml.split("\n").forEach((line, index) => {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#") || !trimmed.includes(":")) return;

      const [key, ...rest] = trimmed.split(":");
      const cleanKey = key.trim().toLowerCase();
      const value = rest.join(":").trim().replace(/['"]/g, "");

      if (!value) return;

      if (
        (cleanKey === "broker_address" ||
          cleanKey.endsWith("_ip") ||
          cleanKey === "agent_ip") &&
        !ipRx.test(value) &&
        !domainRx.test(value) &&
        !value.includes("://")
      ) {
        errs.push(
          `Line ${index + 1}: "${cleanKey}" must be a valid IP or domain`
        );
      }

      if (
        (cleanKey.includes("url") || cleanKey === "endpoint") &&
        !urlRx.test(value)
      ) {
        errs.push(`Line ${index + 1}: "${cleanKey}" has invalid URL format`);
      }

      if (cleanKey === "broker_port" || cleanKey.includes("port")) {
        const portNum = parseInt(value, 10);
        if (!portRx.test(value) || portNum < 1 || portNum > 65535) {
          errs.push(
            `Line ${index + 1}: "${cleanKey}" must be a valid port (1-65535)`
          );
        }
      }
    });

    return errs;
  }, []);

  // Parse server error responses into user-friendly messages
  const parseServerError = useCallback((error) => {
    try {
      const errorStr = error.message || error.toString();

      if (
        errorStr.includes("MQTT Broker Unreachable:") ||
        errorStr.includes("MQTT Connection Failed:") ||
        errorStr.includes("Connection Timeout:")
      ) {
        return errorStr;
      }

      if (
        errorStr.includes("MQTT broker unreachable") ||
        errorStr.includes("broker unreachable")
      ) {
        return "MQTT Broker Unreachable: Cannot connect to the specified broker. Please verify the broker address and port.";
      }

      if (errorStr.includes("refused") && errorStr.includes("broker")) {
        return "MQTT Connection Failed: The broker refused the connection. Please check your credentials and broker settings.";
      }

      if (errorStr.includes("connection failed") && errorStr.includes("mqtt")) {
        return "MQTT Connection Failed: Unable to establish connection with the MQTT broker. Please verify your configuration.";
      }

      if (errorStr.includes('"detail"')) {
        try {
          const jsonMatch = errorStr.match(/\{.*\}/);
          if (jsonMatch) {
            const errorObj = JSON.parse(jsonMatch[0]);
            if (errorObj.detail) {
              if (errorObj.detail.includes("MQTT broker unreachable")) {
                return "MQTT Broker Unreachable: Cannot connect to the specified broker. Please verify the broker address and port.";
              }
              if (errorObj.detail.includes("MQTT connection failed")) {
                return "MQTT Connection Failed: The broker rejected the connection. Please check your broker configuration.";
              }
              return errorObj.detail;
            }
          }
        } catch (parseErr) {
          // Fall through to pattern matching if JSON parsing fails
        }
      }

      if (errorStr.includes("validation error")) {
        const match = errorStr.match(/validation error for ([^\\]+)/i);
        if (match) {
          return `Configuration error: ${match[1].trim()}`;
        }
      }

      if (errorStr.includes("missing")) {
        return "Required configuration field is missing. Please check your YAML syntax.";
      }

      if (errorStr.includes("input_type=dict")) {
        return "Configuration format error. Expected a valid YAML structure.";
      }

      if (errorStr.includes("topic_family")) {
        return "Topic family configuration error. Please verify your topic family selection matches your configuration.";
      }

      if (errorStr.includes("mqtt") && !errorStr.includes("broker")) {
        return "MQTT configuration error. Please check your broker settings (address, port, credentials).";
      }

      if (errorStr.includes("schema error")) {
        return "Configuration schema error. Please check that all required fields are present and properly formatted.";
      }

      return (
        errorStr
          .replace(/\{.*?\}/g, "")
          .replace(/\\n/g, " ")
          .replace(/Error:\s*/g, "")
          .trim() || "Configuration validation failed"
      );
    } catch (parseError) {
      return "Configuration validation failed. Please check your settings.";
    }
  }, []);

  // Comprehensive validation function (client-side + server-side)
  const validateConfiguration = useCallback(async () => {
    setValidationError("");
    setIsValidating(true);

    try {
      if (!connectionType) {
        throw new Error("Please select a connection type");
      }
      if (!topicFamily) {
        throw new Error("Please select a topic family");
      }
      if (!configuration.trim()) {
        throw new Error("Configuration YAML is required");
      }

      const configErrors = validateYAMLFormats(configuration);
      if (configErrors.length > 0) {
        throw new Error(`Configuration errors:\n${configErrors.join("\n")}`);
      }

      if (mqttConfig.trim()) {
        const mqttErrors = validateYAMLFormats(mqttConfig);
        if (mqttErrors.length > 0) {
          throw new Error(
            `MQTT configuration errors:\n${mqttErrors.join("\n")}`
          );
        }
      }

      const wrappedConfig = wrapConfigForProtocol(
        connectionType,
        configuration
      );

      const combinedConfig =
        `topic_family: ${topicFamily}\n\n${wrappedConfig}` +
        (mqttConfig.trim()
          ? `\n\n${mqttConfig.replace(
              "mqtt:",
              `mqtt:\n  topic_family: ${topicFamily}`
            )}`
          : "");

      await callConfig("/config/validate", {
        text: combinedConfig,
        topicFamily,
      });

      return true;
    } catch (err) {
      const userFriendlyError = parseServerError(err);
      setValidationError(userFriendlyError);
      return false;
    } finally {
      setIsValidating(false);
    }
  }, [
    connectionType,
    topicFamily,
    configuration,
    mqttConfig,
    validateYAMLFormats,
    parseServerError,
  ]);

  // Debounced validation effect - runs 800ms after user stops typing
  useEffect(() => {
    if (!isOpen || !connectionType || !topicFamily || !configuration.trim()) {
      setValidationError("");
      return;
    }

    const timeoutId = setTimeout(() => {
      validateConfiguration();
    }, 800);

    return () => clearTimeout(timeoutId);
  }, [
    configuration,
    mqttConfig,
    topicFamily,
    connectionType,
    isOpen,
    validateConfiguration,
  ]);

  const wrapConfigForProtocol = (connectionType, configuration) => {
    const protocolMapping = {
      ROS: "ros",
      "ROS Files": "ros_files",
      "Local Files": "file",
      MTConnect: "mtconnect",
      "MQTT-ADP": "mqtt",
    };

    const protocolKey = protocolMapping[connectionType];

    if (!protocolKey) {
      return configuration;
    }

    return `${protocolKey}:\n${configuration.replace(/^/gm, "  ")}`;
  };

  // Handle save operation with validation and server connection
  const handleSave = useCallback(async () => {
    if (isSubmitting) return;

    setIsSubmitting(true);
    setValidationError("");

    try {
      const isValid = await validateConfiguration();
      if (!isValid) {
        setIsSubmitting(false);
        return;
      }

      const wrappedConfig = wrapConfigForProtocol(
        connectionType,
        configuration
      );
      const combinedConfig =
        `topic_family: ${topicFamily}\n\n${wrappedConfig}` +
        (mqttConfig.trim()
          ? `\n\n${mqttConfig.replace(
              "mqtt:",
              `mqtt:\n  topic_family: ${topicFamily}`
            )}`
          : "");

      const isEditMode = initialData && initialData.id;

      if (isEditMode) {
        console.log("Edit mode: Updating existing connection", initialData.id);

        const updatedConnectionData = {
          id: initialData.id,
          type: connectionType,
          topicFamily,
          configuration,
          mqttConfig: mqttConfig.trim() ? mqttConfig : null,
          name:
            connectionType ||
            initialData.name ||
            `Connection ${initialData.id.slice(0, 8)}`,
          savedAt: initialData.savedAt,
          updatedAt: new Date().toISOString(),
        };

        ConnectionManager.saveConnection(initialData.id, updatedConnectionData);
        console.log("Updated connection:", updatedConnectionData.name);

        onSave(updatedConnectionData);
        onClose();
      } else {
        console.log("Create mode: Making new connection");

        const connectionId = crypto.randomUUID();
        setActiveConnectionId(connectionId);
        setStep("Connecting to adapter...");

        try {
          await callConfig(`/config/connect/${connectionId}`, {
            text: combinedConfig,
            topicFamily,
          });
        } catch (connectionError) {
          const errorMessage =
            connectionError.message || connectionError.toString();
          if (errorMessage.includes("502")) {
            if (errorMessage.includes("MQTT broker unreachable")) {
              throw new Error(
                "MQTT Broker Unreachable: Cannot connect to the specified broker. Please check the broker address and port."
              );
            } else if (errorMessage.includes("MQTT connection failed")) {
              throw new Error(
                "MQTT Connection Failed: The broker refused the connection. Check your broker settings and credentials."
              );
            } else if (errorMessage.includes("Connection timed out")) {
              throw new Error(
                "Connection Timeout: The broker is not responding. Please verify the broker address and your network connection."
              );
            }
          }
          throw connectionError;
        }

        const connectionData = {
          id: connectionId,
          type: connectionType,
          topicFamily,
          configuration,
          mqttConfig: mqttConfig.trim() ? mqttConfig : null,
          name: connectionType || `Connection ${connectionId.slice(0, 8)}`,
          savedAt: new Date().toISOString(),
        };

        ConnectionManager.saveConnection(connectionId, connectionData);
        console.log("Created new connection:", connectionData.name);

        onSave(connectionData);
        onClose();
      }
    } catch (err) {
      const userFriendlyError = parseServerError(err);
      const isEditMode = initialData && initialData.id;
      setValidationError(
        `${isEditMode ? "Update" : "Connection"} failed: ${userFriendlyError}`
      );
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
    parseServerError,
  ]);

  const handleCloseWithCleanup = useCallback(async () => {
    if (isSubmitting) return;

    const isEditMode = initialData && initialData.id;

    if (activeConnectionId && !isEditMode) {
      try {
        await callConfig(`/config/disconnect/${activeConnectionId}`, {});
        setActiveConnectionId(null);
      } catch (error) {
        console.warn("Failed to disconnect adapter on modal close:", error);
      }
    }

    onClose();
  }, [isSubmitting, activeConnectionId, initialData, onClose]);

  // Determine if save button should be enabled
  const canSave =
    connectionType &&
    topicFamily &&
    configuration.trim() &&
    !validationError &&
    !isValidating;

  const helpData = CONFIG_HELP[connectionType];
  const isEditMode = initialData && initialData.id;

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
            {CONNECTION_TYPES.map((type) => (
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
          <label htmlFor="configuration">Configuration (YAML): </label>
          <div className="textarea-container">
            <textarea
              id="configuration"
              rows={10}
              className="form-textarea"
              value={configuration}
              onChange={handleConfigurationChange}
              disabled={isSubmitting}
              placeholder="Enter your YAML configuration here..."
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
                <div className="help-content">{generateHelpText(helpData)}</div>
              </div>
            )}
          </div>
        </div>

        <div style={{ textAlign: "center", margin: "8px 0" }}>OR</div>

        <div className="file-upload-wrapper">
          <input
            ref={fileInputRef}
            type="file"
            accept=".yaml,.yml,.txt"
            onChange={handleFileUpload}
            style={{ display: "none" }}
            disabled={isSubmitting}
          />
          <button
            type="button"
            className="small-button file-upload-button"
            onClick={handleFileButtonClick}
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
          {!isEditingMqtt ? (
            <button
              type="button"
              className="edit-mqtt-button"
              onClick={() => setIsEditingMqtt(true)}
              disabled={isSubmitting}
            >
              Edit Broker
            </button>
          ) : (
            <button
              type="button"
              className="edit-mqtt-button"
              onClick={() => setIsEditingMqtt(false)}
              disabled={isSubmitting}
            >
              Done Editing
            </button>
          )}
        </div>
      </div>

      {isValidating && (
        <div className="info-message">Validating configuration...</div>
      )}

      {step && <div className="info-message">{step}</div>}

      {validationError && (
        <div className="error-message-box">
          <span className="error-title">Validation Error:</span>
          <pre className="error-details">{validationError}</pre>
        </div>
      )}

      <div className="button-group small">
        <button
          type="button"
          className="small-button cancel-button"
          onClick={handleCloseWithCleanup}
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
    </Modal>
  );
}
