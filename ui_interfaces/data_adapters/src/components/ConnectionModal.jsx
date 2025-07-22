import React, { useState, useRef, useEffect, useCallback } from "react";
import Modal from "./Modal";
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
  const [connectionType, setConnectionType] = useState(""); // Selected protocol type
  const [topicFamily, setTopicFamily] = useState(""); // MQTT topic classification
  const [configuration, setConfiguration] = useState(""); // Main adapter configuration YAML
  const [mqttConfig, setMqttConfig] = useState(makeDefaultMqttConfig()); // MQTT broker settings

  // UI interaction state
  const [isEditingMqtt, setIsEditingMqtt] = useState(false); // Controls MQTT config editability
  const [step, setStep] = useState(""); // Current processing step for user feedback
  const [validationError, setValidationError] = useState(""); // User-friendly validation errors
  const [selectedFile, setSelectedFile] = useState(null); // Uploaded configuration file
  const [isSubmitting, setIsSubmitting] = useState(false); // Prevents duplicate submissions
  const [isValidating, setIsValidating] = useState(false); // Shows validation in progress
  const fileInputRef = useRef(null); // Reference for programmatic file input trigger
  const [showHelp, setShowHelp] = useState(false);
  const [activeConnectionId, setActiveConnectionId] = useState(null);
  // Handle connection type selection and auto-populate configuration
  const handleTypeChange = useCallback((e) => {
    const selectedType = e.target.value;
    setConnectionType(selectedType);
    setConfiguration(EXAMPLE_CONFIGS[selectedType]?.configuration || ""); // Load example config
    // Set default topic family based on protocol characteristics
    setTopicFamily(
      selectedType === "MTConnect"
        ? "historian" // MTConnect typically streams time-series data
        : selectedType === "Local Files"
        ? "blob" // File monitoring handles binary/document content
        : "" // Let user choose for other protocols
    );
    setMqttConfig(makeDefaultMqttConfig()); // Reset MQTT config to defaults
    setValidationError(""); // Clear any previous validation errors
  }, []);

  // Handle topic family radio button selection
  const handleTopicChange = useCallback((e) => {
    setTopicFamily(e.target.value);
    setValidationError(""); // Clear validation errors when user makes changes
  }, []);

  // Handle main configuration text area changes
  const handleConfigurationChange = useCallback((e) => {
    setConfiguration(e.target.value);
    setValidationError(""); // Clear errors to show input is being processed
  }, []);

  // Handle MQTT broker configuration changes
  const handleMqttConfigChange = useCallback((e) => {
    setMqttConfig(e.target.value);
    setValidationError(""); // Clear errors on any change
  }, []);

  // Handle configuration file uploads and read content
  const handleFileUpload = useCallback((e) => {
    const file = e.target.files?.[0];
    if (!file) {
      setSelectedFile(null);
      return;
    }
    setSelectedFile(file); // Store file reference for display
    const reader = new FileReader();
    // Populate configuration field with file content
    reader.onload = ({ target }) =>
      typeof target.result === "string" &&
      setConfiguration(
        target.result.match(/^\w+:\s*\n/)
          ? target.result.replace(/^\w+:\s*\n/, "").replace(/^  /gm, "")
          : target.result
      );
    reader.onerror = () => setValidationError("Failed to read file");
    reader.readAsText(file); // Read as text for YAML/config files
  }, []);

  // Trigger hidden file input programmatically
  const handleFileButtonClick = useCallback(
    () => fileInputRef.current?.click(),
    []
  );

  // Handle modal close with submission state protection
  const handleClose = useCallback(async () => {
    if (isSubmitting) return; // Prevent close during save operation
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
    // Populate fields from initial data (edit mode) or defaults (new mode)
    setConnectionType(initialData.type || "");
    setTopicFamily(initialData.topicFamily || "");
    setConfiguration(initialData.configuration || "");
    setMqttConfig(initialData.mqttConfig || makeDefaultMqttConfig());
    // Reset UI state to defaults
    setIsEditingMqtt(false);
    setStep("");
    setValidationError("");
    setSelectedFile(null);
    setIsSubmitting(false);
    if (fileInputRef.current) fileInputRef.current.value = ""; // Clear file input
  }, [isOpen, initialData]);

  // Client-side YAML format validation with regex patterns
  const validateYAMLFormats = useCallback((yaml) => {
    if (!yaml?.trim()) return ["Configuration cannot be empty"];

    const errs = [];
    // Regular expressions for validating common configuration values
    const ipRx =
      /^(25[0-5]|2[0-4]\d|[01]?\d\d?)(\.(25[0-5]|2[0-4]\d|[01]?\d\d?)){3}$/;
    const domainRx =
      /^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?)*$/;
    const urlRx = /^(https?:\/\/|mqtts?:\/\/|opc\.tcp:\/\/)[\w\d./:?=#-]+$/i;
    const portRx = /^\d{1,5}$/;

    // Parse YAML line by line for format validation
    yaml.split("\n").forEach((line, index) => {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#") || !trimmed.includes(":")) return; // Skip empty/comment lines

      const [key, ...rest] = trimmed.split(":");
      const cleanKey = key.trim().toLowerCase();
      const value = rest.join(":").trim().replace(/['"]/g, ""); // Remove quotes

      if (!value) return; // Skip empty values

      // Validate IP addresses and hostnames
      if (
        (cleanKey === "broker_address" ||
          cleanKey.endsWith("_ip") ||
          cleanKey === "agent_ip") &&
        !ipRx.test(value) &&
        !domainRx.test(value) &&
        !value.includes("://") // Allow full URLs to pass through
      ) {
        errs.push(
          `Line ${index + 1}: "${cleanKey}" must be a valid IP or domain`
        );
      }

      // Validate URL formats for endpoints and agent URLs
      if (
        (cleanKey.includes("url") || cleanKey === "endpoint") &&
        !urlRx.test(value)
      ) {
        errs.push(`Line ${index + 1}: "${cleanKey}" has invalid URL format`);
      }

      // Validate port numbers are within valid range
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
  // Parse server error responses into user-friendly messages
  const parseServerError = useCallback((error) => {
    try {
      const errorStr = error.message || error.toString();

      // Handle broker-specific errors first (highest priority)
      if (
        errorStr.includes("MQTT Broker Unreachable:") ||
        errorStr.includes("MQTT Connection Failed:") ||
        errorStr.includes("Connection Timeout:")
      ) {
        return errorStr; // Return the already formatted broker error
      }

      // Handle other broker-related patterns
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

      // Try to extract JSON error details from API response
      if (errorStr.includes('"detail"')) {
        try {
          const jsonMatch = errorStr.match(/\{.*\}/);
          if (jsonMatch) {
            const errorObj = JSON.parse(jsonMatch[0]);
            if (errorObj.detail) {
              // Check if the detail contains broker errors
              if (errorObj.detail.includes("MQTT broker unreachable")) {
                return "MQTT Broker Unreachable: Cannot connect to the specified broker. Please verify the broker address and port.";
              }
              if (errorObj.detail.includes("MQTT connection failed")) {
                return "MQTT Connection Failed: The broker rejected the connection. Please check your broker configuration.";
              }
              return errorObj.detail; // Return clean error detail
            }
          }
        } catch (parseErr) {
          // Fall through to pattern matching if JSON parsing fails
        }
      }

      // Pattern matching for common error types with helpful suggestions
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

      // Clean up raw error messages by removing JSON artifacts
      return (
        errorStr
          .replace(/\{.*?\}/g, "") // Remove JSON objects
          .replace(/\\n/g, " ") // Convert escaped newlines to spaces
          .replace(/Error:\s*/g, "") // Remove "Error:" prefix
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
      // Step 1: Basic form completeness validation
      if (!connectionType) {
        throw new Error("Please select a connection type");
      }
      if (!topicFamily) {
        throw new Error("Please select a topic family");
      }
      if (!configuration.trim()) {
        throw new Error("Configuration YAML is required");
      }

      // Step 2: Client-side YAML format validation for main config
      const configErrors = validateYAMLFormats(configuration);
      if (configErrors.length > 0) {
        throw new Error(`Configuration errors:\n${configErrors.join("\n")}`);
      }

      // Step 3: Client-side YAML format validation for MQTT config (if provided)
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
      // Step 4: Prepare combined configuration for server validation
      const combinedConfig =
        `topic_family: ${topicFamily}\n\n${wrappedConfig}` +
        (mqttConfig.trim()
          ? `\n\n${mqttConfig.replace(
              "mqtt:",
              `mqtt:\n  topic_family: ${topicFamily}` // Inject topic family into MQTT config
            )}`
          : "");

      // Step 5: Server-side validation for schema and connectivity
      await callConfig("/config/validate", {
        text: combinedConfig,
        topicFamily,
      });

      return true; // Validation passed
    } catch (err) {
      const userFriendlyError = parseServerError(err);
      setValidationError(userFriendlyError);
      return false; // Validation failed
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
    // Skip validation if modal closed or form incomplete
    if (!isOpen || !connectionType || !topicFamily || !configuration.trim()) {
      setValidationError("");
      return;
    }

    // Debounce validation to reduce server load
    const timeoutId = setTimeout(() => {
      validateConfiguration();
    }, 800); // Wait 800ms after last change

    return () => clearTimeout(timeoutId); // Cleanup on dependency change
  }, [
    configuration,
    mqttConfig,
    topicFamily,
    connectionType,
    isOpen,
    validateConfiguration,
  ]);

  const wrapConfigForProtocol = (connectionType, configuration) => {
    // Protocol mapping for UI types to config keys
    const protocolMapping = {
      ROS: "ros",
      "ROS Files": "ros_files",
      "Local Files": "file",
      MTConnect: "mtconnect",
      "MQTT-ADP": "mqtt",
    };

    const protocolKey = protocolMapping[connectionType];

    // If no mapping found, return as-is
    if (!protocolKey) {
      return configuration;
    }

    // Wrap the configuration under the protocol key
    return `${protocolKey}:\n${configuration.replace(/^/gm, "  ")}`; // Indent each line by 2 spaces
  };
  // Handle save operation with validation and server connection
  // Handle save operation with validation and server connection
  const handleSave = useCallback(async () => {
    if (isSubmitting) return; // Prevent duplicate submissions

    setIsSubmitting(true);
    setValidationError("");

    try {
      // Final validation check before attempting connection
      const isValid = await validateConfiguration();
      if (!isValid) {
        setIsSubmitting(false);
        return;
      }

      const wrappedConfig = wrapConfigForProtocol(
        connectionType,
        configuration
      );
      // Prepare complete configuration for server
      const combinedConfig =
        `topic_family: ${topicFamily}\n\n${wrappedConfig}` +
        (mqttConfig.trim()
          ? `\n\n${mqttConfig.replace(
              "mqtt:",
              `mqtt:\n  topic_family: ${topicFamily}` // Inject topic family
            )}`
          : "");

      const connectionId = crypto.randomUUID(); // Generate unique connection ID
      setActiveConnectionId(connectionId);
      setStep("Connecting to adapter..."); // Show progress to user

      // Enhanced connection attempt with specific error handling
      try {
        await callConfig(`/config/connect/${connectionId}`, {
          text: combinedConfig,
          topicFamily,
        });
      } catch (connectionError) {
        // Check if it's a broker-related error by examining the error message
        const errorMessage =
          connectionError.message || connectionError.toString();

        if (errorMessage.includes("502")) {
          // 502 errors are broker/connection issues from your backend
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

        // Re-throw the original error if it's not a recognized broker error
        throw connectionError;
      }

      // Notify parent component of successful save
      onSave({
        id: connectionId,
        type: connectionType,
        topicFamily,
        configuration,
        mqttConfig: mqttConfig.trim() ? mqttConfig : null, // Only include if non-empty
      });

      onClose(); // Close modal on success
    } catch (err) {
      const userFriendlyError = parseServerError(err);
      setValidationError(`Connection failed: ${userFriendlyError}`);
    } finally {
      setStep(""); // Clear processing message
      setIsSubmitting(false);
    }
  }, [
    isSubmitting,
    validateConfiguration,
    configuration,
    connectionType,
    topicFamily,
    mqttConfig,
    onSave,
    onClose,
    parseServerError,
  ]);

  // Determine if save button should be enabled
  const canSave =
    connectionType &&
    topicFamily &&
    configuration.trim() &&
    !validationError &&
    !isValidating;
  const helpData = CONFIG_HELP[connectionType];
  return (
    <Modal
      isOpen={isOpen}
      onClose={handleClose}
      title={isEditing ? "Edit Adapter" : "New Adapter"}
    >
      <div className="modal-content configuration-modal">
        {/* Connection Type Selection Dropdown */}
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

        {/* Topic Family Radio Button Selection */}
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

            {/* Help button inside textarea */}
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

            {/* Help tooltip */}
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
            style={{ display: "none" }} // Hidden file input
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
            disabled={isSubmitting || !isEditingMqtt} // Disabled unless in edit mode
            readOnly={!isEditingMqtt}
            placeholder="MQTT broker configuration..."
          />
          {/* Toggle button for MQTT config editing */}
          {!isEditingMqtt ? (
            <button
              type="button"
              className="edit-mqtt-button"
              onClick={() => setIsEditingMqtt(true)}
              disabled={isSubmitting}
            >
              ✎ Edit Broker
            </button>
          ) : (
            <button
              type="button"
              className="edit-mqtt-button"
              onClick={() => setIsEditingMqtt(false)}
              disabled={isSubmitting}
            >
              ✓ Done Editing
            </button>
          )}
        </div>
      </div>

      {/* MQTT Broker Configuration with Protected Editing */}

      {/* Validation Status Indicator */}
      {isValidating && (
        <div className="info-message">Validating configuration...</div>
      )}

      {/* Processing Step Indicator */}
      {step && <div className="info-message">{step}</div>}

      {/* Validation Error Display */}
      {validationError && (
        <div className="error-message-box">
          <span className="error-title">Validation Error:</span>
          <pre className="error-details">{validationError}</pre>
        </div>
      )}

      {/* Action Buttons */}
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
          disabled={!canSave || isSubmitting} // Disabled until form is valid
        >
          {isSubmitting ? "Saving..." : "Save"}
        </button>
      </div>
    </Modal>
  );
}
