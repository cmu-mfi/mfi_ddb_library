import React, { useState, useRef, useEffect } from "react";
import Modal from "./Modal";

const CONNECTION_TYPES = ["MTConnect", "OPC UA", "MQTT", "Local Files", "ROS"];

const TOPIC_FAMILIES = [
  { label: "Key-Value (kv)", value: "kv" },
  { label: "Blob", value: "blob" },
  { label: "Historian", value: "historian" },
];

const EXAMPLE_CONFIGS = {
  MTConnect: {
    trialId: "haas_online",
    configuration: `mtconnect:
  agent_ip: 192.168.1.100
  agent_url: 'http://192.168.1.100:8082/'
  trial_id: 'haas_online'
  stream_rate: 10
  device_name: 'haascnc'`,
  },
  "Local Files": {
    trialId: "testing_lfs",
    configuration: `watch_dir: 
  - '/home/shobhit/repos/mfi_ddb_library/examples'

system:
  name: 'keyence'
  description: '...'
  type: '...'
  trial_id: 'testing_lfs'

wait_before_read: 1 # seconds
buffer_size: 10`,
  },
  ROS: {
    trialId: "untitled",
    configuration: `trial_id: 'untitled'
set_ros_callback: False

devices:
  yk_architect:
    namespace: yk_architect
    rostopics: [fts]
    attributes:
      description: Architect - Yaskawa GP4 Robot System
      type: Robot
      version: 0.01
      .
  device2_name:
    ....`,
  },
  "OPC UA": {
    trialId: `OPC-${Date.now()}`,
    configuration: `# OPC UA Configuration
endpoint: opc.tcp://localhost:4840
security:
  mode: None
  policy: None
authentication:
  type: Anonymous
subscription:
  publishing_interval: 1000
  lifetime_count: 10000
  keepalive_count: 200`,
  },
  MQTT: {
    trialId: `MQTT-${Date.now()}`,
    configuration: `# MQTT Configuration
broker: mqtt://localhost:1883
client_id: client-${Date.now()}
credentials:
  username: mqtt_user
  password: mqtt_pass
topics:
  publish:
    - sensor/data
    - sensor/status
  subscribe:
    - command/+
    - config/update
qos: 1
retain: false`,
  },
};

const ConnectionModal = ({
  isOpen,
  onClose,
  onSave,
  initialData = {},
  isEditing = false,
}) => {
  // Store type and form fields (trialId + YAML)
  const [connectionType, setConnectionType] = useState(initialData.type || "");
  const [formData, setFormData] = useState({
    topicFamily: initialData.topicFamily || "",
    configuration: initialData.configuration || "",
  });
  const [validation, setValidation] = useState({
    isValid: false,
    errors: {},
  });
  const [isPopulated, setIsPopulated] = useState(false);

  const fileInputRef = useRef(null);

  // Whenever the modal opens (or initialData changes), prefill if editing, or reset if new
  useEffect(() => {
    if (isOpen) {
      setConnectionType(initialData.type || "");
      setFormData({
        trialId: initialData.trialId || "",
        configuration: initialData.configuration || "",
      });
      setValidation({ isValid: false, errors: {} });
      setIsPopulated(false);
    }
  }, [isOpen, initialData]);

  // Extract trial_id / trial_id from YAML text
  const extractTrialId = (yamlContent) => {
    const lines = yamlContent.split("\n");
    for (const line of lines) {
      const match = line
        .trim()
        .match(/^(trial_id|trial_id):\s*['"]?(.+?)['"]?$/);
      if (match) return match[2].trim();
    }
    return "";
  };

  // Validate basic formats (IP, URL, port, numeric, email) line by line
  const validateYAMLFormats = (yamlContent) => {
    const formatErrors = [];
    const lines = yamlContent.split("\n");

    const ipRegex =
      /^(25[0-5]|2[0-4]\d|[01]?\d\d?)(\.(25[0-5]|2[0-4]\d|[01]?\d\d?)){3}$/;
    const urlRegex =
      /^(https?:\/\/)((25[0-5]|2[0-4]\d|[01]?\d\d?)(\.(25[0-5]|2[0-4]\d|[01]?\d\d?)){3}|([\da-z\.-]+\.[a-z\.]{2,6}))(:[0-9]{1,5})?(\/[\w\.-\/]*)?$/;

    const portRegex = /^([0-9]{1,5})$/;
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

    lines.forEach((line, idx) => {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#") || !trimmed.includes(":")) return;
      const [keyPart, ...rest] = trimmed.split(":");
      const keyName = keyPart.trim().toLowerCase();
      const value = rest.join(":").trim().replace(/['"]/g, "");

      // IP / Host / Address
      if (
        (keyName === "ip" || keyName.endsWith("_ip")) &&
        value &&
        !["localhost", "0.0.0.0", "127.0.0.1"].includes(value) &&
        !value.startsWith("/")
      ) {
        if (!ipRegex.test(value) && !value.includes("://")) {
          formatErrors.push({
            line: idx + 1,
            key: keyPart.trim(),
            value,
            error: "Invalid IP address format",
          });
        }
      }

      // URL / Endpoint
      if ((keyName.includes("url") || keyName.includes("endpoint")) && value) {
        if (!value.startsWith("opc.tcp://") && !value.startsWith("mqtt://")) {
          if (!urlRegex.test(value)) {
            formatErrors.push({
              line: idx + 1,
              key: keyPart.trim(),
              value,
              error: "Invalid URL format",
            });
          }
        }
      }

      // Port
      if (keyName === "port" && value) {
        if (portRegex.test(value)) {
          const p = parseInt(value, 10);
          if (p < 1 || p > 65535) {
            formatErrors.push({
              line: idx + 1,
              key: keyPart.trim(),
              value,
              error: "Port must be between 1 and 65535",
            });
          }
        } else {
          formatErrors.push({
            line: idx + 1,
            key: keyPart.trim(),
            value,
            error: "Invalid port format",
          });
        }
      }

      // Email / Contact
      if ((keyName.includes("email") || keyName === "contact") && value) {
        if (!emailRegex.test(value)) {
          formatErrors.push({
            line: idx + 1,
            key: keyPart.trim(),
            value,
            error: "Invalid email format",
          });
        }
      }

      // Numeric fields
      if (
        keyName.includes("rate") ||
        keyName.includes("interval") ||
        keyName.includes("timeout") ||
        keyName.includes("buffer_size") ||
        keyName.includes("count")
      ) {
        if (value && isNaN(value) && !value.includes("#")) {
          formatErrors.push({
            line: idx + 1,
            key: keyPart.trim(),
            value,
            error: "Value must be numeric",
          });
        }
      }
    });

    return formatErrors;
  };

  // Validate entire form: type, trial ID, and YAML content
  const validateForm = (data = formData) => {
    const errors = {};
    let ok = true;

    if (!connectionType) {
      errors.connectionType = "Please select a connection type";
      ok = false;
    }

    if (!data.topicFamily.trim()) {
      errors.topicFamily = "Topic family is required";
      ok = false;
    }

    if (!data.configuration.trim()) {
      errors.configuration = "Configuration is required";
      ok = false;
    }

    if (data.configuration) {
      const yamlErrors = validateYAMLFormats(data.configuration);
      if (yamlErrors.length > 0) {
        const msgs = yamlErrors.map(
          (e) => `Line ${e.line}: ${e.key} – ${e.error} (found: ${e.value})`
        );
        errors.configuration = msgs.join("\n");
        ok = false;
      }
    }

    setValidation({ isValid: ok, errors });
    return ok;
  };

  // Handle YAML file upload (extract trial_id and fill the textarea)
  const handleFileUpload = (e) => {
    const file = e.target.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (ev) => {
      const text = ev.target.result;
      const tid = extractTrialId(text);
      setFormData({ trialId: tid || "", configuration: text });
      setIsPopulated(true);
      validateForm({ trialId: tid || "", configuration: text });
    };
    reader.readAsText(file);
  };

  // Populate sample YAML & trial ID for the selected type
  const handlePopulate = () => {
    if (!connectionType) return; // do nothing if no type
    const sample = EXAMPLE_CONFIGS[connectionType];
    if (sample) {
      setFormData(sample);
      setIsPopulated(true);
      validateForm(sample);
    }
  };

  const handleValidate = () => {
    validateForm();
  };

  // "Save Connection" → validate and pass data back to parent
  const handleSave = () => {
    if (!validateForm()) {
      alert("Please fix validation errors before saving.");
      return;
    }
    onSave({
      type: connectionType,
      topicFamily: formData.topicFamily,
      configuration: formData.configuration,
    });
    onClose();
  };

  // When the user changes the dropdown, reset the YAML & trial ID
  const handleTypeChange = (e) => {
    const newType = e.target.value;
    setConnectionType(newType);
    setFormData({ trialId: "", configuration: "" });
    setValidation({ isValid: false, errors: {} });
    setIsPopulated(false);
  };

  return (
    <Modal
      isOpen={isOpen}
      onClose={onClose}
      title={isEditing ? "Edit Adapter" : "New Adapter"}
    >
      {/* Wrap content in a wider container */}
      <div className="modal-content configuration-modal">
        {/* -------- Connection Type Dropdown (always visible) -------- */}
        <div className="form-group">
          <label htmlFor="connection-type">Connection Type:</label>
          <select
            id="connection-type"
            className={`form-select ${
              validation.errors.connectionType ? "error" : ""
            }`}
            value={connectionType}
            onChange={handleTypeChange}
          >
            <option value="">Select a type</option>
            {CONNECTION_TYPES.map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
          {validation.errors.connectionType && (
            <span className="error-message">
              {validation.errors.connectionType}
            </span>
          )}
        </div>

        {/* -------- Topic Family Radio Group (always visible) -------- */}
        <div className="form-group">
          <label>Topic Family:</label>
          <div className="radio-group">
            {TOPIC_FAMILIES.map(({ label, value }) => (
              <label key={value} className="radio-label">
                <input
                  type="radio"
                  name="topicFamily"
                  value={value}
                  checked={formData.topicFamily === value}
                  onChange={() =>
                    setFormData({ ...formData, topicFamily: value })
                  }
                />
                {label}
              </label>
            ))}
          </div>
          {validation.errors.topicFamily && (
            <span className="error-message">
              {validation.errors.topicFamily}
            </span>
          )}
        </div>

        {/* -------- YAML Configuration Textarea (always visible) -------- */}
        <div className="form-group">
          <label htmlFor="configuration">Configuration (YAML):</label>
          <textarea
            id="configuration"
            className={`form-textarea ${
              validation.errors.configuration ? "error" : ""
            }`}
            value={formData.configuration}
            onChange={(e) =>
              setFormData({ ...formData, configuration: e.target.value })
            }
            placeholder="Enter YAML configuration"
            rows="10"
          />
          {validation.errors.configuration && (
            <div className="error-message-box">
              <span className="error-title">Configuration Errors:</span>
              <pre className="error-details">
                {validation.errors.configuration}
              </pre>
            </div>
          )}
        </div>

        {/* -------- File Upload Button -------- */}
        <div className="form-group">
          <label htmlFor="file-upload">Upload Configuration File:</label>
          <div className="file-upload-wrapper">
            <input
              ref={fileInputRef}
              id="file-upload"
              type="file"
              className="file-input"
              onChange={handleFileUpload}
              accept=".yaml,.yml,.conf"
            />
            <button
              className="file-upload-button"
              onClick={() => fileInputRef.current?.click()}
            >
              Choose File
            </button>
            <span className="file-name">
              {fileInputRef.current?.files?.[0]?.name || "No file chosen"}
            </span>
          </div>
        </div>

        {/* -------- Populate & Validate Buttons -------- */}
        <div className="button-group">
          <button className="populate-button" onClick={handlePopulate}>
            Populate Sample
          </button>
          <button
            className={`validate-button ${validation.isValid ? "valid" : ""}`}
            onClick={handleValidate}
          >
            {validation.isValid ? "✓ Valid" : "Validate"}
          </button>
        </div>

        {isPopulated && (
          <div className="status-message success">
            Configuration populated successfully!
          </div>
        )}

        {/* -------- Save Connection Button -------- */}
        <button className="save-button-full" onClick={handleSave}>
          Save Connection
        </button>
      </div>
    </Modal>
  );
};

export default ConnectionModal;
