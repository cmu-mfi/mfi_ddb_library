// src/components/ConnectionModal.js

import React, { useState, useRef, useEffect, useCallback } from "react";
import Modal from "./Modal";
import { callConfig } from "../api";
import { v4 as uuidv4 } from "uuid";

const CONNECTION_TYPES = [
  "MTConnect",
  "OPC UA",
  "MQTT-ADP",
  "Local Files",
  "ROS",
];

const TOPIC_FAMILIES = [
  { label: "Key-Value (kv)", value: "kv" },
  { label: "Blob", value: "blob" },
  { label: "Historian", value: "historian" },
];

const EXAMPLE_CONFIGS = {
  MTConnect: {
    configuration: `mtconnect:
  agent_ip: 8.8.8.8
  agent_url: 'http://agent.mtconnect.org/'
  trial_id: 'nist_test_${Date.now()}'  
  stream_rate: 10
  device_name: 'GFAgie01'`,
  },
  "OPC UA": {
    configuration: `endpoint: opc.tcp://localhost:4840
security:
  mode: None
authentication:
  type: Anonymous
subscription:
  publishing_interval: 1000`,
  },
  "MQTT-ADP": {
    configuration: `mqtt:
  broker_address: 128.237.92.30
  broker_port: 1883
  username: admin
  password: CMUmfi2024!

trial_id: 'proj_zbc0505'
queue_size: 10

topics:
  - topic: "home/temperature/#"
    component_id: "sensor.temperature"
  - topic: "home/humidity/#"
    component_id: "humidity"
    trial_id: "zbc_0505_humidity"`,
  },
  "Local Files": {
    configuration: `watch_dir:
  - 'C:/Users/mysor/Desktop/test_files'

system:
  name: 'keyence'
  trial_id: 'testing_lfs'

wait_before_read: 1 # seconds
buffer_size: 50`,
  },
  ROS: {
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

  yk_builder:
    namespace: yk_builder
    rostopics: [fts]
    attributes:
      description: Builder - Yaskawa GP4 Robot System
      type: Robot
      version: 0.01`,
  },
};

function makeDefaultMqttConfig() {
  return `mqtt:
  broker_address: 128.237.92.30
  broker_port: 1883
  enterprise: test-enterprise
  site: test-site
  username: admin
  password: CMUmfi2024!
  tls_enabled: True
  debug: True`;
}

export default function ConnectionModal({
  isOpen,
  onClose,
  onSave,
  initialData = {},
  isEditing = false,
}) {
  // === STATE ===
  const [connectionType, setConnectionType] = useState("");
  const [topicFamily, setTopicFamily] = useState("");
  const [configuration, setConfiguration] = useState("");
  const [mqttConfig, setMqttConfig] = useState(makeDefaultMqttConfig());
  const [isEditingMqtt, setIsEditingMqtt] = useState(false);
  const [step, setStep] = useState("");
  const [error, setError] = useState(null);
  const [validation, setValidation] = useState({ isValid: false, errors: {} });
  const [selectedFile, setSelectedFile] = useState(null);
  const [isSubmitting, setIsSubmitting] = useState(false);

  const [connId] = useState(initialData.id || uuidv4());
  const fileInputRef = useRef(null);

  // === VALIDATION HELPERS ===
  const validateYAMLFormats = useCallback((yaml) => {
    const errs = [];

    if (!yaml || !yaml.trim()) {
      return ["Configuration cannot be empty"];
    }

    const lines = yaml.split("\n");
    const ipRx =
      /^(25[0-5]|2[0-4]\d|[01]?\d\d?)(\.(25[0-5]|2[0-4]\d|[01]?\d\d?)){3}$/;
    const domainRx =
      /^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?)*$/;
    const urlRx = /^(https?:\/\/|mqtts?:\/\/|opc\.tcp:\/\/)[\w\d./:?=#-]+$/i;
    const portRx = /^\d{1,5}$/;

    lines.forEach((line, index) => {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#") || !trimmed.includes(":")) return;

      const colonIndex = trimmed.indexOf(":");
      const key = trimmed.substring(0, colonIndex).trim().toLowerCase();
      const value = trimmed
        .substring(colonIndex + 1)
        .trim()
        .replace(/['"]/g, "");

      if (!value) return;

      if (
        (key === "broker_address" ||
          key.endsWith("_ip") ||
          key === "agent_ip") &&
        value &&
        !ipRx.test(value) &&
        !domainRx.test(value) &&
        !value.includes("://")
      ) {
        errs.push(
          `Line ${
            index + 1
          }: "${key}" must be a valid IP address or domain name`
        );
      }

      if (
        (key.includes("url") || key === "endpoint") &&
        value &&
        !urlRx.test(value)
      ) {
        errs.push(`Line ${index + 1}: "${key}" has invalid URL format`);
      }

      if ((key === "broker_port" || key.includes("port")) && value) {
        if (
          !portRx.test(value) ||
          parseInt(value) > 65535 ||
          parseInt(value) < 1
        ) {
          errs.push(
            `Line ${index + 1}: "${key}" must be a valid port number (1-65535)`
          );
        }
      }
    });

    return errs;
  }, []);

  const validateYAMLConfig = useCallback(
    (yaml) => {
      const errs = validateYAMLFormats(yaml);
      return { isValid: errs.length === 0, errors: errs.join("\n") };
    },
    [validateYAMLFormats]
  );

  const validateForm = useCallback(() => {
    const errs = {};
    let isValid = true;

    if (!connectionType) {
      errs.connectionType = "Please select a connection type";
      isValid = false;
    }

    if (!topicFamily) {
      errs.topicFamily = "Please select a topic family";
      isValid = false;
    }

    if (!configuration.trim()) {
      errs.configuration = "Configuration YAML is required";
      isValid = false;
    } else {
      const { isValid: yamlValid, errors: yamlErrors } =
        validateYAMLConfig(configuration);
      if (!yamlValid) {
        errs.configuration = yamlErrors;
        isValid = false;
      }
    }

    const validationResult = { isValid, errors: errs };
    setValidation(validationResult);
    return isValid;
  }, [connectionType, topicFamily, configuration, validateYAMLConfig]);

  // === RESET ON OPEN ===
  useEffect(() => {
    if (!isOpen) return;

    setConnectionType(initialData.type || "");
    setTopicFamily(initialData.topicFamily || "");
    setConfiguration(initialData.configuration || "");
    setMqttConfig(initialData.mqttConfig || makeDefaultMqttConfig());
    setIsEditingMqtt(false);
    setStep("");
    setError(null);
    setValidation({ isValid: false, errors: {} });
    setSelectedFile(null);
    setIsSubmitting(false);

    if (fileInputRef.current) {
      fileInputRef.current.value = "";
    }
  }, [isOpen, initialData]);

  // === VALIDATION EFFECT ===
  useEffect(() => {
    if (isOpen && (connectionType || topicFamily || configuration)) {
      const timeoutId = setTimeout(() => {
        validateForm();
      }, 100);

      return () => clearTimeout(timeoutId);
    }
  }, [connectionType, topicFamily, configuration, isOpen, validateForm]);

  // === HANDLERS ===
  const handleTypeChange = useCallback((e) => {
    const selectedType = e.target.value;
    setConnectionType(selectedType);
    const exampleConfig = EXAMPLE_CONFIGS[selectedType];
    if (exampleConfig && exampleConfig.configuration) {
      setConfiguration(exampleConfig.configuration);
    } else {
      setConfiguration("");
    }
    setTopicFamily("");
    setMqttConfig(makeDefaultMqttConfig());
    setError(null);
  }, []);

  const handleTopicChange = useCallback((e) => {
    setTopicFamily(e.target.value);
    setError(null);
  }, []);

  const handleConfigurationChange = useCallback((e) => {
    setConfiguration(e.target.value);
    setError(null);
  }, []);

  const handleMqttConfigChange = useCallback((e) => {
    setMqttConfig(e.target.value);
  }, []);

  const handleFileUpload = useCallback((e) => {
    const file = e.target.files?.[0];
    if (!file) {
      setSelectedFile(null);
      return;
    }

    setSelectedFile(file);
    setError(null);

    const reader = new FileReader();
    reader.onload = (event) => {
      const content = event.target?.result;
      if (typeof content === "string") {
        setConfiguration(content);
      }
    };
    reader.onerror = () => {
      setError("Failed to read file");
      setSelectedFile(null);
    };
    reader.readAsText(file);
  }, []);

  const handleFileButtonClick = useCallback(() => {
    fileInputRef.current?.click();
  }, []);

  const handleSave = useCallback(async () => {
    if (!validateForm() || isSubmitting) return;

    setError(null);
    setIsSubmitting(true);

    try {
      let combinedConfig;
      let mqttWithTopicFamily = null;
      const rootTopicLine = `topic_family: ${topicFamily}\n\n`;

      if (mqttConfig.trim()) {
        mqttWithTopicFamily = mqttConfig.replace(
          "mqtt:",
          `mqtt:\n  topic_family: ${topicFamily}`
        );
      }

      // Build combinedConfig with root-level topic_family always
      combinedConfig =
        rootTopicLine +
        `${configuration}` +
        (mqttWithTopicFamily ? `\n\n${mqttWithTopicFamily}` : "");

      console.log("Sending combined configuration:", combinedConfig);

      setStep("validate");
      await callConfig("/config/validate", { text: combinedConfig });

      setStep("test");
      await callConfig("/config/test", { text: combinedConfig });

      setStep("connect");
      await callConfig(`/config/connect/${connId}`, { text: combinedConfig });
      await callConfig("/config/publish", {
        text: combinedConfig,
        topicFamily,
      });
      onSave({
        id: connId,
        type: connectionType,
        topicFamily,
        configuration,
        mqttConfig: mqttWithTopicFamily,
      });
      onClose();
    } catch (err) {
      console.error(`Error during ${step}:`, err);
      setError(`Failed at ${step} step: ${err.message || err}`);
    } finally {
      setStep("");
      setIsSubmitting(false);
    }
  }, [
    validateForm,
    isSubmitting,
    configuration,
    connectionType,
    topicFamily,
    mqttConfig,
    connId,
    onSave,
    onClose,
    step,
  ]);

  const handleClose = useCallback(() => {
    if (!isSubmitting) onClose();
  }, [isSubmitting, onClose]);

  return (
    <Modal
      isOpen={isOpen}
      onClose={handleClose}
      title={isEditing ? "Edit Adapter" : "New Adapter"}
    >
      <div className="modal-content configuration-modal">
        {/* Connection Type */}
        <div className="form-group">
          <label htmlFor="connection-type">Connection Type:</label>
          <select
            id="connection-type"
            value={connectionType}
            onChange={handleTypeChange}
            className={`form-select ${
              validation.errors.connectionType ? "error" : ""
            }`}
            disabled={isSubmitting}
          >
            <option value="">Select a type</option>
            {CONNECTION_TYPES.map((type) => (
              <option key={type} value={type}>
                {type}
              </option>
            ))}
          </select>
          {validation.errors.connectionType && (
            <div className="error-message">
              {validation.errors.connectionType}
            </div>
          )}
        </div>

        {/* Topic Family */}
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
          {validation.errors.topicFamily && (
            <div className="error-message">{validation.errors.topicFamily}</div>
          )}
        </div>

        {/* YAML Configuration */}
        <div className="form-group">
          <label htmlFor="configuration">Configuration (YAML):</label>
          <textarea
            id="configuration"
            rows={10}
            className={`form-textarea ${
              validation.errors.configuration ? "error" : ""
            }`}
            value={configuration}
            onChange={handleConfigurationChange}
            disabled={isSubmitting}
            placeholder="Enter your YAML configuration here..."
          />
          {validation.errors.configuration && (
            <div className="error-message-box">
              <span className="error-title">YAML Errors:</span>
              <pre className="error-details">
                {validation.errors.configuration}
              </pre>
            </div>
          )}

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
        </div>

        {/* MQTT Broker Configuration */}
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
          {!isEditingMqtt && (
            <button
              type="button"
              className="edit-mqtt-button"
              onClick={() => setIsEditingMqtt(true)}
              disabled={isSubmitting}
            >
              ✎ Edit Broker
            </button>
          )}
          {isEditingMqtt && (
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

        {/* Progress Indicator */}
        {step && <div className="info-message">Processing: {step}...</div>}

        {/* Error Display */}
        {error && (
          <div className="error-message-box">
            <span className="error-title">Error:</span>
            <div className="error-details">{error}</div>
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
            disabled={!validation.isValid || isSubmitting}
          >
            {isSubmitting ? "Saving..." : "Save"}
          </button>
        </div>
      </div>
    </Modal>
  );
}
