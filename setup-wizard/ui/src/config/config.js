export const CONFIG_BLUEPRINT = {
  infra: {
    title: "Core Ingestion Channel (MQTT Broker)",
    fields: [
      { key: "MQTT_BROKER_HOST", label: "Internal Broker Hostname", type: "text", default: "mqtt-broker" },
      { key: "MQTT_BROKER_PORT", label: "Local Broker Host Binding Port", type: "number", default: 1883 },
      { key: "MQTT_DASHBOARD_PORT", label: "EMQX Dashboard UI Port", type: "number", default: 18083 },
      { key: "MQTT_WEBSOCKET_PORT", label: "MQTT WebSockets Port", type: "number", default: 8083 },
      { key: "MQTT_USERNAME", label: "Broker Authentication Username (Optional)", type: "text", default: "" },
      { key: "MQTT_PASSWORD", label: "Broker Authentication Password (Optional)", type: "password", default: "" }
    ]
  },
  kv: {
    title: "Key-Value Paradigm Parameters",
    fields: [
      { key: "KV_DB_HOST_PORT", label: "Database Host Binding Port (External)", type: "number", default: 5431 },
      { key: "KV_DB_USER", label: "Database Root Username", type: "text", default: "mfi" },
      { key: "KV_DB_PASSWORD", label: "Database Root Password", type: "password", default: "mfiddb" },
      { key: "KV_DB_NAME", label: "Initial Schema Logical Catalog", type: "text", default: "mfi_kv" },
      { key: "KV_CONNECTOR_CLIENT_ID", label: "MQTT Connector Client ID", type: "text", default: "kv-psql-connector" },
      { key: "KV_TOPIC_SUBSCRIPTION", label: "MQTT Topic Subscription Pattern", type: "text", default: "mfi-v1.0-kv/#" },
      { key: "KV_DWS_PORT", label: "KV DWS Internal gRPC Port", type: "number", default: 50051 }
    ]
  },
  ts: {
    title: "TimescaleDB Historian Database Settings",
    fields: [
      { key: "TS_DB_HOST_PORT", label: "Database Cluster Engine Port (External)", type: "number", default: 5432 },
      { key: "TS_DB_USER", label: "Database Administrative Identity", type: "text", default: "tsdb" },
      { key: "TS_DB_PASSWORD", label: "Security Token Key (Password)", type: "password", default: "timescale" },
      { key: "TS_DB_NAME", label: "Historian Database Name", type: "text", default: "ddb_ts" },
      { key: "TS_TOPIC_SUBSCRIPTION", label: "MQTT Historian Topic Subscription Pattern", type: "text", default: "mfi-v1.0-historian/#" },
      { key: "TS_COMPONENT_ID", label: "Default Component Identifier", type: "text", default: "default_component" },
      { key: "TS_DWS_PORT", label: "Timescale DWS Internal gRPC Port", type: "number", default: 50052 }
    ]
  },
  blob: {
    title: "Blob Object Ingestion Mapping",
    fields: [
      { key: "MFI_BLOB_HOST_PATH", label: "Absolute Host Storage Volume Mount Directory", type: "text", default: "~/mfi_storage/blob" },
      { key: "BLOB_DWS_PORT", label: "Blob DWS Internal gRPC Port", type: "number", default: 50053 },
      { key: "BLOB_TOPIC_SUBSCRIPTION", label: "Blob Topic Subscription Pattern", type: "text", default: "mfi-v1.0-blob/#" }
    ]
  },
  rws: {
    title: "Global Retrieval API & Metadata Integration (RWS)",
    fields: [
      { key: "RWS_API_PORT", label: "External Application REST Target Port", type: "number", default: 8000 },
      { key: "MDS_DB_HOST_PORT", label: "Metadata DB External Binding Port", type: "number", default: 5430 },
      { key: "MDS_DB_USER", label: "Metadata DB Username", type: "text", default: "mfi" },
      { key: "MDS_DB_PASSWORD", label: "Metadata DB Password", type: "password", default: "mfiddb" },
      { key: "MDS_DB_NAME", label: "Metadata Database Name", type: "text", default: "mds" },
      { key: "MDS_TOPIC_FAMILY", label: "Metadata Default Topic Family", type: "text", default: "kv" },
      { key: "MDS_TOPIC_VERSION", label: "Metadata Schema Architecture Version", type: "text", default: "1.0" },
      { key: "MDS_ENTERPRISE", label: "Metadata Corporate Enterprise Key", type: "text", default: "CMU" }
    ]
  }
};