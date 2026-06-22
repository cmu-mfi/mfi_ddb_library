export const CONFIG_BLUEPRINT = {
  infra: {
    title: "Core Ingestion Channel (MQTT)",
    fields: [
      { key: "MQTT_BROKER_PORT", label: "Local Broker Host Binding Port", type: "number", default: "1883" }
    ]
  },
  kv: {
    title: "Key-Value Paradigm Parameters",
    fields: [
      { key: "KV_DB_HOST_PORT", label: "Database System Binding Port", type: "number", default: "5431" },
      { key: "KV_DB_USER", label: "Root Username", type: "text", default: "mfi" },
      { key: "KV_DB_PASSWORD", label: "Root Security Password", type: "password", default: "secure_kv_password" },
      { key: "KV_DB_NAME", label: "Initial Schema Logical Catalog", type: "text", default: "mfi_kv" }
    ]
  },
  ts: {
    title: "TimescaleDB Historian Database Settings",
    fields: [
      { key: "TS_DB_HOST_PORT", label: "Database Cluster Engine Target Port", type: "number", default: "5432" },
      { key: "TS_DB_USER", label: "Database Administrative Identity", type: "text", default: "postgres" },
      { key: "TS_DB_PASSWORD", label: "Security Token Key", type: "password", default: "secure_ts_password" }
    ]
  },
  blob: {
    title: "Blob Object Ingestion Mapping",
    fields: [
      { key: "MFI_BLOB_HOST_PATH", label: "Absolute Host Storage Volume Mount Directory", type: "text", default: "~/mfi_storage/blob" }
    ]
  },
  rws: {
    title: "Global Retrieval API Integration (RWS)",
    fields: [
      { key: "RWS_API_PORT", label: "External Application REST Target Port", type: "number", default: "8000" }
    ]
  }
};