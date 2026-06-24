from pydantic import BaseModel

class InfraConfig(BaseModel):
    MQTT_BROKER_HOST: str = "mqtt-broker"
    MQTT_BROKER_PORT: int = 1883
    MQTT_DASHBOARD_PORT: int = 18083
    MQTT_WEBSOCKET_PORT: int = 8083
    MQTT_USERNAME: str = ""
    MQTT_PASSWORD: str = ""

class KVConfig(BaseModel):
    KV_DEPLOYMENT: str = "internal"
    KV_DB_HOST: str = "kv-psql-db"
    KV_DB_HOST_PORT: int = 5431
    KV_DB_USER: str = "mfi"
    KV_DB_PASSWORD: str = "mfiddb"
    KV_DB_NAME: str = "mfi_kv"
    KV_CONNECTOR_CLIENT_ID: str = "kv-psql-connector"
    KV_TOPIC_SUBSCRIPTION: str = "mfi-v1.0-kv/#"
    KV_DWS_PORT: int = 50051

class TSConfig(BaseModel):
    TS_DEPLOYMENT: str = "internal"
    TS_DB_HOST: str = "timescaledb-db"
    TS_DB_HOST_PORT: int = 5432
    TS_DB_USER: str = "tsdb"
    TS_DB_PASSWORD: str = "timescale"
    TS_DB_NAME: str = "ddb_ts"
    TS_TOPIC_SUBSCRIPTION: str = "mfi-v1.0-historian/#"
    TS_COMPONENT_ID: str = "default_component"
    TS_DWS_PORT: int = 50052

class BlobConfig(BaseModel):
    MFI_BLOB_HOST_PATH: str = "~/mfi_storage/blob"
    BLOB_DWS_PORT: int = 50053
    BLOB_TOPIC_SUBSCRIPTION: str = "mfi-v1.0-blob/#"

class RWSConfig(BaseModel):
    RWS_DEPLOYMENT: str = "internal"
    MDS_DB_HOST: str = "metadata-store-db"
    RWS_API_PORT: int = 8000
    MDS_DB_HOST_PORT: int = 5430
    MDS_DB_USER: str = "mfi"
    MDS_DB_PASSWORD: str = "mfiddb"
    MDS_DB_NAME: str = "mds"
    MDS_TOPIC_FAMILY: str = "kv"
    MDS_TOPIC_VERSION: str = "1.0"
    MDS_ENTERPRISE: str = "CMU"

class MasterConfigPayload(BaseModel):
    infra: InfraConfig
    kv: KVConfig
    ts: TSConfig
    blob: BlobConfig
    rws: RWSConfig