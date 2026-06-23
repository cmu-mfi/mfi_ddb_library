import os
from pathlib import Path

from mfi_ddb.topic_families import blob

def write_runtime_configs(config_dir: Path, payload) -> None:
    """
    Accepts an initialized file system directory path and a validated 
    Pydantic MasterConfigPayload instance, writing out cleanly formatted strings.
    """
    infra = payload.infra
    kv = payload.kv
    ts = payload.ts
    blob = payload.blob
    rws = payload.rws

    # ==========================================
    # 1. KEY-VALUE CONNECTOR CONFIGURATION (.yaml)
    # ==========================================
    kv_yaml_content = (
        f"mqtt:\n"
        f"  broker: \"{infra.MQTT_BROKER_HOST}\"\n"
        f"  port: {infra.MQTT_BROKER_PORT}\n"
        f"  client_id: \"{kv.KV_CONNECTOR_CLIENT_ID}\"\n"
        f"  topics:\n"
        f"    - \"{kv.KV_TOPIC_SUBSCRIPTION}\"\n\n"
        f"postgres:\n"
        f"  host: \"mfi-kv-psql-db\"\n"
        f"  port: 5432\n"
        f"  database: \"{kv.KV_DB_NAME}\"\n"
        f"  user: \"{kv.KV_DB_USER}\"\n"
        f"  password: \"{kv.KV_DB_PASSWORD}\"\n"
    )
    (config_dir / "kv_psql_connector.yaml").write_text(kv_yaml_content)

    # ==========================================
    # 2. TIMESCALEDB CONNECTOR CONFIGURATION (.yaml)
    # ==========================================
    ts_yaml_content = (
        f"mqtt:\n"
        f"  broker_address: \"{infra.MQTT_BROKER_HOST}\"\n"
        f"  broker_port: {infra.MQTT_BROKER_PORT}\n"
        f"  topic: \"{ts.TS_TOPIC_SUBSCRIPTION}\"\n"
        f"  username: \"{infra.MQTT_USERNAME}\"\n"
        f"  password: \"{infra.MQTT_PASSWORD}\"\n\n"
        f"timescaledb:\n"
        f"  host: \"mfi-timescaledb-db\"\n"
        f"  port: 5432\n"
        f"  user: \"{ts.TS_DB_USER}\"\n"
        f"  password: \"{ts.TS_DB_PASSWORD}\"\n"
        f"  dbname: \"{ts.TS_DB_NAME}\"\n\n"
        f"component_id: \"{ts.TS_COMPONENT_ID}\"\n"
    )
    (config_dir / "timescale_connector.yaml").write_text(ts_yaml_content)

    # ==========================================
    # 3. RETRIEVAL API ROUTING RULES (.yaml)
    # ==========================================
    # Strips out trailing wildfire patterns '/#' so your API matches base route targets cleanly
    kv_family_base = kv.KV_TOPIC_SUBSCRIPTION.replace("/#", "")
    ts_family_base = ts.TS_TOPIC_SUBSCRIPTION.replace("/#", "")
    blob_family_base = blob.BLOB_TOPIC_SUBSCRIPTION.replace("/#", "")

    rws_endpoints_content = (
        f"services:\n"
        f"  metadata_service:\n"
        f"    url: \"http://mfi-metadata-store-db:5432\"\n"
        f"    topic_families:\n"
        f"    - \"{kv_family_base}\"\n"
        f"  kv_service:\n"
        f"    url: \"http://mfi-kv-psql-dws:{kv.KV_DWS_PORT}\"\n"
        f"    topic_families:\n"
        f"    - \"{kv_family_base}\"\n"
        f"  historian_service:\n"
        f"    url: \"http://mfi-timescaledb-dws:{ts.TS_DWS_PORT}\"\n"
        f"    topic_families:\n"
        f"    - \"{ts_family_base}\"\n"
        f"  cfs_service:\n"
        f"    url: \"http://mfi-blob-dws:{blob.BLOB_DWS_PORT}\"\n"
        f"    topic_families:\n"
        f"    - \"{blob_family_base}\"\n"
    )
    (config_dir / "rws_endpoints.yaml").write_text(rws_endpoints_content)

    # ==========================================
    # 4. METADATA BROKER SUBSCRIPTIONS (.ini)
    # ==========================================
    metadata_broker_content = (
        f"[mqtt]\n"
        f"broker_address={infra.MQTT_BROKER_HOST}\n"
        f"broker_port={infra.MQTT_BROKER_PORT}\n"
        f"username={infra.MQTT_USERNAME}\n"
        f"password={infra.MQTT_PASSWORD}\n"
        f"tls_enabled=false\n\n"
        f"[topic]\n"
        f"topic_family={rws.MDS_TOPIC_FAMILY}\n"
        f"version={rws.MDS_TOPIC_VERSION}\n"
        f"enterprise={rws.MDS_ENTERPRISE}\n"
    )
    (config_dir / "metadata_broker.ini").write_text(metadata_broker_content)

    # ==========================================
    # 5. POSTGRES INGESTION INI CONFIGURATIONS (.ini)
    # ==========================================
    metadata_db_content = (
        f"[postgresql]\n"
        f"host=mfi-metadata-store-db\n"
        f"port=5432\n"
        f"database={rws.MDS_DB_NAME}\n"
        f"user={rws.MDS_DB_USER}\n"
        f"password={rws.MDS_DB_PASSWORD}\n"
    )
    # Maps directly across both systemic consumers cleanly
    (config_dir / "metadata_pg.ini").write_text(metadata_db_content)
    (config_dir / "rws_pg.ini").write_text(metadata_db_content)


def generate_master_compose(runtime_dir: Path, payload) -> None:
    """
    Accepts the runtime root directory path and the validated Pydantic payload,
    generating a pristine, production-grade docker-compose.yaml file 
    fully matching the structural behavior of the original separate stack layers.
    """
    infra = payload.infra
    kv = payload.kv
    ts = payload.ts
    blob = payload.blob
    rws = payload.rws

    resolved_blob_path = os.path.abspath(os.path.expanduser(blob.MFI_BLOB_HOST_PATH))

    compose_content = f"""networks:
  mfi_network:
    driver: bridge

volumes:
  timescale_storage:
  kv_psql_storage:
  blob_storage:
  mds_storage:
  emqx_data:
  emqx_log:

services:
  # ==========================================
  # SHARED INFRASTRUCTURE
  # ==========================================
  mqtt-broker:
    image: emqx/emqx:5.8.0
    container_name: mfi-mqtt-broker
    ports:
      - "{infra.MQTT_BROKER_PORT}:1883"
      - "{infra.MQTT_WEBSOCKET_PORT}:8083"
      - "{infra.MQTT_DASHBOARD_PORT}:18083"
    environment:
      - EMQX_NAME=mfi_broker
    volumes:
      - emqx_data:/opt/emqx/data
      - emqx_log:/opt/emqx/log
    healthcheck:
      test: ["CMD", "emqx", "ctl", "status"]
      interval: 5s
      timeout: 5s
      retries: 3
    networks:
      mfi_network:
        aliases:
          - "{infra.MQTT_BROKER_HOST}"
    restart: always

  # ==========================================
  # BLOB DATABASE LAYER
  # ==========================================
  blob-connector:
    image: cmumfi/mfi-ddb-blob-connector:latest
    container_name: mfi-blob-connector
    depends_on:
      mqtt-broker:
        condition: service_healthy
    volumes:
      - "{resolved_blob_path}:/data/blob_storage"
    networks:
      - mfi_network
    restart: on-failure

  blob-dws:
    image: cmumfi/mfi-ddb-blob-dws:latest
    container_name: mfi-blob-dws
    ports:
      - "{blob.BLOB_DWS_PORT}:50053"
    depends_on:
      - blob-connector
    volumes:
      - "{resolved_blob_path}:/data/blob_storage:ro"
    networks:
      - mfi_network
    restart: always

  # ==========================================
  # KEY-VALUE POSTGRESQL LAYER
  # ==========================================
  kv-psql-db:
    image: postgres:15-alpine
    container_name: mfi-kv-psql-db
    environment:
      - POSTGRES_USER={kv.KV_DB_USER}
      - POSTGRES_PASSWORD={kv.KV_DB_PASSWORD}
      - POSTGRES_DB={kv.KV_DB_NAME}
    ports:
      - "{kv.KV_DB_HOST_PORT}:5432"
    volumes:
      - kv_psql_storage:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U {kv.KV_DB_USER} -d {kv.KV_DB_NAME}"]
      interval: 5s
      timeout: 5s
      retries: 5
    networks:
      - mfi_network
    restart: always

  kv-psql-connector:
    image: cmumfi/mfi-ddb-kv-psql-connector:latest
    container_name: mfi-kv-psql-connector
    depends_on:
      mqtt-broker:
        condition: service_healthy
      kv-psql-db:
        condition: service_healthy
    networks:
      - mfi_network
    volumes:
      - ./runtime_configs/kv_psql_connector.yaml:/app/config.yaml:ro
    restart: on-failure

  kv-psql-dws:
    image: cmumfi/mfi-ddb-kv-psql-dws:latest
    container_name: mfi-kv-psql-dws
    ports:
      - "{kv.KV_DWS_PORT}:50051"
    command: >
      sh -c "python -m mfi_ddb.databases.kv-psql.dws.init_db && 
             python -m mfi_ddb.databases.kv-psql.dws.server"
    depends_on:
      kv-psql-db:
        condition: service_healthy
    networks:
      - mfi_network
    restart: always

  # ==========================================
  # RETRIEVAL API & METADATA LAYER
  # ==========================================
  metadata-store-db:
    image: postgres:15-alpine
    container_name: mfi-metadata-store-db
    environment:
      - POSTGRES_USER={rws.MDS_DB_USER}
      - POSTGRES_PASSWORD={rws.MDS_DB_PASSWORD}
      - POSTGRES_DB={rws.MDS_DB_NAME}
    ports:
      - "{rws.MDS_DB_HOST_PORT}:5432"
    volumes:
      - mds_storage:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U {rws.MDS_DB_USER} -d {rws.MDS_DB_NAME}"]
      interval: 5s
      timeout: 5s
      retries: 5
    networks:
      - mfi_network
    restart: always

  metadata-store-connector:
    image: cmumfi/mfi-ddb-metadata-store-connector:latest
    container_name: mfi-metadata-store-connector
    command: >
      python src/mfi_ddb/retrieval_api/metadata_store_pg/connector.py
      --broker_config src/mfi_ddb/retrieval_api/metadata_store_pg/broker.ini
      --pg_config src/mfi_ddb/retrieval_api/metadata_store_pg/pg_database.ini
    depends_on:
      mqtt-broker:
        condition: service_healthy
      metadata-store-db:
        condition: service_healthy
    networks:
      - mfi_network
    volumes:
      - ./runtime_configs/metadata_broker.ini:/app/src/mfi_ddb/retrieval_api/metadata_store_pg/broker.ini:ro
      - ./runtime_configs/metadata_pg.ini:/app/src/mfi_ddb/retrieval_api/metadata_store_pg/pg_database.ini:ro
    restart: on-failure

  rws-app:
    image: cmumfi/mfi-ddb-rws-app:latest
    container_name: mfi-rws-app
    ports:
      - "{rws.RWS_API_PORT}:8000"
    depends_on:
      metadata-store-db:
        condition: service_healthy
      kv-psql-db:
        condition: service_healthy
      timescaledb-db:
        condition: service_healthy
    networks:
      - mfi_network
    volumes:
      - ./runtime_configs/rws_endpoints.yaml:/app/src/mfi_ddb/retrieval_api/rws/app/config/dws.endpoints.yaml:ro
      - ./runtime_configs/rws_pg.ini:/app/src/mfi_ddb/retrieval_api/rws/app/config/pg_database.ini:ro
    restart: always

  # ==========================================
  # TIMESCALEDB METRICS LAYER
  # ==========================================
  timescaledb-db:
    image: timescale/timescaledb:latest-pg16
    container_name: mfi-timescaledb-db
    environment:
      - POSTGRES_USER={ts.TS_DB_USER}
      - POSTGRES_PASSWORD={ts.TS_DB_PASSWORD}
      - POSTGRES_DB={ts.TS_DB_NAME}
    ports:
      - "{ts.TS_DB_HOST_PORT}:5432"
    volumes:
      - timescale_storage:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U {ts.TS_DB_USER} -d {ts.TS_DB_NAME}"]
      interval: 5s
      timeout: 5s
      retries: 5
    networks:
      - mfi_network
    restart: always

  timescaledb-connector:
    image: cmumfi/mfi-ddb-timescaledb-connector:latest
    container_name: mfi-timescaledb-connector
    depends_on:
      mqtt-broker:
        condition: service_healthy
      timescaledb-db:
        condition: service_healthy
    volumes:
      - ./runtime_configs/timescale_connector.yaml:/app/config.yaml:ro
    networks:
      - mfi_network
    restart: on-failure

  timescaledb-dws:
    image: cmumfi/mfi-ddb-timescaledb-dws:latest
    container_name: mfi-timescaledb-dws
    ports:
      - "{ts.TS_DWS_PORT}:50052"
    depends_on:
      timescaledb-db:
        condition: service_healthy
    networks:
      - mfi_network
    restart: always
"""
    (runtime_dir / "docker-compose.yaml").write_text(compose_content)