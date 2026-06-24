import os
from pathlib import Path

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
        f"  host: \"{kv.KV_DB_HOST}\"\n"
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
        f"  host: \"{ts.TS_DB_HOST}\"\n"
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
    kv_family_base = kv.KV_TOPIC_SUBSCRIPTION.replace("/#", "")
    ts_family_base = ts.TS_TOPIC_SUBSCRIPTION.replace("/#", "")
    blob_family_base = blob.BLOB_TOPIC_SUBSCRIPTION.replace("/#", "")

    # Compute the URLs cleanly before formatting the block
    if kv.KV_DEPLOYMENT == "external":
        kv_routing_url = f"http://{kv.KV_DB_HOST}:{kv.KV_DWS_PORT}"
    else:
        kv_routing_url = f"http://mfi-kv-psql-dws:{kv.KV_DWS_PORT}"

    if ts.TS_DEPLOYMENT == "external":
        ts_routing_url = f"http://{ts.TS_DB_HOST}:{ts.TS_DWS_PORT}"
    else:
        ts_routing_url = f"http://mfi-timescaledb-dws:{ts.TS_DWS_PORT}"

    rws_endpoints_content = (
        f"services:\n"
        f"  kv_service:\n"
        f"    url: \"{kv_routing_url}\"\n"
        f"    topic_families:\n"
        f"    - \"{kv_family_base}\"\n"
        f"  historian_service:\n"
        f"    url: \"{ts_routing_url}\"\n"
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
        f"host={rws.MDS_DB_HOST}\n"
        f"port=5432\n"
        f"database={rws.MDS_DB_NAME}\n"
        f"user={rws.MDS_DB_USER}\n"
        f"password={rws.MDS_DB_PASSWORD}\n"
    )
    (config_dir / "metadata_pg.ini").write_text(metadata_db_content)
    (config_dir / "rws_pg.ini").write_text(metadata_db_content)


def generate_master_compose(runtime_dir: Path, payload) -> None:
    """
    Generates a decoupled, robust docker-compose.yaml architecture
    where services can run in pure isolation without hard dependency failures.
    """
    infra = payload.infra
    kv = payload.kv
    ts = payload.ts
    blob = payload.blob
    rws = payload.rws

    resolved_blob_path = os.path.abspath(os.path.expanduser(blob.MFI_BLOB_HOST_PATH))

# Dynamic loopback string generation logic
    extra_hosts_block = ""
    if getattr(kv, 'KV_DB_HOST', '') == "host.docker.internal" or getattr(ts, 'TS_DB_HOST', '') == "host.docker.internal":
        extra_hosts_block = (
            "\n    extra_hosts:"
            "\n      - \"host.docker.internal:host-gateway\""
        )

    # Note the explicit prepended newline and consistent block indentations
    kv_depends_on = ""
    if kv.KV_DB_HOST in ["kv-psql-db", "mfi-kv-psql-db"]:
        kv_depends_on = (
            "\n    depends_on:"
            "\n      kv-psql-db:"
            "\n        condition: service_healthy"
        )

    ts_depends_on = ""
    if ts.TS_DB_HOST in ["timescaledb-db", "mfi-timescaledb-db"]:
        ts_depends_on = (
            "\n    depends_on:"
            "\n      timescaledb-db:"
            "\n        condition: service_healthy"
        )

    rws_depends_on = ""
    if rws.MDS_DB_HOST in ["metadata-store-db", "mfi-metadata-store-db"]:
        rws_depends_on = (
            "\n    depends_on:"
            "\n      metadata-store-db:"
            "\n        condition: service_healthy"
        )

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
  # SHARED INFRASTRUCTURE (Profile: broker)
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
    profiles:
      - "infra"
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
  # BLOB DATABASE LAYER (Profile: blob)
  # ==========================================
  blob-connector:
    image: cmumfi/mfi-ddb-blob-connector:latest
    container_name: mfi-blob-connector
    profiles:
      - "blob"
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
    profiles:
      - "blob"
    volumes:
      - "{resolved_blob_path}:/data/blob_storage:ro"
    networks:
      - mfi_network
    restart: always

  # ==========================================
  # KEY-VALUE POSTGRESQL LAYER (Profile: kv)
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
    profiles:
      - "kv"
    networks:
      - mfi_network
    restart: always

  kv-psql-connector:
    image: cmumfi/mfi-ddb-kv-psql-connector:latest
    container_name: mfi-kv-psql-connector{extra_hosts_block}{kv_depends_on}
    environment:
      - DB_HOST={kv.KV_DB_HOST}
    networks:
      - mfi_network
    profiles:
      - "kv"
    volumes:
      - ./runtime_configs/kv_psql_connector.yaml:/app/config.yaml:ro
    restart: always

  kv-psql-dws:
    image: cmumfi/mfi-ddb-kv-psql-dws:latest
    container_name: mfi-kv-psql-dws
    ports:
      - "{kv.KV_DWS_PORT}:50051"{kv_depends_on}
    command: >
      sh -c "python -m mfi_ddb.databases.kv-psql.dws.init_db --host {kv.KV_DB_HOST} && 
             python -m mfi_ddb.databases.kv-psql.dws.server"
    profiles:
      - "kv"
    networks:
      - mfi_network
    restart: always

  # ==========================================
  # RETRIEVAL API & METADATA LAYER (Profile: rws)
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
    profiles:
      - "rws"
    restart: always

  metadata-store-connector:
    image: cmumfi/mfi-ddb-metadata-store-connector:latest
    container_name: mfi-metadata-store-connector
    command: >
      python src/mfi_ddb/retrieval_api/metadata_store_pg/connector.py
      --broker_config src/mfi_ddb/retrieval_api/metadata_store_pg/broker.ini
      --pg_config src/mfi_ddb/retrieval_api/metadata_store_pg/pg_database.ini{rws_depends_on}
    networks:
      - mfi_network
    profiles:
      - "rws"
    volumes:
      - ./runtime_configs/metadata_broker.ini:/app/src/mfi_ddb/retrieval_api/metadata_store_pg/broker.ini:ro
      - ./runtime_configs/metadata_pg.ini:/app/src/mfi_ddb/retrieval_api/metadata_store_pg/pg_database.ini:ro
    restart: always

  rws-app:
    image: cmumfi/mfi-ddb-rws-app:latest
    container_name: mfi-rws-app
    ports:
      - "{rws.RWS_API_PORT}:8000"{rws_depends_on}
    networks:
      - mfi_network
    volumes:
      - ./runtime_configs/rws_endpoints.yaml:/app/src/mfi_ddb/retrieval_api/rws/app/config/dws.endpoints.yaml:ro
      - ./runtime_configs/rws_pg.ini:/app/src/mfi_ddb/retrieval_api/rws/app/config/pg_database.ini:ro
    profiles:
      - "rws"
    restart: always

  # ==========================================
  # TIMESCALEDB METRICS LAYER (Profile: ts)
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
    profiles:
      - "ts"
    networks:
      - mfi_network
    restart: always

  timescaledb-connector:
    image: cmumfi/mfi-ddb-timescaledb-connector:latest
    container_name: mfi-timescaledb-connector{extra_hosts_block}{ts_depends_on}
    environment:
      - DB_HOST={ts.TS_DB_HOST}
    volumes:
      - ./runtime_configs/timescale_connector.yaml:/app/config.yaml:ro
    profiles:
      - "ts"
    networks:
      - mfi_network
    restart: on-failure

  timescaledb-dws:
    image: cmumfi/mfi-ddb-timescaledb-dws:latest
    container_name: mfi-timescaledb-dws
    ports:
      - "{ts.TS_DWS_PORT}:50052"{ts_depends_on}
    command: >
      sh -c "python -m mfi_ddb.databases.timescaledb.dws.server --host {ts.TS_DB_HOST}"
    profiles:
      - "ts"
    networks:
      - mfi_network
    restart: always
"""
    (runtime_dir / "docker-compose.yaml").write_text(compose_content)