import argparse
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
import time

from pg_check_tables import check_tables
from pg_config import load_config
from pg_connect import connect as pg_connect
from pg_create_tables import create_tables
from pg_mds import MdsConnector

from mfi_ddb.topic_families.key_value import KeyValueTopicFamily
from mfi_ddb.streamer.subscriber import Subscriber
from mfi_ddb.utils.script_utils import get_topic_from_config

logger = logging.getLogger(__name__)
mds = None


from prometheus_client import start_http_server
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import set_meter_provider, get_meter  # <-- 1. Added get_meter
from opentelemetry.sdk.metrics import MeterProvider

reader = PrometheusMetricReader()
provider = MeterProvider(metric_readers=[reader])
set_meter_provider(provider)
start_http_server(port=9464, addr="0.0.0.0")
print("Prometheus metrics server listening on port 9464")


# ==========================================
# OPENTELEMETRY CUSTOM METRICS SETUP
# ==========================================
meter = get_meter("mfi.mds.connector")

mqtt_messages_counter = meter.create_counter(
    "mfi_mds_connector_mqtt_received_total",
    description="Total MQTT payloads received by MDS connector"
)

schema_failures_counter = meter.create_counter(
    "mfi_mds_connector_schema_failures_total",
    description="Total validation schema failures on incoming payloads"
)

db_operations_counter = meter.create_counter(
    "mfi_mds_connector_db_operations_total",
    description="Total DB insertions, updates, and lookups executed by transaction type"
)

db_op_duration = meter.create_histogram(
    "mfi_mds_connector_db_duration_seconds",
    description="Database transaction durations executed by MDS Connector"
)
# ==========================================


def callback(config, topic, message):
    print("Received message on topic:", topic)
    
    # --- 2. Track message intake ---
    mqtt_messages_counter.add(1, {"topic": topic})
    
    if mds is None:
        logging.error("MdsConnector not available")
        return
    
    # Convert bytes to string safely if needed
    if isinstance(message, bytes):
        payload_str = message.decode('utf-8')
    else:
        payload_str = message

    try:
        # Before parsing, let's look at what the schema validator says explicitly
        import jsonschema
        validator = KeyValueTopicFamily.get_schema_validator()
        raw_json_dict = json.loads(payload_str)
        
        # Check for errors manually to print them out
        errors = list(validator.iter_errors(raw_json_dict))
        if errors:
            # --- 3. Track validation failures ---
            schema_failures_counter.add(1, {"topic": topic})
            print("\n❌ --- SCHEMA VALIDATION ERRORS FOUND ---")
            for error in errors:
                print(f"Field: {'.'.join(str(p) for p in error.path)}")
                print(f"Error Message: {error.message}")
            print("------------------------------------------\n")

        data: dict = KeyValueTopicFamily.process_message(payload_str)
    except Exception as e:
        logging.info(f"Message payload: {json.dumps(json.loads(payload_str), indent=2)}")
        logging.error(f"Error occurred while processing message on topic {topic}: {str(e)}")
        return

    # SAFETY CHECK
    if data is None:
        logging.error(f"Message dropped: Core library could not parse payload on topic {topic}. Data was returned as None.")
        return

    logging.info(f"Received message of msg_type: {data.get('msg_type')} on topic: {topic}")

    # 1. DETERMINE MESSAGE TYPE
    msg_type = data["msg_type"]

    # 2. UPDATE MDS TABLES BASED ON MESSAGE TYPE
    # Retrieve project information if available in the message, otherwise set to None
    project = data.get("project", {"id": None, "name":None})
    
    if "project_id" in data or "project_name" in data:
        project["id"] = data.get("project_id")
        project["name"] = data.get("project_name")
    
    if "id" not in project:
        project["id"] = None
    if "name" not in project:
        project["name"] = None
    
    # Lookup DB calls are wrapped with telemetry timers
    if project.get("id") is None and project.get("name") is not None:
        start_time = time.perf_counter()
        try:
            project_row: Optional[List[Dict]] = mds._lookup(
                table="project", 
                conditions={"project_name": project["name"]},
            )
            duration = time.perf_counter() - start_time
            db_op_duration.record(duration, {"operation": "lookup_project"})
            db_operations_counter.add(1, {"operation": "lookup_project", "status": "SUCCESS"})
            
            if project_row is not None and len(project_row) == 1:
                project["id"] = project_row[0]["project_id"]     
        except Exception as e:
            duration = time.perf_counter() - start_time
            db_op_duration.record(duration, {"operation": "lookup_project"})
            db_operations_counter.add(1, {"operation": "lookup_project", "status": "ERROR"})
            
    elif project.get("id") is not None:
        start_time = time.perf_counter()
        try:
            project_row: Optional[List[Dict]] = mds._lookup(
                table="project", 
                conditions={"project_id": project["id"]},
            )
            duration = time.perf_counter() - start_time
            db_op_duration.record(duration, {"operation": "lookup_project"})
            db_operations_counter.add(1, {"operation": "lookup_project", "status": "SUCCESS"})
            
            if project_row is not None and len(project_row) == 1:
                project["name"] = project_row[0]["project_name"]
        except Exception as e:
            duration = time.perf_counter() - start_time
            db_op_duration.record(duration, {"operation": "lookup_project"})
            db_operations_counter.add(1, {"operation": "lookup_project", "status": "ERROR"})

    try:
        # --- 4. Instrument writing/mutation operations ---
        if msg_type == "birth":
            metadata_exclude_keys = ["schema_version", "msg_type", "time", "trial_id"]
            
            start_time = time.perf_counter()
            try:
                mds.insert_trial(
                    trial_id=data["trial_id"],
                    user=(data["user"]["user_id"], data["user"]["domain"]),
                    project_id= project["id"] if project else None,
                    birth_timestamp=data["time"]["birth"],
                    metadata={k: v for k, v in data.items() if k not in metadata_exclude_keys},
                    data_topics=data["data_topics"] if "data_topics" in data else None,
                    timestamp=data["time"]["birth"]
                )
                duration = time.perf_counter() - start_time
                db_op_duration.record(duration, {"operation": "birth_insert"})
                db_operations_counter.add(1, {"operation": "birth_insert", "status": "SUCCESS"})
            except Exception as e:
                duration = time.perf_counter() - start_time
                db_op_duration.record(duration, {"operation": "birth_insert"})
                db_operations_counter.add(1, {"operation": "birth_insert", "status": "ERROR"})
                raise e
                
        elif msg_type == "death":
            if "death" not in data["time"]:
                death_timestamp = datetime.now().isoformat()
                clean_exit = False
            else:
                death_timestamp = data["time"]["death"]
                clean_exit = True           
                
            metadata_exclude_keys = ["schema_version", "msg_type", "time", "trial_id"]
            
            start_time = time.perf_counter()
            try:
                mds.update_trial(
                    trial_id=data["trial_id"],
                    user=(data["user"]["user_id"], data["user"]["domain"]),
                    project_id=project["id"] if project else None,
                    death_timestamp=death_timestamp,
                    clean_exit=clean_exit,
                    timestamp=death_timestamp,
                    time_start=data["time"]["birth"],
                    time_end=death_timestamp
                )
                duration = time.perf_counter() - start_time
                db_op_duration.record(duration, {"operation": "death_update"})
                db_operations_counter.add(1, {"operation": "death_update", "status": "SUCCESS"})
            except Exception as e:
                duration = time.perf_counter() - start_time
                db_op_duration.record(duration, {"operation": "death_update"})
                db_operations_counter.add(1, {"operation": "death_update", "status": "ERROR"})
                raise e
                
        elif msg_type == "user":
            start_time = time.perf_counter()
            try:
                mds.insert_user(
                    user=(data["user_id"], data["domain"]),
                    created_by=(data["created_by_user_id"], data["created_by_domain"]),
                    email=data["email"] if "email" in data else "",
                    name=data["name"] if "name" in data else "",
                    timestamp=data["timestamp"]
                )
                duration = time.perf_counter() - start_time
                db_op_duration.record(duration, {"operation": "user_insert"})
                db_operations_counter.add(1, {"operation": "user_insert", "status": "SUCCESS"})
            except Exception as e:
                duration = time.perf_counter() - start_time
                db_op_duration.record(duration, {"operation": "user_insert"})
                db_operations_counter.add(1, {"operation": "user_insert", "status": "ERROR"})
                raise e
                
        elif msg_type == "project":
            exclude_keys = [
                "schema_version", 
                "msg_type", 
                "project_id", 
                "project_name",
                "created_by_user_id",
                "created_by_domain",
                "timestamp"
            ]
            start_time = time.perf_counter()
            try:
                new_project = mds.insert_project(
                    project_id=data["project_id"] if "project_id" in data else None,
                    project_name=data["project_name"] if "project_name" in data else None,
                    created_by=(data["created_by_user_id"], data["created_by_domain"]),
                    timestamp=data["timestamp"],
                    details={k: v for k, v in data.items() if k not in exclude_keys}
                )
                duration = time.perf_counter() - start_time
                db_op_duration.record(duration, {"operation": "project_insert"})
                db_operations_counter.add(1, {"operation": "project_insert", "status": "SUCCESS"})
            except Exception as e:
                duration = time.perf_counter() - start_time
                db_op_duration.record(duration, {"operation": "project_insert"})
                db_operations_counter.add(1, {"operation": "project_insert", "status": "ERROR"})
                raise e
                
            if "user_roles" in data:
                if type(data["user_roles"]) is list:
                    for user in data["user_roles"]:
                        role = user["role"]
                        if role not in ["admin", "operator", "maintainer", "researcher"]:
                            role = "operator"
                        
                        start_time = time.perf_counter()
                        try:
                            mds.insert_user_project_role_linking(
                                user_id=user.get("user_id"),
                                domain=user.get("domain",""),
                                project_id=new_project["project_id"],
                                role=role
                            )
                            duration = time.perf_counter() - start_time
                            db_op_duration.record(duration, {"operation": "role_link_insert"})
                            db_operations_counter.add(1, {"operation": "role_link_insert", "status": "SUCCESS"})
                        except Exception as ex:
                            duration = time.perf_counter() - start_time
                            db_op_duration.record(duration, {"operation": "role_link_insert"})
                            db_operations_counter.add(1, {"operation": "role_link_insert", "status": "ERROR"})
                            raise ex
                            
        elif msg_type == "tp-tag":
            start_time = time.perf_counter()
            try:
                selected_trials = mds._lookup("trial", {
                    "trial_name": data["trial_id"], 
                    "user_id": data["trial_user_id"], 
                    "user_domain": data["trial_user_domain"],
                    "birth_timestamp": ('between', data["time_start"], data["time_end"]) if data["time_start"] and data["time_end"] else None,
                    "death_timestamp": ('between', data["time_start"], data["time_end"]) if data["time_start"] and data["time_end"] else None
                })
                project_roles = mds._lookup("user_project_role_linking", {
                    "project_id": project["id"]
                })
                duration = time.perf_counter() - start_time
                db_op_duration.record(duration, {"operation": "tp_tag_lookups"})
                db_operations_counter.add(1, {"operation": "tp_tag_lookups", "status": "SUCCESS"})
            except Exception as e:
                duration = time.perf_counter() - start_time
                db_op_duration.record(duration, {"operation": "tp_tag_lookups"})
                db_operations_counter.add(1, {"operation": "tp_tag_lookups", "status": "ERROR"})
                raise e
                
            if selected_trials is None or len(selected_trials) != 1:
                logger.error(f"Cannot update the tag for the trial {data['trial_id']}")
                return
            
            authorized_users = [(selected_trials[0]["user_id"], selected_trials[0]["user_domain"])]
            if project_roles is not None:
                authorized_users.extend([(role["user_id"], role["domain"]) for role in project_roles])
            if (data["created_by_user_id"], data["created_by_domain"]) not in authorized_users:
                logger.error(f"User {(data['created_by_user_id'], data['created_by_domain'])} not authorized\
                                to update trial {selected_trials[0]['trial_name']}")
                return
                        
            start_time = time.perf_counter()
            try:
                mds.update_trial(
                    trial_id=data["trial_id"],
                    user=(data["trial_user_id"], data["trial_user_domain"]),
                    project_id=project["id"] if project else None,
                    time_start=data["time_start"],
                    time_end=data["time_end"],
                    timestamp=data["timestamp"]
                )
                duration = time.perf_counter() - start_time
                db_op_duration.record(duration, {"operation": "trial_update"})
                db_operations_counter.add(1, {"operation": "trial_update", "status": "SUCCESS"})
            except Exception as e:
                duration = time.perf_counter() - start_time
                db_op_duration.record(duration, {"operation": "trial_update"})
                db_operations_counter.add(1, {"operation": "trial_update", "status": "ERROR"})
                raise e
                
        elif msg_type == "data":
            pass
        else:
            logging.info(f"Unknown message type: {msg_type} with topic: {topic}")
    except Exception as e:
        logger.error(f"Couldn't process the msg: \n {message}. Error: {str(e)}")


def main(broker_config_path, pg_config_path):
    # CONNECT TO MDS (POSTGRESQL) AND CHECK/CREATE TABLES
    try:        
        if not check_tables(pg_config_path):
            create_tables(pg_config_path)
        
        global mds
        mds = MdsConnector(config_path=pg_config_path)    
    except Exception as e:
        logging.error(f"Failed to initialize MDS Connector. Error: \n {str(e)}")
        return
    
    # LOAD CONFIG
    config_file = broker_config_path
    mqtt_config = load_config(config_file, section="mqtt")

    # INIT A CONNECTION
    try:
        mqtt_sub = Subscriber({"mqtt": mqtt_config})
        logging.info(f"Connected to MQTT broker at {mqtt_config['broker_address']}:{mqtt_config['broker_port']}, client id: {mqtt_sub.client._client_id.decode('utf-8')}")
    except Exception as e:
        logging.error(f"Failed to connect to the MQTT broker. Error: \n {str(e)}")
        return
    
    # SUBSCRIBE TO TOPIC AND SET A CALLBACK
    topic_config = load_config(config_file, section="topic")
    topic = get_topic_from_config(topic_config)
    mqtt_sub.create_message_callback(
        topic, lambda full_topic, message: callback(mqtt_config, full_topic, message)
    )

    mqtt_sub.client.loop_start()
    logging.info(f"Subscribed to topic: {topic}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass

    mqtt_sub.client.loop_stop()
    mqtt_sub.client.disconnect()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    parser = argparse.ArgumentParser(description="MDS Connector")
    parser.add_argument("--broker_config", "-b", type=str, default="broker.ini", help="Path to the configuration file")
    parser.add_argument("--pg_config", "-p", type=str, default="pg_database.ini", help="Path to the DB configuration file")
    args = parser.parse_args()
    
    try:
        pg_conn = pg_connect(args.pg_config)
        logging.info("Successfully connected to PostgreSQL database.")
    except Exception as e:
        logging.error(f"Failed to connect to PostgreSQL database. Error: \n {str(e)}")
        exit(1)
    
    main(args.broker_config, args.pg_config)