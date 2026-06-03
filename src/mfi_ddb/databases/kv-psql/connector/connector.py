#!/usr/bin/env python3
"""
MQTT Connector for PostgreSQL Key-Value Store
Subscribes to MQTT topics and writes data to PostgreSQL database.
"""

import argparse
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
import psycopg2
import yaml
from psycopg2 import sql, extensions
from psycopg2.extras import Json

logger = logging.getLogger(__name__)

class MQTTConnector:
    def __init__(self, config: dict, debug: bool = False):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self._setup_logging(debug)
        
        # MQTT configuration
        self.mqtt_config = config.get('mqtt', {})
        self.mqtt_broker = self.mqtt_config.get('broker', 'localhost')
        self.mqtt_port = self.mqtt_config.get('port', 1883)
        self.mqtt_username = self.mqtt_config.get('username')
        self.mqtt_password = self.mqtt_config.get('password')
        self.mqtt_topics = self.mqtt_config.get('topics', ['mfi-v1.0-#'])
        
        # PostgreSQL configuration
        self.postgres_config = config.get('postgres', {})
        self.db_conn: Optional[extensions.connection] = None
        
        self.running = False
        
    def _setup_logging(self, debug: bool = False):
        """Configure logging format and level."""
        level = logging.DEBUG if debug else logging.INFO
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def connect_database(self) -> bool:
        """Establish connection to PostgreSQL database."""
        try:
            self.db_conn = psycopg2.connect(
                host=self.postgres_config.get('host', 'localhost'),
                port=self.postgres_config.get('port', 5432),
                database=self.postgres_config.get('database', 'mfi_kv'),
                user=self.postgres_config.get('user', 'postgres'),
                password=self.postgres_config.get('password', '')
            )
            self.db_conn.autocommit = True
            self.logger.info("Connected to PostgreSQL database")
            return True
        except psycopg2.Error as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False
    
    def disconnect_database(self):
        """Close the database connection."""
        if self.db_conn:
            self.db_conn.close()
            self.logger.info("Disconnected from PostgreSQL database")
    
    # def create_table_if_not_exists(self):
    #     """Create the kv_data table if it doesn't exist."""
    #     with self.db_conn.cursor() as cursor:
    #         cursor.execute("""
    #             CREATE TABLE IF NOT EXISTS kv_data (
    #                 timestamp TIMESTAMPTZ NOT NULL,
    #                 topic TEXT NOT NULL,
    #                 payload JSONB NOT NULL
    #             )
    #         """)
    #         self.logger.info("Table 'kv_data' ready")
    def create_table_if_not_exists(self):
        """Create the kv_data table and indexes if they don't exist."""
        
        # to show db_conn is not none
        if not self.db_conn:
            self.logger.error("Database connection is missing. Cannot create tables.")
            return
        
        with self.db_conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS kv_data (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    topic TEXT NOT NULL,
                    payload JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            
            create_index_queries = [
                "CREATE INDEX IF NOT EXISTS idx_kv_data_timestamp ON kv_data(timestamp DESC);",
                "CREATE INDEX IF NOT EXISTS idx_kv_data_topic ON kv_data(topic);",
                "CREATE INDEX IF NOT EXISTS idx_kv_data_topic_timestamp ON kv_data(topic, timestamp DESC);",
                "CREATE INDEX IF NOT EXISTS idx_kv_data_created_at ON kv_data(created_at);"
            ]
            for query in create_index_queries:
                cursor.execute(query)
                
        self.logger.info("Table 'kv_data' and performance indexes verified and ready")
    
    def store_data_point(self, topic: str, payload: dict):
        """Store a single data point in the database."""
        if not self.db_conn:
            self.logger.error("Database connection not established")
            return False
        
        timestamp = datetime.now(timezone.utc)
        
        try:
            with self.db_conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO kv_data (timestamp, topic, payload)
                    VALUES (%s, %s, %s)
                    """,
                    (timestamp, topic, Json(payload))
                )
            # self.logger.debug(f"Stored data point: topic={topic}")
            self.logger.info(f"Stored data point: topic={topic}")
            return True
        except psycopg2.Error as e:
            self.logger.error(f"Failed to store data point: {e}")
            return False
    
    def on_connect(self, client: mqtt.Client, userdata, flags, rc, properties=None):
        """Callback for when the client connects to the broker."""
        if rc == 0:
            self.logger.info("Connected to MQTT broker")
            for topic in self.mqtt_topics:
                client.subscribe(topic)
                self.logger.info(f"Subscribed to topic: {topic}")
        else:
            self.logger.error(f"Failed to connect to MQTT broker, code: {rc}")
    
    def on_message(self, client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
        """Callback for when a message is received from the broker."""
        try:
            # Try to parse payload as JSON
            try:
                payload = json.loads(msg.payload.decode('utf-8'))
            except json.JSONDecodeError:
                # If not JSON, store as string value
                payload = {"value": msg.payload.decode('utf-8')}
            
            self.store_data_point(msg.topic, payload)
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
    
    def start(self):
        """Start the MQTT connector."""
        self.running = True
        
        # Set up MQTT client
        self.mqtt_client = mqtt.Client(
            client_id=f"kv-psql-connector-{int(time.time())}",
            callback_api_version=CallbackAPIVersion.VERSION2
        )
        
        if self.mqtt_username and self.mqtt_password:
            self.mqtt_client.username_pw_set(self.mqtt_username, self.mqtt_password)
        
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        
        # Connect to MQTT broker
        try:
            self.mqtt_client.connect(self.mqtt_broker, self.mqtt_port, 60)
        except Exception as e:
            self.logger.error(f"Failed to connect to MQTT broker: {e}")
            return
        
        # Connect to database
        if not self.connect_database():
            self.logger.error("Failed to connect to database, exiting")
            return
        
        # Ensure table exists
        self.create_table_if_not_exists()
        
        # Handle shutdown signals
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        # Start MQTT loop
        self.mqtt_client.loop_start()
        self.logger.info("MQTT Connector started")
        
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()
    
    def stop(self):
        """Stop the MQTT connector."""
        self.logger.info("Shutting down MQTT connector...")
        self.running = False
        
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
        
        self.disconnect_database()
        self.logger.info("MQTT Connector stopped")
    
    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals."""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.stop()


def load_config(config_path: str) -> dict:
    """Load configuration from YAML file."""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.error(f"Config file not found: {config_path}")
        return {}
    except yaml.YAMLError as e:
        logger.error(f"Error parsing config file: {e}")
        return {}


def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='MQTT Connector for PostgreSQL')
    parser.add_argument('-v', '--verbose', action='store_true', 
                        help='Enable debug logging')
    args = parser.parse_args()
    
    # Load configuration
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config = load_config(os.path.join(script_dir,'config.yaml'))
    
    if not config:
        logger.error("Failed to load configuration")
        sys.exit(1)
    
    # Create and start connector
    connector = MQTTConnector(config, debug=args.verbose)
    connector.start()


if __name__ == "__main__":
    main()