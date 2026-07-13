#!/usr/bin/env python3
"""
Database initialization script for PostgreSQL Key-Value Store.
Creates the necessary tables for storing MQTT data.
"""

import psycopg2
from psycopg2 import sql, extensions
import sys
import os
import yaml


def load_config(config_path: str) -> dict:
    """Load and validate the configuration from YAML file."""
    if not os.path.exists(config_path):
        print(f"Error: Configuration file not found: {config_path}")
        sys.exit(1)
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    if config is None:
        print("Error: Configuration file is empty or invalid")
        sys.exit(1)
    
    postgres_config = config.get('postgres', {})
    
    # Required fields for PostgreSQL connection
    required_fields = ['host', 'port', 'database', 'user', 'password']
    missing_fields = [field for field in required_fields if field not in postgres_config]
    
    if missing_fields:
        print(f"Error: Missing required PostgreSQL configuration fields: {', '.join(missing_fields)}")
        sys.exit(1)
    
    return postgres_config


def init_database(host: str, port: int, database: str, user: str, password: str):
    """Initialize the database with the required schema."""
    try:
        # Connect to PostgreSQL, added strict type annotation for conn to show it's a psycopg2 connection
        conn: extensions.connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        
        cursor = conn.cursor()
        
        # Create the kv_data table
        create_table_query = """
            CREATE TABLE IF NOT EXISTS kv_data (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                topic TEXT NOT NULL,
                payload JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """
        
        cursor.execute(create_table_query)
        
        # Create indexes for better query performance
        create_index_queries = [
            "CREATE INDEX IF NOT EXISTS idx_kv_data_timestamp ON kv_data(timestamp DESC);",
            "CREATE INDEX IF NOT EXISTS idx_kv_data_topic ON kv_data(topic);",
            "CREATE INDEX IF NOT EXISTS idx_kv_data_topic_timestamp ON kv_data(topic, timestamp DESC);",
            "CREATE INDEX IF NOT EXISTS idx_kv_data_created_at ON kv_data(created_at);"
        ]
        
        for query in create_index_queries:
            cursor.execute(query)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("Database initialized successfully!")
        print("Table 'kv_data' created with the following structure:")
        print("  - id: SERIAL PRIMARY KEY")
        print("  - timestamp: TIMESTAMPTZ (NOT NULL, default NOW())")
        print("  - topic: TEXT (NOT NULL)")
        print("  - payload: JSONB (NOT NULL)")
        print("  - created_at: TIMESTAMPTZ (NOT NULL, default NOW())")
        print()
        print("Indexes created:")
        print("  - idx_kv_data_timestamp")
        print("  - idx_kv_data_topic")
        print("  - idx_kv_data_topic_timestamp")
        print("  - idx_kv_data_created_at")
        
        return True
        
    except psycopg2.Error as e:
        print(f"Error initializing database: {e}")
        return False


def main():
    """Main entry point."""
    # Get the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config.yaml')
    
    postgres_config = load_config(config_path)
    
    success = init_database(
        host=postgres_config['host'],
        port=postgres_config['port'],
        database=postgres_config['database'],
        user=postgres_config['user'],
        password=postgres_config['password']
    )
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()