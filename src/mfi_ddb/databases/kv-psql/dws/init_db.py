#!/usr/bin/env python3
"""
Database initialization script for PostgreSQL Key-Value Store.
Creates the necessary tables for storing MQTT data.
"""

import psycopg2
from psycopg2 import sql
import sys


def init_database(host: str = 'localhost', port: int = 5432,
                   database: str = 'mfi_kv', user: str = 'postgres',
                  password: str = ''):
    """Initialize the database with the required schema."""
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
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
    import yaml
    import argparse
    
    parser = argparse.ArgumentParser(description='Initialize the PostgreSQL database')
    parser.add_argument('--host', default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--database', default='mfi_kv', help='Database name')
    parser.add_argument('--user', default='postgres', help='Database user')
    parser.add_argument('--password', default='', help='Database password')
    
    args = parser.parse_args()
    
    success = init_database(
        host=args.host,
        port=args.port,
        database=args.database,
        user=args.user,
        password=args.password
    )
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()