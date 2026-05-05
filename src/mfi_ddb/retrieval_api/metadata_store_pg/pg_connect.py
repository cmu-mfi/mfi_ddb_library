import argparse

import psycopg2
from pg_config import load_config

def connect(pg_config):
    """ Connect to the PostgreSQL database server """
    config = load_config(pg_config)
    try:
        # connecting to the PostgreSQL server
        with psycopg2.connect(**config) as conn:
            print('Connected to the PostgreSQL server.')
            return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="PG Connection Check")
    parser.add_argument("--pg_config", "-p", type=str, default="pg_database.ini", help="Path to the DB configuration file")
    args = parser.parse_args()
        
    connect(args.pg_config)