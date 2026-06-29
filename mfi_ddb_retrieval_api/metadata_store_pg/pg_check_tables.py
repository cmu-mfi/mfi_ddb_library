import psycopg2
from pg_config import load_config

def check_tables(config_path='pg_database.ini'):
    tables = [
        "ddb_user",
        "project",
        "user_project_role_linking",
        "trial",
        "graph_edges"
    ]
    
    query = \
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name IN %s
        """
    
    try:
        config = load_config(filename=config_path)
        print(f"Checking for tables: {', '.join(tables)} ...")
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (tuple(tables),))
                existing_tables = {row[0] for row in cur.fetchall()}
                
                missing_tables = set(tables) - existing_tables
                if missing_tables:
                    print(f"Missing tables: {', '.join(missing_tables)}")
                    return False
                else:
                    print("All tables exist.")
                    return True

    except (psycopg2.DatabaseError, Exception) as error:
        print(error)
        return False
        
if __name__ == '__main__':
    check_tables()