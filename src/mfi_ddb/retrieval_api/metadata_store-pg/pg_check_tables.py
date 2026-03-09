import psycopg2
from pg_config import load_config

def check_tables():
    tables = [
        "user",
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
        config = load_config()
        print(f"Checking for tables: {', '.join(tables)} ...")
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (tuple(tables),))
                existing_tables = {row[0] for row in cur.fetchall()}
                
                missing_tables = set(tables) - existing_tables
                if missing_tables:
                    print(f"Missing tables: {', '.join(missing_tables)}")
                else:
                    print("All tables exist.")

    except (psycopg2.DatabaseError, Exception) as error:
        print(error)
        
if __name__ == '__main__':
    check_tables()