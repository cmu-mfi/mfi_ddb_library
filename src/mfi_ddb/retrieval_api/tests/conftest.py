import psycopg2
import pytest
# /from metadata_store_pg.pg_mds import MdsConnector

'''
# 1. Fixture for the raw psycopg2 connection
@pytest.fixture(scope="session")
def db_connection():
    config = load_config("./metadata_store_pg/pg_database.test.ini")
    conn = psycopg2.connect(**config)
    
    yield conn    
    
    conn.close()

# 2. Fixture for a clean cursor per test (Rollback strategy)
@pytest.fixture(scope="function")
def db_session(db_connection):
    conn = db_connection
    # Start a transaction
    with conn:
        with conn.cursor() as cur:
            yield cur
        # Rollback after the test so data doesn't leak between tests
        conn.rollback()

# 3. Fixture for FastAPI TestClient
@pytest.fixture
def client():
    return TestClient(app)
'''

# @pytest.fixture(scope="session")
# def connector():
#     return MdsConnector(config_path="./metadata_store_pg/pg_database.test.ini")
 