import pytest
import psycopg2
from fastapi.testclient import TestClient
from my_app.main import app
from my_app.database import get_db_connection

# 1. Fixture for the raw psycopg2 connection
@pytest.fixture(scope="session")
def db_connection():
    # Setup: Connect to the test database
    conn = psycopg2.connect(
        dbname="test_db", user="user", password="password", host="localhost", port="5433"
    )
    # Create tables (you might use an Alembic migration here instead)
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name TEXT);")
    conn.commit()
    
    yield conn
    
    # Teardown: Close connection
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