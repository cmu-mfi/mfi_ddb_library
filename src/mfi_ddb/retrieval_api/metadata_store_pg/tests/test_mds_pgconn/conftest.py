"""
Integration tests for MdsConnector public methods.

Requires a running PostgreSQL test database configured via pg_database.test.ini
(or the MDS_DB_CONFIG env var pointing to an alternative ini file).

Run with:
    pytest test_mds_connector.py -v
"""

import pytest

from pg_mds import MdsConnector

@pytest.fixture(scope="session")
def connector():
    return MdsConnector(config_path="pg_database.test.ini")
