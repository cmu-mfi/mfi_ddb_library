# Tests

This directory contains pytest-based tests for the DWS server.

## Running Tests

```bash
# From the kv-psql directory
python -m pytest tests/test_dws.py -v
```

## Test Environment

Tests use a temporary PostgreSQL database named `test_mfi_kv` by default. The database connection parameters can be configured via environment variables:

- `TEST_DB_HOST` - Database host (default: localhost)
- `TEST_DB_PORT` - Database port (default: 5432)
- `TEST_DB_NAME` - Database name (default: test_mfi_kv)
- `TEST_DB_USER` - Database user (default: mfi)
- `TEST_DB_PASSWORD` - Database password (default: mfiddb)

## Test Coverage

The test suite covers:

- `GetDataPoint` gRPC method:
  - Exact timestamp match retrieval
  - Closest past retrieval
  - Closest future retrieval
  - Not found handling

- `GetDataRange` gRPC method:
  - Basic range queries
  - Empty range handling
  - Pagination with page_size
  - Multiple topic support