test-mds:
	PYTHONPATH=./metadata_store_pg python -m pytest -v tests/test_mds_pgconn
	PYTHONPATH=./metadata_store_pg python -m pytest -v tests/test_mds_connector

test-all:
	pytest -v