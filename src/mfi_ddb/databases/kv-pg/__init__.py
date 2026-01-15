"""
Key–Value store connector and retrieval helpers.

The KV store is a simple PostgreSQL table that stores arbitrary JSONB
payloads keyed by `trial_id`. See `schema/kv.json` for the logical
payload schema.
"""


