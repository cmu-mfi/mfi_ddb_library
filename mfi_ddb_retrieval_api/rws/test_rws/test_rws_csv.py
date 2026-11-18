"""
data_format=csv for Type 2 (and Type 3 with a unique trial).

No database or DWS needed: the metadata store and the DWS agent are faked.

    cd mfi_ddb_retrieval_api/rws
    pytest -v test_rws/test_rws_csv.py
"""

import csv
import io

import pytest
from fastapi.testclient import TestClient

from app.api.v0 import router as router_module
from app.api.v0.router import get_metadata_reader
from main import app

TRIAL = {
    "uuid": "57be8ff9-238f-4ece-81dc-0c7899008d3a",
    "trial_name": "trial_001",
    "project_id": None,
    "project_name": None,
    "metadata": {"user": {"user_id": "superadmin", "domain": "superadmin"}},
    "data_topics": ["mfi-v1.0-historian/CMU/SC-Nanofab/pump/#"],
    "birth_timestamp": "2026-11-11T16:00:00+00:00",
    "death_timestamp": "2026-11-11T17:00:00+00:00",
    "created_at": "2026-11-11T16:00:00+00:00",
    "updated_at": "2026-11-11T16:00:00+00:00",
}

DATA = {
    "pump/rms_x": [
        {"timestamp": "2026-11-11T16:00:03.080000+00:00", "value": 1.5},
        {"timestamp": "2026-11-11T16:00:03.083000+00:00", "value": 1.6},
    ],
    "pump/unit": [{"timestamp": "2026-11-11T16:00:03.080000+00:00", "value": 'm/s2, "raw"'}],
    "pump/peaks": [{"timestamp": "2026-11-11T16:00:03.080000+00:00", "value": {"f": [1, 2]}}],
}


class FakeReader:
    def get_trial_by_uuid(self, uuid, user_id, user_domain):
        return dict(TRIAL) if uuid == TRIAL["uuid"] else None

    def find_trials(self, **_kwargs):
        return [dict(TRIAL)]


@pytest.fixture
def client(monkeypatch):
    app.dependency_overrides[get_metadata_reader] = lambda: FakeReader()
    monkeypatch.setattr(router_module.DwsAgent, "get_data", lambda topics, start, end, bytes=False: DATA)
    yield TestClient(app)  # no `with`: the real database pool is not opened
    app.dependency_overrides.clear()


def _type2(client, fmt):
    return client.post("/mfi-ddb/type2", json={
        "trial_uuid": TRIAL["uuid"], "user_id": "superadmin", "user_domain": "superadmin",
        "time_start": "2026-11-11T16:00:00Z", "time_end": "2026-11-11T16:01:00Z", "data_format": fmt,
    })


def test_type2_csv_rows_and_headers(client):
    res = _type2(client, "csv")
    assert res.status_code == 200, res.text
    assert res.headers["content-type"].startswith("text/csv")
    assert res.headers["x-trial-uuid"] == TRIAL["uuid"]

    rows = list(csv.reader(io.StringIO(res.text)))
    assert rows[0] == ["topic", "timestamp", "value"]
    assert len(rows) == 1 + 4  # one row per datapoint
    by_topic = {}
    for topic, ts, value in rows[1:]:
        by_topic.setdefault(topic, []).append((ts, value))
    assert by_topic["pump/rms_x"] == [("2026-11-11T16:00:03.080000+00:00", "1.5"),
                                      ("2026-11-11T16:00:03.083000+00:00", "1.6")]
    assert by_topic["pump/unit"][0][1] == 'm/s2, "raw"'        # quoted and escaped
    assert by_topic["pump/peaks"][0][1] == '{"f":[1,2]}'        # JSON values as JSON


def test_type2_json_unchanged(client):
    res = _type2(client, "json")
    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "success" and body["data"]["data"] == DATA


def test_type3_csv_when_unique(client):
    res = client.post("/mfi-ddb/type3", json={
        "time_start": "2026-11-11T16:00:00Z", "time_end": "2026-11-11T16:01:00Z",
        "user_id": "superadmin", "user_domain": "superadmin", "trial_id": "trial_001", "data_format": "csv",
    })
    assert res.status_code == 200, res.text
    assert res.text.splitlines()[0] == "topic,timestamp,value"
