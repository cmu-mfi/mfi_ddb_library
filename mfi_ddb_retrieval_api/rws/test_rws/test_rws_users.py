"""
Tests for the user directory endpoints (/users).

These replace the metadata store with an in-memory fake, so unlike the other
RWS tests they need no database, broker or connector:

    cd mfi_ddb_retrieval_api/rws
    pytest -v test_rws/test_rws_users.py
"""

import pytest
from fastapi.testclient import TestClient

from app.api.v0.router import get_metadata_reader
from app.services.pg_mds import SYSTEM_USERS
from main import app

USERS = [
    {"user_id": "superadmin", "domain": "superadmin", "name": "Super Admin"},
    {"user_id": "ssterenr", "domain": "ANDREW", "name": "Stephanie Steren-Ruta"},
    {"user_id": "jdoe", "domain": "ANDREW", "name": "Jane Doe"},
    {"user_id": "test_user", "domain": "default", "name": "Test User"},
]


class FakeReader:
    def get_user(self, user_id, domain):
        return next((dict(u) for u in USERS if u["user_id"] == user_id and u["domain"] == domain), None)

    def search_users(self, search=None, limit=25, include_system=False):
        q = (search or "").lower()
        hits = [
            dict(u)
            for u in USERS
            if (include_system or (u["user_id"], u["domain"]) not in SYSTEM_USERS)
            and (not q or any(q in (u[k] or "").lower() for k in ("user_id", "name", "domain")))
        ]
        return hits[:limit]


@pytest.fixture
def client():
    # No `with`: the app lifespan (which opens the real database pool) is not run.
    app.dependency_overrides[get_metadata_reader] = lambda: FakeReader()
    yield TestClient(app)
    app.dependency_overrides.clear()


def as_user(user_id, domain):
    return {"X-User-Id": user_id, "X-User-Domain": domain}


def test_search_returns_public_fields_only(client):
    res = client.get("/mfi-ddb/users", params={"search": "doe"}, headers=as_user("ssterenr", "ANDREW"))
    assert res.status_code == 200, res.text
    users = res.json()["users"]
    assert users == [{"user_id": "jdoe", "domain": "ANDREW", "name": "Jane Doe"}]
    assert "email" not in res.text


def test_search_without_text_lists_users_up_to_limit(client):
    res = client.get("/mfi-ddb/users", params={"limit": 2}, headers=as_user("superadmin", "superadmin"))
    assert res.status_code == 200
    assert len(res.json()["users"]) == 2


def test_get_one_user(client):
    ok = client.get("/mfi-ddb/users/ANDREW/jdoe", headers=as_user("ssterenr", "ANDREW"))
    assert ok.status_code == 200 and ok.json()["user"]["name"] == "Jane Doe"

    missing = client.get("/mfi-ddb/users/ANDREW/nobody", headers=as_user("ssterenr", "ANDREW"))
    assert missing.status_code == 404


def test_requester_header_required(client):
    assert client.get("/mfi-ddb/users").status_code == 401


def test_unregistered_requester_is_refused(client):
    res = client.get("/mfi-ddb/users", headers=as_user("stranger", "ANDREW"))
    assert res.status_code == 403
    res = client.get("/mfi-ddb/users/ANDREW/jdoe", headers=as_user("stranger", "ANDREW"))
    assert res.status_code == 403


def test_limit_is_bounded(client):
    res = client.get("/mfi-ddb/users", params={"limit": 1000}, headers=as_user("superadmin", "superadmin"))
    assert res.status_code == 422


def test_superadmin_hidden_from_search(client):
    res = client.get("/mfi-ddb/users", params={"search": "admin"}, headers=as_user("ssterenr", "ANDREW"))
    assert res.json()["users"] == []
    # still a registered user for checks (e.g. "created by" validation)
    assert client.get("/mfi-ddb/users/superadmin/superadmin", headers=as_user("ssterenr", "ANDREW")).status_code == 200


def test_include_system_only_for_superadmin(client):
    denied = client.get("/mfi-ddb/users", params={"include_system": True}, headers=as_user("ssterenr", "ANDREW"))
    assert denied.status_code == 403
    ok = client.get(
        "/mfi-ddb/users", params={"search": "admin", "include_system": True}, headers=as_user("superadmin", "superadmin")
    )
    assert [u["user_id"] for u in ok.json()["users"]] == ["superadmin"]


def test_users_listed_in_type0(client):
    endpoints = [e["endpoint"] for e in client.get("/mfi-ddb/type0").json()["endpoints"]]
    assert "/users" in endpoints
