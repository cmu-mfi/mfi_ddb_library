import pytest
import uuid
from helpers import uid, now_iso

@pytest.fixture()
def user_and_project(connector):
    user_id = uid("user_")
    project_name = uid("proj_")
    connector.insert_user((user_id, "test.com"), timestamp=now_iso())
    project_row = connector.insert_project(timestamp=now_iso(), project_name=project_name)
    project_id = project_row["project_id"]
    return user_id, project_id

def test_returns_inserted_row(connector, user_and_project):
    user_id, project_id = user_and_project

    row = connector.insert_user_project_role_linking(
        user_id=user_id,
        domain="test.com",
        project_id=project_id,
        role="admin",
    )

    assert row["user_id"] == user_id
    assert row["project_id"] == project_id
    assert row["role"] == "admin"

def test_db_generates_uuid_when_id_is_omitted(connector, user_and_project):
    user_id, project_id = user_and_project

    row = connector.insert_user_project_role_linking(
        user_id=user_id,
        domain="test.com",
        project_id=project_id,
        role="maintainer",
    )

    assert row.get("id") not in (None, "")

def test_explicit_id_is_persisted(connector, user_and_project):
    user_id, project_id = user_and_project
    explicit_id = str(uuid.uuid4())

    row = connector.insert_user_project_role_linking(
        user_id=user_id,
        domain="test.com",
        project_id=project_id,
        role="admin",
        id=explicit_id,
    )

    assert row["id"] == explicit_id

def test_upsert_updates_existing_role(connector, user_and_project):
    user_id, project_id = user_and_project
    explicit_id = str(uuid.uuid4())

    connector.insert_user_project_role_linking(
        user_id=user_id,
        domain="test.com",
        project_id=project_id,
        role="operator",
        id=explicit_id,
    )

    row = connector.insert_user_project_role_linking(
        user_id=user_id,
        domain="test.com",
        project_id=project_id,
        role="researcher",
        id=explicit_id,
    )

    assert row["role"] == "researcher"

def test_created_at_is_populated(connector, user_and_project):
    user_id, project_id = user_and_project

    row = connector.insert_user_project_role_linking(
        user_id=user_id,
        domain="test.com",
        project_id=project_id,
        role="operator",
    )

    assert row.get("created_at") is not None
