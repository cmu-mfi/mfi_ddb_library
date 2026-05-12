from helpers import now_iso, uid

def test_insert_project_row(connector):
    details = {"env": "test", "version": 2}
    
    row = connector.insert_project(
        timestamp=now_iso(),
        project_name="Auto ID Project",
        details=details
    )

    assert row.get("project_id") not in (None, "")
    assert row["details"] == details
    assert row["created_by_user_id"] == "superadmin"

def test_custom_created_by_is_persisted(connector):
    creator_id = uid("creator_")
    connector.insert_user((creator_id, "test.com"), timestamp=now_iso())

    row = connector.insert_project(
        timestamp=now_iso(),
        project_name=uid("proj_"),
        created_by=(creator_id, "test.com"),
    )

    assert row["created_by_user_id"] == creator_id

def test_upsert_updates_existing_project(connector):
    details = {"env": "test", "version": 2}
    row = connector.insert_project(
        timestamp=now_iso(),
        project_name="Old Name",
        details=details
    )
    project_id = row["project_id"]

    details = {"env": "test", "version": 3}
    row = connector.insert_project(
        timestamp=now_iso(),
        project_id=project_id,
        project_name="Old Name",
        details=details
    )

    assert row["details"]["version"] == 3
