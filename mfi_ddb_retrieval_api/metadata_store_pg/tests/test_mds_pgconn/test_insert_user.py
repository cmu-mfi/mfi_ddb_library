from helpers import uid, now_iso

def test_returns_inserted_row(connector):
    user_id = uid("user_")

    row = connector.insert_user(
        (user_id, "test.com"),
        timestamp=now_iso(),
    )

    assert row["user_id"] == user_id
    assert row["domain"] == "test.com"

def test_optional_email_and_name_are_persisted(connector):
    user_id = uid("user_")

    row = connector.insert_user(
        (user_id, "test.com"),
        timestamp=now_iso(),
        email=f"{user_id}@test.com",
        name="Test User",
    )

    assert row["email"] == f"{user_id}@test.com"
    assert row["name"] == "Test User"

def test_created_by_defaults_to_superadmin(connector):
    row = connector.insert_user(
        (uid("user_"), "test.com"),
        timestamp=now_iso(),
    )

    assert row["created_by_user_id"] == "superadmin"
    assert row["created_by_domain"] == "superadmin"

def test_custom_created_by_is_persisted(connector):
    creator_id = uid("creator_")
    connector.insert_user((creator_id, "test.com"), timestamp=now_iso())

    user_id = uid("user_")
    row = connector.insert_user(
        (user_id, "test.com"),
        timestamp=now_iso(),
        created_by=(creator_id, "test.com"),
    )

    assert row["created_by_user_id"] == creator_id
    assert row["created_by_domain"] == "test.com"

def test_upsert_updates_existing_user(connector):
    user_id = uid("user_")
    connector.insert_user(
        (user_id, "test.com"),
        timestamp=now_iso(),
        name="Original Name",
    )

    row = connector.insert_user(
        (user_id, "test.com"),
        timestamp=now_iso(),
        name="Updated Name",
    )

    assert row["name"] == "Updated Name"

def test_created_at_is_populated(connector):
    row = connector.insert_user(
        (uid("user_"), "test.com"),
        timestamp=now_iso(),
    )

    assert row.get("created_at") is not None

def test_returns_empty_dict_when_domain_is_none(connector):
    # domain is NOT NULL in schema — should fail gracefully
    result = connector.insert_user(
        (uid("user_"), None),
        timestamp=now_iso(),
    )

    assert result == {}
