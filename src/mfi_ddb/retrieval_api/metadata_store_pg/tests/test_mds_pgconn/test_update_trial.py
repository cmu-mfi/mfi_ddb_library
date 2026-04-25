import pytest
from helpers import uid, now_iso

@pytest.fixture()
def existing_trial(connector):
    """Insert a user, project, and trial. Return all three IDs and birth ts."""
    user_id = uid("user_")
    project_name = uid("proj_")
    trial_id = uid("trial_")
    birth = now_iso()

    connector.insert_user((user_id, "test.com"), timestamp=now_iso())
    project_row = connector.insert_project(timestamp=now_iso(), project_name=project_name)
    project_id = project_row['project_id']
    connector.insert_trial(
        trial_id=trial_id,
        user=(user_id, "test.com"),
        birth_timestamp=birth,
        project_id=project_id,
    )
    return user_id, project_id, trial_id, birth

def test_updates_death_timestamp(connector, existing_trial):
    user_id, _, trial_id, _ = existing_trial
    
    row = connector.update_trial(
        trial_id=trial_id,
        user=(user_id, "test.com"),
        death_timestamp=now_iso(),
    )

    assert row["trial_name"] == trial_id
    assert row["death_timestamp"] is not None

    assert True
    
def test_updates_clean_exit(connector, existing_trial):
    user_id, _, trial_id, _ = existing_trial

    row = connector.update_trial(
        trial_id=trial_id,
        user=(user_id, "test.com"),
        clean_exit=True,
    )

    assert row["clean_exit"]

def test_updates_metadata(connector, existing_trial):
    user_id, _, trial_id, _ = existing_trial
    new_meta = {"updated": True, "reason": "test"}

    row = connector.update_trial(
        trial_id=trial_id,
        user=(user_id, "test.com"),
        metadata=new_meta,
    )

    assert row["metadata"] == new_meta

def test_returns_empty_dict_when_trial_not_found(connector):
    result = connector.update_trial(
        trial_id="nonexistent_trial_xyz",
        user=("nobody", "test.com"),
    )

    assert result == {}

def test_returns_empty_dict_when_time_range_excludes_trial(connector, existing_trial):
    user_id, _, trial_id, _ = existing_trial

    result = connector.update_trial(
        trial_id=trial_id,
        user=(user_id, "test.com"),
        time_start="2000-01-01T00:00:00+00:00",
        time_end="2000-01-02T00:00:00+00:00",
    )

    assert result == {}

def test_time_range_matching_trial_allows_update(connector, existing_trial):
    user_id, _, trial_id, _ = existing_trial

    row = connector.update_trial(
        trial_id=trial_id,
        user=(user_id, "test.com"),
        clean_exit=True,
        time_start="2000-01-01T00:00:00+00:00",
        time_end="2999-12-31T23:59:59+00:00",
    )

    assert row["clean_exit"] is True

def test_updated_at_changes_after_update(connector, existing_trial):
    user_id, _, trial_id, _ = existing_trial

    original = connector.update_trial(
        trial_id=trial_id,
        user=(user_id, "test.com"),
        clean_exit=False,
    )

    updated = connector.update_trial(
        trial_id=trial_id,
        user=(user_id, "test.com"),
        clean_exit=True,
    )

    assert updated.get("updated_at") >= original.get("updated_at")
