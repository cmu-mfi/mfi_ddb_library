import pytest
from helpers import now_iso, uid


@pytest.fixture()
def user_and_project(connector):
    """Insert a fresh user and project for each test that needs them."""
    user_id = uid("user_")
    project_name = uid("proj_")
    connector.insert_user((user_id, "test.com"), timestamp=now_iso())
    project_row = connector.insert_project(timestamp=now_iso(), project_name=project_name)
    return user_id, project_row["project_id"]

def test_returns_inserted_row(connector, user_and_project):
    user_id, project_id = user_and_project
    trial_id = uid("trial_")

    row = connector.insert_trial(
        trial_id=trial_id,
        user=(user_id, "test.com"),
        birth_timestamp=now_iso(),
        project_id=project_id,
    )

    assert row["trial_name"] == trial_id
    assert row["user_id"] == user_id
    assert row["project_id"] == project_id

def test_optional_metadata_and_topics_are_persisted(connector, user_and_project):
    user_id, project_id = user_and_project
    metadata = {"sensor": "lidar", "fps": 10}
    topics = ["ros/imu", "ros/camera"]

    row = connector.insert_trial(
        trial_id=uid("trial_"),
        user=(user_id, "test.com"),
        birth_timestamp=now_iso(),
        project_id=project_id,
        metadata=metadata,
        data_topics=topics,
    )

    assert row["metadata"] == metadata
    assert row["data_topics"] == topics

def test_clean_exit_is_persisted(connector, user_and_project):
    user_id, _ = user_and_project

    row = connector.insert_trial(
        trial_id=uid("trial_"),
        user=(user_id, "test.com"),
        birth_timestamp=now_iso(),
        clean_exit=True,
    )

    assert row["clean_exit"] is True

def test_death_timestamp_is_persisted(connector, user_and_project):
    user_id, _ = user_and_project

    row = connector.insert_trial(
        trial_id=uid("trial_"),
        user=(user_id, "test.com"),
        birth_timestamp=now_iso(),
        death_timestamp=now_iso(),
    )

    assert row["death_timestamp"] is not None

def test_upsert_updates_existing_trial(connector, user_and_project):
    user_id, _ = user_and_project
    trial_id = uid("trial_")

    connector.insert_trial(
        trial_id=trial_id,
        user=(user_id, "test.com"),
        birth_timestamp=now_iso(),
        clean_exit=False,
    )

    row = connector.insert_trial(
        trial_id=trial_id,
        user=(user_id, "test.com"),
        birth_timestamp=now_iso(),
        clean_exit=True,
    )

    assert row["clean_exit"] is True
