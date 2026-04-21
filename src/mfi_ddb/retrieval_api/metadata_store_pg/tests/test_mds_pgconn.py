"""
Integration tests for MdsConnector public methods.

Requires a running PostgreSQL test database configured via pg_database.test.ini
(or the MDS_DB_CONFIG env var pointing to an alternative ini file).

Run with:
    pytest test_mds_connector.py -v
"""

import time
import uuid
from datetime import datetime, timezone

import pytest
from pg_mds import MdsConnector

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def uid(prefix: str = "") -> str:
    """Return a short collision-proof string, optionally prefixed."""
    return f"{prefix}{uuid.uuid4().hex[:10]}"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# insert_user
# ---------------------------------------------------------------------------

class TestInsertUser:
    def test_returns_inserted_row(self, connector):
        user_id = uid("user_")

        row = connector.insert_user(
            (user_id, "test.com"),
            timestamp=now_iso(),
        )

        assert row["user_id"] == user_id
        assert row["domain"] == "test.com"

    def test_optional_email_and_name_are_persisted(self, connector):
        user_id = uid("user_")

        row = connector.insert_user(
            (user_id, "test.com"),
            timestamp=now_iso(),
            email=f"{user_id}@test.com",
            name="Test User",
        )

        assert row["email"] == f"{user_id}@test.com"
        assert row["name"] == "Test User"

    def test_created_by_defaults_to_superadmin(self, connector):
        row = connector.insert_user(
            (uid("user_"), "test.com"),
            timestamp=now_iso(),
        )

        assert row["created_by_user_id"] == "superadmin"
        assert row["created_by_domain"] == "superadmin"

    def test_custom_created_by_is_persisted(self, connector):
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

    def test_upsert_updates_existing_user(self, connector):
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

    def test_created_at_is_populated(self, connector):
        row = connector.insert_user(
            (uid("user_"), "test.com"),
            timestamp=now_iso(),
        )

        assert row.get("created_at") is not None

    def test_returns_empty_dict_when_domain_is_none(self, connector):
        # domain is NOT NULL in schema — should fail gracefully
        result = connector.insert_user(
            (uid("user_"), None),
            timestamp=now_iso(),
        )

        assert result == {}


# ---------------------------------------------------------------------------
# insert_project
# ---------------------------------------------------------------------------

class TestInsertProject:
    def test_insert_project_row(self, connector):
        details = {"env": "test", "version": 2}
        
        row = connector.insert_project(
            timestamp=now_iso(),
            project_name="Auto ID Project",
            details=details
        )

        assert row.get("project_id") not in (None, "")
        assert row["details"] == details
        assert row["created_by_user_id"] == "superadmin"

    def test_custom_created_by_is_persisted(self, connector):
        creator_id = uid("creator_")
        connector.insert_user((creator_id, "test.com"), timestamp=now_iso())

        row = connector.insert_project(
            timestamp=now_iso(),
            project_name=uid("proj_"),
            created_by=(creator_id, "test.com"),
        )

        assert row["created_by_user_id"] == creator_id

    def test_upsert_updates_existing_project(self, connector):
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

# ---------------------------------------------------------------------------
# insert_trial
# ---------------------------------------------------------------------------

class TestInsertTrial:
    @pytest.fixture()
    def user_and_project(self, connector):
        """Insert a fresh user and project for each test that needs them."""
        user_id = uid("user_")
        project_name = uid("proj_")
        connector.insert_user((user_id, "test.com"), timestamp=now_iso())
        project_row = connector.insert_project(timestamp=now_iso(), project_name=project_name)
        return user_id, project_row["project_id"]

    def test_returns_inserted_row(self, connector, user_and_project):
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

    def test_optional_metadata_and_topics_are_persisted(self, connector, user_and_project):
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

    def test_clean_exit_is_persisted(self, connector, user_and_project):
        user_id, _ = user_and_project

        row = connector.insert_trial(
            trial_id=uid("trial_"),
            user=(user_id, "test.com"),
            birth_timestamp=now_iso(),
            clean_exit=True,
        )

        assert row["clean_exit"] is True

    def test_death_timestamp_is_persisted(self, connector, user_and_project):
        user_id, _ = user_and_project

        row = connector.insert_trial(
            trial_id=uid("trial_"),
            user=(user_id, "test.com"),
            birth_timestamp=now_iso(),
            death_timestamp=now_iso(),
        )

        assert row["death_timestamp"] is not None

    def test_upsert_updates_existing_trial(self, connector, user_and_project):
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


# ---------------------------------------------------------------------------
# update_trial
# ---------------------------------------------------------------------------

class TestUpdateTrial:
    @pytest.fixture()
    def existing_trial(self, connector):
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

    def test_updates_death_timestamp(self, connector, existing_trial):
        user_id, _, trial_id, _ = existing_trial
        
        row = connector.update_trial(
            trial_id=trial_id,
            user=(user_id, "test.com"),
            death_timestamp=now_iso(),
        )

        assert row["trial_name"] == trial_id
        assert row["death_timestamp"] is not None

        assert True
        
    def test_updates_clean_exit(self, connector, existing_trial):
        user_id, _, trial_id, _ = existing_trial

        row = connector.update_trial(
            trial_id=trial_id,
            user=(user_id, "test.com"),
            clean_exit=True,
        )

        assert row["clean_exit"] is True

    def test_updates_metadata(self, connector, existing_trial):
        user_id, _, trial_id, _ = existing_trial
        new_meta = {"updated": True, "reason": "test"}

        row = connector.update_trial(
            trial_id=trial_id,
            user=(user_id, "test.com"),
            metadata=new_meta,
        )

        assert row["metadata"] == new_meta

    def test_returns_empty_dict_when_trial_not_found(self, connector):
        result = connector.update_trial(
            trial_id="nonexistent_trial_xyz",
            user=("nobody", "test.com"),
        )

        assert result == {}

    def test_returns_empty_dict_when_time_range_excludes_trial(self, connector, existing_trial):
        user_id, _, trial_id, _ = existing_trial

        result = connector.update_trial(
            trial_id=trial_id,
            user=(user_id, "test.com"),
            time_start="2000-01-01T00:00:00+00:00",
            time_end="2000-01-02T00:00:00+00:00",
        )

        assert result == {}

    def test_time_range_matching_trial_allows_update(self, connector, existing_trial):
        user_id, _, trial_id, _ = existing_trial

        row = connector.update_trial(
            trial_id=trial_id,
            user=(user_id, "test.com"),
            clean_exit=True,
            time_start="2000-01-01T00:00:00+00:00",
            time_end="2999-12-31T23:59:59+00:00",
        )

        assert row["clean_exit"] is True

    def test_updated_at_changes_after_update(self, connector, existing_trial):
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


# ---------------------------------------------------------------------------
# insert_user_project_role_linking
# ---------------------------------------------------------------------------

class TestInsertUserProjectRoleLinking:
    @pytest.fixture()
    def user_and_project(self, connector):
        user_id = uid("user_")
        project_name = uid("proj_")
        connector.insert_user((user_id, "test.com"), timestamp=now_iso())
        project_row = connector.insert_project(timestamp=now_iso(), project_name=project_name)
        project_id = project_row["project_id"]
        return user_id, project_id

    def test_returns_inserted_row(self, connector, user_and_project):
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

    def test_db_generates_uuid_when_id_is_omitted(self, connector, user_and_project):
        user_id, project_id = user_and_project

        row = connector.insert_user_project_role_linking(
            user_id=user_id,
            domain="test.com",
            project_id=project_id,
            role="maintainer",
        )

        assert row.get("id") not in (None, "")

    def test_explicit_id_is_persisted(self, connector, user_and_project):
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

    def test_upsert_updates_existing_role(self, connector, user_and_project):
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

    def test_created_at_is_populated(self, connector, user_and_project):
        user_id, project_id = user_and_project

        row = connector.insert_user_project_role_linking(
            user_id=user_id,
            domain="test.com",
            project_id=project_id,
            role="operator",
        )

        assert row.get("created_at") is not None