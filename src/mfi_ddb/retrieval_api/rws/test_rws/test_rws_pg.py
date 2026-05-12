"""
Requires:
* A running test PostgreSQL database (pg_database.test.ini)
* A running test MQTT broker (broker.test.ini)
* The connector service running against both of the above

All of the above are configured in docker-compose.test.yml.
"""

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Tuple

import pytest

from app.services.pg_mds import MdsReader
from test_rws.helpers import (
    insert_trial,
    insert_user,
    insert_user_roles,
    uid,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def mds():
    return MdsReader(config_file="pg_database.test.ini")


@pytest.fixture(scope="session")
def base_project(test_project):
    return test_project


@pytest.fixture(scope="session")
def base_user(mqtt_client, test_project) -> Tuple[str, str]:
    """Single reusable user for the whole test session."""
    user = (uid("usr_"), "testdomain.io")
    insert_user(mqtt_client, user)
    insert_user_roles(mqtt_client, user, test_project, "admin")
    return user


def _get_trial_uuid(mds: MdsReader, trial_name: str) -> str:
    """Resolve a trial_name to its UUID primary key via _lookup."""
    rows = mds._lookup("trial", {"trial_name": trial_name})
    assert rows, f"Trial '{trial_name}' not found in DB"
    return str(rows[0]["uuid"])


# ---------------------------------------------------------------------------
# get_trial_by_uuid
# ---------------------------------------------------------------------------


class TestGetTrialByUuid:
    def test_returns_dict_for_known_trial(self, mds, trial_factory, superuser):
        trial_name = trial_factory()
        trial_uuid = _get_trial_uuid(mds, trial_name)

        result = mds.get_trial_by_uuid(trial_uuid, superuser[0], superuser[1])

        assert result is not None
        assert isinstance(result, dict)
        assert str(result["uuid"]) == trial_uuid
        assert result["trial_name"] == trial_name

    def test_returned_dict_has_expected_keys(self, mds, trial_factory, superuser):
        trial_uuid = _get_trial_uuid(mds, trial_factory())

        result = mds.get_trial_by_uuid(trial_uuid, superuser[0], superuser[1])

        for key in ("uuid", "trial_name", "user_id", "birth_timestamp"):
            assert key in result, f"Missing expected key: '{key}'"

    def test_trial_user_has_access(self, mds, user_factory, mqtt_client):
        user = user_factory()
        trial_name = insert_trial(mqtt_client, user, 0.1)
        trial_uuid = _get_trial_uuid(mds, trial_name)

        result = mds.get_trial_by_uuid(trial_uuid, user[0], user[1])

        assert result is not None
        assert isinstance(result, dict)
        assert str(result["uuid"]) == trial_uuid
        assert result["trial_name"] == trial_name

    def test_admin_user_has_access(self, mds, user_factory, test_project, mqtt_client):
        project_name = test_project
        user_admin = user_factory()
        user_operator = user_factory()
        insert_user_roles(mqtt_client, user_admin, project_name, "admin")
        trial_name = insert_trial(mqtt_client, user_operator, 0.2, "", project_name)
        trial_uuid = _get_trial_uuid(mds, trial_name)

        result = mds.get_trial_by_uuid(trial_uuid, user_admin[0], user_admin[1])

        assert result is not None
        assert isinstance(result, dict)
        assert str(result["uuid"]) == trial_uuid
        assert result["trial_name"] == trial_name


# ---------------------------------------------------------------------------
# find_trials
# ---------------------------------------------------------------------------


class TestFindTrials:
    # --- return type ---

    def test_returns_list_of_dicts(self, mds, trial_factory, superuser):
        trial_factory()
        results = mds.find_trials(user_id=superuser[0], user_domain=superuser[1])
        logger.debug(f"RESULTS:::{results}")
        assert all(isinstance(r, dict) for r in results)

    # --- user_id ---

    def test_filter_by_user_id_includes_own_trials(self, mds, user_factory, trial_factory):
        unique_user = user_factory()        
        trial_name = trial_factory(unique_user, 0.1)
        results = mds.find_trials(user_id=unique_user[0], user_domain=unique_user[1])
        assert any(r["trial_name"] == trial_name for r in results)

    # --- trial_id (maps to trial_name column) ---

    def test_filter_by_trial_id_exact_match(self, mds, trial_factory, superuser):
        trial_name = trial_factory()
        results = mds.find_trials(
            trial_id=trial_name, user_id=superuser[0], user_domain=superuser[1]
        )
        assert len(results) == 1
        assert results[0]["trial_name"] == trial_name

    # --- project_id ---

    def test_filter_by_project_id(self, mds, test_project, user_factory, trial_factory):
        project_name = test_project
        unique_user = user_factory()
        trial_name = trial_factory(unique_user, 0.1, project_name=project_name)
        rows = mds._lookup("trial", {"trial_name": trial_name})
        project_id = rows[0].get("project_id")
        if project_id is None:
            pytest.skip("Trial has no project_id")
        results = mds.find_trials(
            project_id=project_id, user_id=unique_user[0], user_domain=unique_user[1]
        )
        assert any(r["trial_name"] == trial_name for r in results)

    # --- project_name (triggers LEFT JOIN) ---

    def test_filter_by_project_name_does_not_raise(
        self, mds, user_factory, mqtt_client, test_project
    ):
        unique_user = user_factory()
        insert_user_roles(mqtt_client, unique_user, test_project, "admin")
        results = mds.find_trials(
            project_name=test_project,
            user_id=unique_user[0],
            user_domain=unique_user[1],
        )
        assert isinstance(results, list)

    # --- time_start + time_end ---

    def test_filter_time_range_includes_trial(self, mds, trial_factory, superuser):
        trial_name = trial_factory(duration=0.2)
        t_start = datetime.now(timezone.utc) - timedelta(minutes=2)
        t_end = datetime.now(timezone.utc) + timedelta(minutes=2)
        results = mds.find_trials(
            time_start=t_start,
            time_end=t_end,
            user_id=superuser[0],
            user_domain=superuser[1],
        )
        assert any(r["trial_name"] == trial_name for r in results)

    def test_filter_time_range_excludes_trial_outside_window(
        self, mds, user_factory, trial_factory
    ):
        unique_user = user_factory()
        trial_name = trial_factory(unique_user, 0.1)
        # Window entirely in the future — the just-created trial must be excluded
        t_start = datetime.now(timezone.utc) + timedelta(days=1)
        t_end = datetime.now(timezone.utc) + timedelta(days=2)
        results = mds.find_trials(
            time_start=t_start,
            time_end=t_end,
            user_id=unique_user[0],
            user_domain=unique_user[1],
        )
        assert all(r["trial_name"] != trial_name for r in results)

    # --- search_terms ---

    def test_search_terms_match_trial_name(self, mds, trial_factory, superuser):
        prefix = uid("srch_")
        trial_name = trial_factory(trial_name=prefix)
        results = mds.find_trials(
            search_terms=[prefix], user_id=superuser[0], user_domain=superuser[1]
        )
        assert any(r["trial_name"] == trial_name for r in results)

    def test_search_terms_no_match_returns_empty(self, mds, superuser):
        results = mds.find_trials(
            search_terms=[uid("zzz_nomatch_")],
            user_id=superuser[0],
            user_domain=superuser[1],
        )
        assert results == []

    def test_multiple_search_terms_all_must_match(self, mds, trial_factory, superuser):
        shared = uid("shared_")
        trial_name = trial_factory(trial_name=f"{shared}_extra")
        results = mds.find_trials(
            search_terms=[shared, "extra"],
            user_id=superuser[0],
            user_domain=superuser[1],
        )
        assert any(r["trial_name"] == trial_name for r in results)

    def test_search_terms_partial_match_excludes_trial(
        self, mds, trial_factory, superuser
    ):
        shared = uid("shared_")
        trial_factory(trial_name=f"{shared}_extra")
        # Second term won't match — trial must be excluded
        results = mds.find_trials(
            search_terms=[shared, uid("nomatch_")],
            user_id=superuser[0],
            user_domain=superuser[1],
        )
        assert all(shared not in r["trial_name"] for r in results)

    # --- combined filters ---

    def test_combined_user_id_and_trial_id(self, mds, user_factory, trial_factory):
        unique_user = user_factory()
        trial_name = trial_factory(user=unique_user, duration=0.1)
        results = mds.find_trials(
            user_id=unique_user[0], user_domain=unique_user[1], trial_id=trial_name
        )
        assert len(results) == 1
        assert results[0]["trial_name"] == trial_name

    def test_combined_user_id_and_time_range(
        self, mds, user_factory, trial_factory
    ):
        unique_user = user_factory()
        trial_name = trial_factory(unique_user, 0.1)
        t_start = datetime.now(timezone.utc) - timedelta(minutes=2)
        t_end = datetime.now(timezone.utc) + timedelta(minutes=2)
        results = mds.find_trials(
            user_id=unique_user[0],
            time_start=t_start,
            time_end=t_end,
            user_domain=unique_user[1],
        )
        assert any(r["trial_name"] == trial_name for r in results)

    def test_combined_user_id_and_search_term(
        self, mds, user_factory, trial_factory
    ):
        unique_user = user_factory()        
        prefix = uid("srch_")
        trial_name = trial_factory(unique_user, 0.1, trial_name=prefix)
        results = mds.find_trials(user_id=unique_user[0], user_domain=unique_user[1], search_terms=[prefix])
        assert any(r["trial_name"] == trial_name for r in results)

    # --- ordering ---

    def test_results_ordered_by_birth_timestamp_desc(
        self, mds, user_factory, trial_factory
    ):
        unique_user = user_factory()
        t1 = trial_factory(unique_user, 0.1)
        time.sleep(0.3)
        t2 = trial_factory(unique_user, 0.1)
        results = mds.find_trials(user_id=unique_user[0], user_domain=unique_user[1])
        names = [r["trial_name"] for r in results]
        assert names.index(t2) < names.index(t1), "Newer trial should appear first"
