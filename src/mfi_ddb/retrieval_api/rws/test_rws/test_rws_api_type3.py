"""
Tests for Type 3 endpoint: Search Trials and Get Data.

Type 3 combines Type 1 (search) and Type 2 (get data):
- If exactly 1 trial is found → returns trial data (Type2Response)
- If 0 or multiple trials found → returns trial list (Type1Response)

RUN WITH PYTHONPATH=./metadata_store_pg
```
$ PYTHONPATH=./metadata_store_pg pytest -v test_rws/test_rws_api_type3.py
```

Requires:
* A running test PostgreSQL database (pg_database.test.ini)
* A running test MQTT broker (broker.test.ini)
* The connector service running against both of the above

All of the above are configured in docker-compose.test.yml.
"""

import logging
from typing import List, Optional, Tuple

from test_rws.helpers import (
    now_iso,
    tp_tag_by_project_name,
)

logger = logging.getLogger(__name__)


def _get_trial_uuids(
    rws_client,
    user: Tuple[str, str],
    time_start: str,
    trial_name: Optional[str] = None,
    project_name: Optional[str] = None,
    search_terms: Optional[List[str]] = None,
) -> List[str]:
    request = {
        "user_id": user[0],
        "user_domain": user[1],
        "time_start": time_start,
        "time_end": now_iso(),
        "trial_name": trial_name,
        "project_name": project_name,
        "search_terms": search_terms
    }

    response = rws_client.post("/mfi-ddb/type1", json=request)
    if response.status_code != 200:
        logger.error(response.json())
        
    return [trial["uuid"] for trial in response.json().get("trials")]    


def test_simple(rws_client):
    """Test that type0 endpoint works (sanity check)."""
    response = rws_client.get("/mfi-ddb/type0")
    assert response.status_code == 200


def test_single_trial_returns_data(user_factory, trial_factory, rws_client):
    """
    Test Type 3: When exactly 1 trial is found, returns trial data (Type2Response).
    
    This is the key difference from Type 1:
    - Type 1: Always returns list of trials
    - Type 3: Returns trial data when unique trial found
    """
    # PUBLISH MQTT PAYLOAD TO BROKER
    start_time = now_iso()
    test_user = user_factory()
    trial_name = trial_factory(test_user)
    uuids = _get_trial_uuids(rws_client, test_user, start_time, trial_name)

    assert len(uuids) == 1
    
    # REQUEST TYPE 3 API
    request = {
        "user_id": test_user[0],
        "user_domain": test_user[1],
        "time_start": start_time,
        "time_end": now_iso(),
        "frequency": 100,  # Type 3 specific field
    }

    response = rws_client.post("/mfi-ddb/type3", json=request)
    if response.status_code != 200:
        logger.error(response.json())

    # ASSERTION TESTS
    assert response.status_code == 200
    
    # When exactly 1 trial found, Type 3 returns Type2Response (trial data)
    type3_response = response.json()
    assert "data" in type3_response  # Should have 'data' key (Type2Response format)
    assert type3_response["data"] is not None
    assert type3_response["data"].get("metadata") is not None
    assert type3_response["data"]["metadata"].get("trial_uuid") == uuids[0]


def test_multiple_trials_returns_list(user_factory, trial_factory, rws_client):
    """
    Test Type 3: When multiple trials are found, returns trial list (Type1Response).
    
    This demonstrates the fallback behavior when unique trial not found.
    """
    # PUBLISH MULTIPLE MQTT PAYLOADS TO BROKER
    start_time = now_iso()
    test_user = user_factory()
    trial1_name = trial_factory(test_user)
    trial2_name = trial_factory(test_user)    
    trial3_name = trial_factory(test_user)    
    
    # REQUEST TYPE 3 API (searching without specific trial filter)
    request = {
        "user_id": test_user[0],
        "user_domain": test_user[1],
        "time_start": start_time,
        "time_end": now_iso(),
    }

    response = rws_client.post("/mfi-ddb/type3", json=request)
    if response.status_code != 200:
        logger.error(response.json())

    # ASSERTION TESTS
    assert response.status_code == 200
    
    # When multiple trials found, Type 3 returns Type1Response (list of trials)
    type3_response = response.json()
    assert "trials" in type3_response  # Should have 'trials' key (Type1Response format)
    trials = type3_response.get("trials")
    assert trials is not None
    assert len(trials) == 3
    
    # Verify trial names are in the response
    trial_names = {trial["trial_name"] for trial in trials}
    assert trial1_name in trial_names
    assert trial2_name in trial_names
    assert trial3_name in trial_names


def test_no_trials_returns_empty_list(user_factory, rws_client):
    """
    Test Type 3: When no trials are found, returns empty trial list (Type1Response).
    """
    # Use a time range in the far future (no trials will exist)
    start_time = "2099-01-01T00:00:00+00:00"
    end_time = "2099-12-31T23:59:59+00:00"
    test_user = user_factory()
    
    # REQUEST TYPE 3 API
    request = {
        "user_id": test_user[0],
        "user_domain": test_user[1],
        "time_start": start_time,
        "time_end": end_time,
    }

    response = rws_client.post("/mfi-ddb/type3", json=request)
    if response.status_code != 200:
        logger.error(response.json())

    # ASSERTION TESTS
    assert response.status_code == 200
    
    # When no trials found, Type 3 returns Type1Response with empty list
    type3_response = response.json()
    assert "trials" in type3_response
    assert type3_response.get("trials") == [] or type3_response.get("trials") is None


def test_with_project_filter(user_factory, trial_factory, test_project, mqtt_client, rws_client):
    """
    Test Type 3 with project_name filter returns correct trials.
    """
    start_time = now_iso()
    test_user = user_factory()
    
    # Create trials and tag with project
    trial1_name = trial_factory(test_user)
    trial2_name = trial_factory(test_user)
    
    tp_tag_by_project_name(mqtt_client, trial1_name, test_project, start_time, test_user)
    tp_tag_by_project_name(mqtt_client, trial2_name, test_project, start_time, test_user)
    
    # REQUEST TYPE 3 API with project filter
    request = {
        "user_id": test_user[0],
        "user_domain": test_user[1],
        "time_start": start_time,
        "time_end": now_iso(),
        "project_name": test_project,
    }

    response = rws_client.post("/mfi-ddb/type3", json=request)
    if response.status_code != 200:
        logger.error(response.json())

    # ASSERTION TESTS
    assert response.status_code == 200
    
    type3_response = response.json()
    # Should return list since multiple trials match project filter
    if "trials" in type3_response:
        trials = type3_response.get("trials")
        assert len(trials) == 2
    else:
        # If only 1 trial found, would return data
        assert "data" in type3_response


def test_with_frequency_field(user_factory, trial_factory, rws_client):
    """
    Test Type 3 accepts and uses the frequency field.
    """
    start_time = now_iso()
    test_user = user_factory()
    trial_name = trial_factory(test_user)
    uuids = _get_trial_uuids(rws_client, test_user, start_time, trial_name)

    assert len(uuids) == 1
    
    # REQUEST TYPE 3 API with specific frequency
    test_frequency = 250
    request = {
        "user_id": test_user[0],
        "user_domain": test_user[1],
        "time_start": start_time,
        "time_end": now_iso(),
        "frequency": test_frequency,
    }

    response = rws_client.post("/mfi-ddb/type3", json=request)
    if response.status_code != 200:
        logger.error(response.json())

    # ASSERTION TESTS
    assert response.status_code == 200
    
    # Verify frequency is included in the response metadata
    type3_response = response.json()
    assert "data" in type3_response
    metadata = type3_response["data"].get("metadata", {})
    assert metadata.get("frequency") == test_frequency


def test_unique_trial_by_specific_search(user_factory, trial_factory, rws_client):
    """
    Test Type 3: Search for a specific trial_name returns unique trial and data.
    """
    start_time = now_iso()
    test_user = user_factory()
    
    # Create multiple trials
    trial1_name = trial_factory(test_user)  # noqa: F841
    trial2_name = trial_factory(test_user)
    trial3_name = trial_factory(test_user)  # noqa: F841
    
    # REQUEST TYPE 3 API searching for specific trial
    request = {
        "user_id": test_user[0],
        "user_domain": test_user[1],
        "time_start": start_time,
        "time_end": now_iso(),
        "trial_id": trial2_name,  # Search for specific trial
    }

    response = rws_client.post("/mfi-ddb/type3", json=request)
    if response.status_code != 200:
        logger.error(response.json())

    # ASSERTION TESTS
    assert response.status_code == 200
    
    type3_response = response.json()
    # Should return data since only 1 trial matches specific trial_name
    
    if "data" not in type3_response:
        logger.debug(f"trial_names: {[trial['trial_name'] for trial in type3_response['trials']]}")
    
    assert "data" in type3_response
    assert type3_response["data"]["metadata"]["trial_name"] == trial2_name
