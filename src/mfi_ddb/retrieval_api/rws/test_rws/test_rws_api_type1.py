"""
Tests for MdsReader.get_trial_by_uuid and MdsReader.find_trials.

RUN WITH PYTHONPATH=./metadata_store_pg
```
$ PYTHONPATH=./metadata_store_pg pytest -v tests/test_mds_connector/test_pg_mds.py
```

Requires:
* A running test PostgreSQL database (pg_database.test.ini)
* A running test MQTT broker (broker.test.ini)
* The connector service running against both of the above

All of the above are configured in docker-compose.test.yml.
"""

import logging

import pytest  # noqa: F401

from test_rws.helpers import (
    insert_trial,
    insert_user_roles,
    tp_tag_by_project_name,
    uid,
    now_iso
)

logger = logging.getLogger(__name__)


def test_simple(rws_client):
    response = rws_client.get("/mfi-ddb/type0")
    assert response.status_code == 200

def test_single_trial(user_factory, trial_factory, rws_client):
    
    # PUBLISHING MQTT PAYLOAD TO BROKER
    start_time = now_iso()
    test_user = user_factory()
    trial_name = trial_factory(test_user)
    
    # REQUESTING RWS API
    request = {
        'user_id': test_user[0],
        'user_domain': test_user[1],
        'time_start': start_time,
        'time_end': now_iso()    
    }
    
    response = rws_client.post("/mfi-ddb/type1", json = request)
    if response.status_code != 200:
        logger.error(response.json())
    
    # ASSERTION TESTS 
    assert response.status_code == 200
    type1_response = response.json().get("trials")
    assert len(type1_response) == 1
    assert type1_response[0].get("trial_name") == trial_name

def test_multiple_trials(user_factory, trial_factory, rws_client):
    
    # PUBLISHING MQTT PAYLOAD TO BROKER
    start_time = now_iso()
    test_user = user_factory()
    trial1_name = trial_factory(test_user)
    trial2_name = trial_factory(test_user)    
    trial3_name = trial_factory(test_user)    
    
    # REQUESTING RWS API
    request = {
        'user_id': test_user[0],
        'user_domain': test_user[1],
        'time_start': start_time,
        'time_end': now_iso()    
    }
    
    response = rws_client.post("/mfi-ddb/type1", json = request)
    if response.status_code != 200:
        logger.error(response.json())
    
    # ASSERTION TESTS 
    assert response.status_code == 200
    type1_response = response.json().get("trials")
    assert len(type1_response) == 3
    
    # THE TRIALS SHOULD BE ORDERED BY BIRTH TIMESTAMPS
    assert type1_response[2].get("trial_name") == trial1_name
    assert type1_response[1].get("trial_name") == trial2_name    
    assert type1_response[0].get("trial_name") == trial3_name    

def test_access_trials_with_admin_project_role(user_factory, trial_factory, test_project, mqtt_client, rws_client):
    
    # PUBLISHING MQTT PAYLOADS TO BROKER
    user_admin = user_factory()
    user_operator = user_factory()
    
    insert_user_roles(mqtt_client, user_admin, test_project, "admin")
    insert_user_roles(mqtt_client, user_operator, test_project, "operator")
    
    start_time = now_iso()
    trial1 = trial_factory(user_operator)
    trial2 = trial_factory(user_operator) # untagged, so not accessible by other project users
    trial3 = trial_factory(user_operator)
    
    tp_tag_by_project_name(mqtt_client, trial1, test_project, start_time, user_operator)
    tp_tag_by_project_name(mqtt_client, trial2, test_project, start_time, user_operator)
    tp_tag_by_project_name(mqtt_client, trial3, test_project, start_time, user_operator)
    
    # REQUESTING RWS API
    request_admin = {
        'user_id': user_admin[0],
        'user_domain': user_admin[1],
        'time_start': start_time,
        'time_end': now_iso()    
    }    
    
    response_admin = rws_client.post("/mfi-ddb/type1", json = request_admin)
    if response_admin.status_code != 200:
        logger.error(response_admin.json())   
    
    response_admin = response_admin.json().get("trials")
     
    # ASSERTIONS
    assert len(response_admin) == 3
    trial_names = {trial1, trial2, trial3}
    response_trial_names = set([trial["trial_name"] for trial in response_admin])
    assert trial_names == response_trial_names
    
def test_access_trials_with_mixed_project_roles(user_factory, trial_factory, test_project, mqtt_client, rws_client):
    
    # PUBLISHING MQTT PAYLOADS TO BROKER
    user_admin = user_factory()
    user_operator = user_factory()
    
    insert_user_roles(mqtt_client, user_admin, test_project, "admin")
    insert_user_roles(mqtt_client, user_operator, test_project, "operator")
    
    start_time = now_iso()
    trial1 = trial_factory(user_operator)
    trial2 = trial_factory(user_operator) # untagged, so not accessible by other project users  # noqa: F841
    trial3 = trial_factory(user_operator)
    
    tp_tag_by_project_name(mqtt_client, trial1, test_project, start_time, user_operator)
    tp_tag_by_project_name(mqtt_client, trial3, test_project, start_time, user_operator)
    
    # REQUESTING RWS API
    request_admin = {
        'user_id': user_admin[0],
        'user_domain': user_admin[1],
        'time_start': start_time,
        'time_end': now_iso()    
    }    
    
    response_admin = rws_client.post("/mfi-ddb/type1", json = request_admin)
    if response_admin.status_code != 200:
        logger.error(response_admin.json())   
    
    response_admin = response_admin.json().get("trials")

    request_operator = {
        'user_id': user_operator[0],
        'user_domain': user_operator[1],
        'time_start': start_time,
        'time_end': now_iso()    
    }    
    
    response_operator = rws_client.post("/mfi-ddb/type1", json = request_operator)
    if response_operator.status_code != 200:
        logger.error(response_operator.json())
    
    response_operator = response_operator.json().get("trials")
     
    # ASSERTIONS
    assert len(response_admin) == 2
    assert len(response_operator) == 3

def test_find_trial_by_search_terms(user_factory, trial_factory, mqtt_client, rws_client):
    
    # PUBLISHING MQTT PAYLOAD TO BROKER
    start_time = now_iso()
    test_user = user_factory()
    other_trial = trial_factory(test_user)  # noqa: F841
    target_trial_name = insert_trial(
        client = mqtt_client,
        user = test_user,
        time_period_sec= 0.2,
        custom_dict_payload= {
            "attributes": {
                "metal_alloy": uid("steel_")
            } 
        }
    )
    
    # REQUESTING RWS API
    request = {
        'user_id': test_user[0],
        'user_domain': test_user[1],
        'time_start': start_time,
        'time_end': now_iso(),
        'search_terms' : ["steel"]
    }
    
    response = rws_client.post("/mfi-ddb/type1", json = request)
    if response.status_code != 200:
        logger.error(response.json())
    
    # ASSERTION TESTS 
    assert response.status_code == 200
    type1_response = response.json().get("trials")
    
    assert len(type1_response) == 1
    assert type1_response[0].get("trial_name") == target_trial_name

def test_find_trial_by_multiple_search_terms(user_factory, trial_factory, mqtt_client, rws_client):
    
    # PUBLISHING MQTT PAYLOAD TO BROKER
    start_time = now_iso()
    test_user = user_factory()
    other_trial = trial_factory(test_user)  # noqa: F841
    target_trial_name = insert_trial(
        client = mqtt_client,
        user = test_user,
        time_period_sec= 0.2,
        custom_dict_payload= {
            "attributes": {
                "metal_alloy": uid("steel_"),
                "printer": "WAAM",
                "robot": {
                    "mfg": "ABB",
                    "model": "IRB1300"
                }
            } 
        }
    )
    
    # REQUESTING RWS API
    request = {
        'user_id': test_user[0],
        'user_domain': test_user[1],
        'time_start': start_time,
        'time_end': now_iso(),
        'search_terms' : ["steel", "abb", "Robot"]
    }
    
    response = rws_client.post("/mfi-ddb/type1", json = request)
    if response.status_code != 200:
        logger.error(response.json())
    
    # ASSERTION TESTS 
    assert response.status_code == 200
    type1_response = response.json().get("trials")
    
    assert len(type1_response) == 1
    assert type1_response[0].get("trial_name") == target_trial_name
