from datetime import datetime, timezone
import time
import copy
import logging
from typing import Tuple

import pytest
from helpers import get_payload_template, grace_wait, now_iso, publish, uid

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def test_project(client):
    payload = {
        "schema_version": "mfi-v1.0",
        "msg_type": "project",
        "project_name": uid("Artemis "),
        "created_by_user_id": "superadmin",
        "created_by_domain": "superadmin",
        "timestamp": now_iso()        
    }
    
    publish(client, payload)
    grace_wait()
    return payload["project_name"]

@pytest.fixture(scope="session")
def admin_user(client, test_project):
    # PUBLISH DATA
    payload = {
        "schema_version": "mfi-v1.0",
        "msg_type": "user",
        "user_id": uid("neo_"),
        "domain": "matrix",
        "created_by_user_id": "superadmin",
        "created_by_domain": "superadmin",
        "timestamp": now_iso()
    }
    
    publish(client, payload)
    grace_wait()
    
    insert_user_roles(client, 
                      (payload["user_id"], payload["domain"]), 
                      test_project, 
                      "admin")
    
    return(payload["user_id"], payload["domain"])

def insert_user(client, user: Tuple[str,str]):
    # PUBLISH DATA
    payload = {
        "schema_version": "mfi-v1.0",
        "msg_type": "user",
        "user_id": user[0],
        "domain": user[1],
        "created_by_user_id": "superadmin",
        "created_by_domain": "superadmin",
        "timestamp": now_iso()
    }
    
    publish(client, payload)
    grace_wait()

def insert_trial(client, user: Tuple[str, str], time_period_sec: float, trial_name: str = uid("test_")) -> str:
    birth_payload = get_payload_template("birth")
    birth_payload["trial_id"] = trial_name
    birth_payload["time"]["birth"] = now_iso()
    birth_payload["user"]["user_id"] = user[0]
    birth_payload["user"]["domain"] = user[1]

    publish(client, birth_payload)
    time.sleep(time_period_sec)

    death_payload = copy.deepcopy(birth_payload)    
    death_payload["msg_type"] = "death"
    death_payload["time"]["death"] = now_iso()
    
    publish(client, death_payload)    
    grace_wait()
    
    return birth_payload["trial_id"]
    
def insert_user_roles(client, user: Tuple[str, str], project_name: str, role: str):
    
    project_payload = {
        "schema_version": "mfi-v1.0",
        "msg_type": "project",
        "project_name": project_name,
        "user_roles":[
            {
            "user_id": user[0],
            "domain": user[1],
            "role": role
            }
        ],        
        "created_by_user_id": "superadmin",
        "created_by_domain": "superadmin",
        "timestamp": now_iso()        
    }
    
    publish(client, project_payload)
    grace_wait()
    

# ---------------------------------------------------------------------------
# TEST FUNCTIONS
# ---------------------------------------------------------------------------

def test_inserted_tag(client, lookup, test_project, admin_user):
    # INSERT PRE-REQ DATA
    # --------------------------------------------
    start_time = now_iso()
    project_name = test_project
    trial_name = insert_trial(client, admin_user, 1.0)
    
    # PREPARE TP TAG AND PUBLISH
    # --------------------------------------------
    payload = {
        "schema_version": "mfi-v1.0",
        "msg_type": "tp-tag",
        "trial_id": trial_name,
        "project_name": project_name,
        "time_start": start_time,
        "time_end": now_iso(),
        "trial_user_id": admin_user[0],
        "trial_user_domain": admin_user[1],
        "created_by_user_id": admin_user[0],
        "created_by_domain": admin_user[1],
        "timestamp": now_iso()        
    }
    
    publish(client, payload)
    grace_wait()

    # LOOKUP AND ASSERTS
    # --------------------------------------------
    project_row = lookup("project", {"project_name":project_name})
    trial_row = lookup("trial", {"trial_name": trial_name})
    
    assert len(trial_row)==1
    assert trial_row[0]["project_id"]==project_row[0]["project_id"]

def test_unauthorized_user_tag(client, lookup, test_project, admin_user):
    # INSERT PRE-REQ DATA
    # --------------------------------------------
    start_time = now_iso()
    unauthorized_user = (uid("trinity_"), "matrix")
    project_name = test_project
    insert_user(client, unauthorized_user)
    trial_name = insert_trial(client, admin_user, 1.0)
    
    # PREPARE TP TAG AND PUBLISH
    # --------------------------------------------
    payload = {
        "schema_version": "mfi-v1.0",
        "msg_type": "tp-tag",
        "trial_id": trial_name,
        "project_name": project_name,
        "time_start": start_time,
        "time_end": now_iso(),
        "trial_user_id": admin_user[0],
        "trial_user_domain": admin_user[1],
        "created_by_user_id": unauthorized_user[0],
        "created_by_domain": unauthorized_user[1],
        "timestamp": now_iso()        
    }
    
    publish(client, payload)
    grace_wait()

    # LOOKUP AND ASSERTS
    # --------------------------------------------
    project_row = lookup("project", {"project_name":project_name})
    row = lookup("trial", {"trial_name": trial_name})
    
    assert len(row)==1
    assert row[0]["project_id"]!=project_row[0]["project_id"]    
   
def test_tag_the_right_trial(client, lookup, test_project, admin_user):
    # INSERT PRE-REQ DATA
    # --------------------------------------------
    start_time = now_iso()
    project_name = test_project
    
    trial_1_name = insert_trial(client, admin_user, 1.0)
    trial_time = now_iso()
    trial_2_name = insert_trial(client, admin_user, 1.0, trial_1_name)

    # PREPARE TP TAG AND PUBLISH
    # --------------------------------------------
    payload = {
        "schema_version": "mfi-v1.0",
        "msg_type": "tp-tag",
        "trial_id": trial_1_name,
        "project_name": project_name,
        "time_start": start_time,
        "time_end": trial_time,
        "trial_user_id": admin_user[0],
        "trial_user_domain": admin_user[1],
        "created_by_user_id": admin_user[0],
        "created_by_domain": admin_user[1],
        "timestamp": now_iso()        
    }
    
    publish(client, payload)
    grace_wait()

    # LOOKUP AND ASSERTS
    # --------------------------------------------
    project_row = lookup("project", {"project_name":project_name})
    trial_1_row = lookup("trial", {
        "trial_name": trial_1_name,
        "death_timestamp": ('<', trial_time)
    })
    trial_2_row = lookup("trial", {
        "trial_name": trial_2_name,
        "birth_timestamp": ('>', trial_time)
    })
    
    assert len(trial_1_row)==1
    assert len(trial_2_row)==1
    assert trial_1_row[0]["project_id"]==project_row[0]["project_id"]
    assert trial_2_row[0]["project_id"] is None       

def test_trial_user_update(client, lookup, test_project):
    # INSERT PRE-REQ DATA
    # --------------------------------------------
    start_time = now_iso()
    operator = (uid("cypher_"), "matrix")
    project_name = test_project
    insert_user(client, operator)
    trial_name = insert_trial(client, operator, 1.0)
    
    # PREPARE TP TAG AND PUBLISH
    # --------------------------------------------
    payload = {
        "schema_version": "mfi-v1.0",
        "msg_type": "tp-tag",
        "trial_id": trial_name,
        "project_name": project_name,
        "time_start": start_time,
        "time_end": now_iso(),
        "trial_user_id": operator[0],
        "trial_user_domain": operator[1],
        "created_by_user_id": operator[0],
        "created_by_domain": operator[1],
        "timestamp": now_iso()        
    }
    
    publish(client, payload)
    grace_wait()

    # LOOKUP AND ASSERTS
    # --------------------------------------------
    project_row = lookup("project", {"project_name":project_name})
    row = lookup("trial", {"trial_name": trial_name})
    
    assert len(row)==1
    assert row[0]["project_id"]==project_row[0]["project_id"]  

def test_maintainer_user_update(client, lookup, test_project):
    # INSERT PRE-REQ DATA
    # --------------------------------------------
    start_time = now_iso()
    operator = (uid("cypher_"), "matrix")
    maintainer = (uid("morpheus_"), "matrix")
    project_name = test_project
    insert_user(client, operator)
    insert_user(client, maintainer)
    insert_user_roles(client, maintainer, project_name, "maintainer")
    trial_name = insert_trial(client, operator, 1.0)
    
    # PREPARE TP TAG AND PUBLISH
    # --------------------------------------------
    payload = {
        "schema_version": "mfi-v1.0",
        "msg_type": "tp-tag",
        "trial_id": trial_name,
        "project_name": project_name,
        "time_start": start_time,
        "time_end": now_iso(),
        "trial_user_id": operator[0],
        "trial_user_domain": operator[1],
        "created_by_user_id": maintainer[0],
        "created_by_domain": maintainer[1],
        "timestamp": now_iso()        
    }
    
    publish(client, payload)
    grace_wait()

    # LOOKUP AND ASSERTS
    # --------------------------------------------
    project_row = lookup("project", {"project_name":project_name})
    row = lookup("trial", {"trial_name": trial_name})
    
    assert len(row)==1
    assert row[0]["project_id"]==project_row[0]["project_id"]