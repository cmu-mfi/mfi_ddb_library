import copy
import logging

import pytest
from helpers import get_payload_template, grace_wait, now_iso, publish, uid

logger = logging.getLogger(__name__)

def test_inserted_project(client, lookup):
    # PUBLISH DATA
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
    
    # LOOKUP AND ASSERT THE VALUES
    row = lookup(
        "project",
        {
            "project_name":payload["project_name"]
        }
    )
    
    assert len(row) == 1
    assert row[0]["project_name"] == payload["project_name"]

def test_inserted_project_invalid_user(client, lookup):
    # PUBLISH DATA
    payload = {
        "schema_version": "mfi-v1.0",
        "msg_type": "project",
        "project_name": uid("Artemis "),
        "user_roles":[
            {
            "user_id": "john_smith",
            "domain": "Acme.Corp",
            "role": "admin"
            }
        ],        
        "created_by_user_id": "superadmin",
        "created_by_domain": "superadmin",
        "timestamp": now_iso()        
    }
    
    publish(client, payload)
    grace_wait()
    
    # LOOKUP AND ASSERT THE VALUES
    row = lookup(
        "project",
        {
            "project_name":payload["project_name"]
        }
    )
    
    assert len(row) == 1
    assert row[0]["project_name"] == payload["project_name"]
    assert row[0]["details"]["user_roles"] == payload["user_roles"]

def test_inserted_user_roles(client, lookup):
    user_payload = {
        "schema_version": "mfi-v1.0",
        "msg_type": "user",
        "user_id": uid("john_smith_"),
        "domain": "external",
        "created_by_user_id": "superadmin",
        "created_by_domain": "superadmin",
        "timestamp": now_iso()   
    }
    
    project_payload = {
        "schema_version": "mfi-v1.0",
        "msg_type": "project",
        "project_name": uid("Artemis "),
        "user_roles":[
            {
            "user_id": user_payload["user_id"],
            "domain": user_payload["domain"],
            "role": "admin"
            }
        ],        
        "created_by_user_id": "superadmin",
        "created_by_domain": "superadmin",
        "timestamp": now_iso()        
    }
    
    publish(client, user_payload)
    grace_wait()
    publish(client, project_payload)
    grace_wait()
    
    # LOOKUP AND ASSERT THE VALUES
    project_row = lookup(
        "project",
        {"project_name":project_payload["project_name"]}
    )
    user_row = lookup(
        "user_project_role_linking",
        {
            "user_id": user_payload["user_id"],
            "project_id": project_row[0]["project_id"]
        }
    )
    
    assert len(project_row) == 1
    assert len(user_row) == 1
    assert user_row[0]["role"] == "admin"

def test_updated_project(client, lookup):
    # PUBLISH DATA
    payload = {
        "schema_version": "mfi-v1.0",
        "msg_type": "project",
        "project_name": uid("Artemis "),
        "created_by_user_id": "superadmin",
        "created_by_domain": "superadmin",
        "timestamp": now_iso(),        
        "attributes":
            {
                "materials": ["SS103"],
                "machines": [uid("CNC")]
            }
    }
    
    updated_payload = copy.deepcopy(payload)
    updated_payload["attributes"] = {
        "materials": ["SS103"],
        "machines": [uid("CNC")]
    }
    
    publish(client, payload)
    grace_wait()
    publish(client, updated_payload)
    grace_wait()
    
    # LOOKUP AND ASSERT THE VALUES
    row = lookup(
        "project",
        {
            "project_name":payload["project_name"]
        }
    )
    
    assert len(row) == 1
    assert row[0]["details"]["attributes"] == updated_payload["attributes"]