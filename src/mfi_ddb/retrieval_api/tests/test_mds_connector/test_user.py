import logging

import pytest
from helpers import get_payload_template, grace_wait, now_iso, publish, uid

logger = logging.getLogger(__name__)

def test_inserted_user(client, lookup):
    # PUBLISH DATA
    payload = get_payload_template("user")
    payload["user_id"] = uid("user_")
    payload["domain"] = "test.com"
    payload["created_by_user_id"] = "superadmin"
    payload["created_by_domain"] = "superadmin"
    
    publish(client, payload)
    grace_wait()
    
    # LOOKUP AND ASSERT THE VALUES
    row = lookup(
        "ddb_user",
        {
            "user_id":payload["user_id"]
        }
    )
    
    assert len(row) == 1
    assert row[0]["user_id"] == payload["user_id"]
