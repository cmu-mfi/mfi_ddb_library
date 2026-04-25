import logging

import pytest
from helpers import get_payload_template, grace_wait, now_iso, publish, uid

logger = logging.getLogger(__name__)

def test_inserted_trial(client, lookup):
    
    # PUBLISH DATA
    payload = get_payload_template("birth")
    payload["trial_id"] = uid("test_")
    payload["time"]["birth"] = now_iso()
    payload["user"]["user_id"] = "superadmin"
    payload["user"]["domain"] = "superadmin"
    
    publish(client, payload)
    
    grace_wait()    

    # LOOKUP AND ASSERT THE VALUES IN MDS
    row = lookup(
        "trial",
        {
            "trial_name": payload["trial_id"]
        }
    )
    
    assert len(row) == 1
    assert row[0]["trial_name"] == payload["trial_id"]