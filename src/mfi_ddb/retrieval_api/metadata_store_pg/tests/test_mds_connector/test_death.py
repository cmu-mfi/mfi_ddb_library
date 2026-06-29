import copy
import logging
from datetime import datetime, timedelta, timezone

import pytest
from helpers import get_payload_template, grace_wait, now_iso, publish, uid

logger = logging.getLogger(__name__)

def test_clean_death(client, lookup):
    # CREATE PAYLOAD
    birth_payload = get_payload_template("birth")
    birth_payload["trial_id"] = uid("test_")
    birth_payload["time"]["birth"] = now_iso()
    birth_payload["user"]["user_id"] = "superadmin"
    birth_payload["user"]["domain"] = "superadmin"        
    death_payload = copy.deepcopy(birth_payload)
    death_payload["msg_type"] = "death"
    death_time = datetime.now(timezone.utc) + timedelta(seconds=30)
    death_payload["time"]["death"]= death_time.isoformat()
    
    # PUBLISH PAYLOAD        
    publish(client, birth_payload)        
    grace_wait()    
    publish(client, death_payload)
    grace_wait()
    
    # LOOKUP AND ASSERT THE VALUES IN MDS
    row = lookup(
        "trial",
        {
            "trial_name": birth_payload["trial_id"]
        }
    )
    
    logger.debug(row[0])
    assert len(row) == 1
    assert row[0]["clean_exit"]   

def test_not_clean_death(client, lookup):
    # CREATE PAYLOAD
    birth_payload = get_payload_template("birth")
    birth_payload["trial_id"] = uid("test_")
    birth_payload["time"]["birth"] = now_iso()
    birth_payload["user"]["user_id"] = "superadmin"
    birth_payload["user"]["domain"] = "superadmin"        
    death_payload = copy.deepcopy(birth_payload)
    death_payload["msg_type"] = "death"
    
    # PUBLISH PAYLOAD        
    publish(client, birth_payload)        
    grace_wait()    
    publish(client, death_payload)
    grace_wait()

    # LOOKUP AND ASSERT THE VALUES IN MDS
    row = lookup(
        "trial",
        {
            "trial_name": birth_payload["trial_id"]
        }
    )
    
    assert len(row) == 1
    assert not row[0]["clean_exit"]    
