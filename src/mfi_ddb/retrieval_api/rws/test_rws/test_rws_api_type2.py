"""
Requires:
* A running test PostgreSQL database (pg_database.test.ini)
* A running test MQTT broker (broker.test.ini)
* The connector service running against both of the above

All of the above are configured in docker-compose.test.yml.
"""

import logging
from typing import List, Optional, Tuple

from test_rws.helpers import now_iso

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
    response = rws_client.get("/mfi-ddb/type0")
    assert response.status_code == 200


def test_single_trial(user_factory, trial_factory, rws_client):

    # PUBLISHING MQTT PAYLOAD TO BROKER
    start_time = now_iso()
    test_user = user_factory()
    trial_name = trial_factory(test_user)
    uuids = _get_trial_uuids(rws_client, test_user, start_time, trial_name)

    assert len(uuids) == 1
    
    # REQUESTING RWS API
    request = {
        "trial_uuid": uuids[0],
        "user_id": test_user[0],
        "user_domain": test_user[1],
        "time_start": start_time,
        "time_end": now_iso(),
    }

    response = rws_client.post("/mfi-ddb/type2", json=request)
    if response.status_code != 200:
        logger.error(response.json())

    # ASSERTION TESTS
    assert response.status_code == 200
    
    type2_response = response.json().get("data")
    assert type(type2_response) is dict
    assert type2_response