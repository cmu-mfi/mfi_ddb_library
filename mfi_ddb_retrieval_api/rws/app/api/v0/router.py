"""
Configuration Router for MFI DDB Retrieval API

...
"""

import datetime
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from dateutil import parser
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request

from app.schema import schema
from app.services.pg_mds import MdsReader
from app.services.dws_agent import DwsAgent

logger = logging.getLogger(__name__)

# FastAPI router
router = APIRouter()

# psql initialization
def get_metadata_reader(request: Request):
    """Dependency to get metadata_reader from app state."""
    return request.app.state.metadata_reader

# dws initialization
dws_agent = None  # Placeholder for DWS agent initialization


def _parse_time_value(value: Optional[str], field_name: str) -> Optional[datetime.datetime]:
    if value is None:
        return None
    try:
        time_iso = parser.parse(value).isoformat()        
        return datetime.datetime.fromisoformat(time_iso)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}: {value}")


def _build_trial_payload(trial_row: Dict[str, Any], request: schema.Type2Request) -> Dict[str, Any]:
    start_ts = _parse_time_value(request.time_start, "time_start")
    end_ts = _parse_time_value(request.time_end, "time_end")

    if start_ts is None:
        start_ts = trial_row.get("birth_timestamp") or datetime.datetime(1, 1, 1)
    if end_ts is None:
        end_ts = datetime.datetime.now()

    if isinstance(start_ts, datetime.datetime):
        start_value = start_ts.isoformat()
    else:
        start_value = str(start_ts)

    if isinstance(end_ts, datetime.datetime):
        end_value = end_ts.isoformat()
    else:
        end_value = str(end_ts)

    metadata = {
        "trial_uuid": trial_row.get("uuid"),
        "trial_name": trial_row.get("trial_name"),
        "metadata": trial_row.get("metadata") or {},
        "request_user": (request.user_id, request.user_domain),
        "time_start": start_value,
        "time_end": end_value,
        "frequency": request.frequency,
        "data_format": request.data_format,
        "retrieved_at": datetime.datetime.now().isoformat() + "Z",
    }
    
    # TODO: USE THE METADATA TO GET DATA USING DWS AGENT
    data_topics = trial_row.get("data_topics", [])
    try:
        data = DwsAgent.get_data(data_topics, start_value, end_value)
        logger.info(f"Retrieved data for trial {trial_row.get('uuid')}: {data}")    
    except Exception as e:
        logger.error(f"Error retrieving data for trial {trial_row.get('trial_name')}: {e}")
        data = {}
    
    return {
        "metadata": metadata,
        "data": data,  # Actual data retrieved using DWS agent
    }


def _validate_time_range(start: Optional[datetime.datetime], end: Optional[datetime.datetime]):
    if start is None or end is None:
        return
    if end < start:
        raise HTTPException(status_code=400, detail="time_end must be equal to or after time_start")


@router.get(
    "/type0",
    description=Path("./app/docs/type0.md").read_text(),
    summary="Type 0: Get Endpoints Info",
    response_model=schema.Type0Response,
)
async def list_endpoints():
    """
    List all available endpoints.
    """

    endpoints = [
        {
            "endpoint": "/type0",
            "description": "Get information about the available Retrieval API endpoints.",
        },
        {
            "endpoint": "/type1",
            "description": "Search trials using metadata store filters.",
        },
        {
            "endpoint": "/type2",
            "description": "Retrieve data for a single trial UUID.",
        },
        {
            "endpoint": "/type3",
            "description": "Search trials and retrieve data when a unique trial is found.",
        },
        {
            "endpoint": "/type4",
            "description": "Download files/bytes data. (NOT IMPLEMENTED YET)",
        },
        {
            "endpoint": "/users",
            "description": "Search registered users (user ID, domain, name). Requester in X-User-Id / X-User-Domain headers.",
        },
        {
            "endpoint": "/users/{domain}/{user_id}",
            "description": "Check whether one user is registered.",
        },
    ]

    return schema.Type0Response(
        status="success",
        message="Available endpoints returned.",
        endpoints=[schema.Type0Response.EndpointInfo(**endpoint) for endpoint in endpoints],
    )


@router.post(
    "/type1",
    description=Path("./app/docs/type1.md").read_text(),
    summary="Type 1: Search Trials",
    response_model=schema.Type1Response,
)
async def search_trials(request: schema.Type1Request, metadata_reader: MdsReader = Depends(get_metadata_reader)):
    """
    Search for trials based on provided criteria.

    - **request**: A JSON object containing search criteria for trials.
    """

    time_start = _parse_time_value(request.time_start, "time_start")
    time_end = _parse_time_value(request.time_end, "time_end")
    _validate_time_range(time_start, time_end)

    trials = metadata_reader.find_trials(
        enterprise_id=request.enterprise_id,
        time_start=time_start,
        time_end=time_end,
        user_id=request.user_id,
        user_domain=request.user_domain,
        site=request.site,
        device=request.device,
        trial_id=request.trial_id,
        project_id=request.project_id,
        project_name=request.project_name,
        search_terms=request.search_terms,
    )
    
    for trial in trials:
        for key, value in trial.items():
            if isinstance(value, datetime.datetime):
                trial[key] = value.isoformat()
    
    return schema.Type1Response(
        status="success",
        message=f"Found {len(trials)} trial(s).",
        trials=[schema.Type1Response.TrialInfo(**trial) for trial in trials],
    )


@router.post(
    "/type2",
    description=Path("./app/docs/type2.md").read_text(),
    summary="Type 2: Get Trial Data",
    response_model=schema.Type2Response,
)
async def get_trial_details(request: schema.Type2Request, metadata_reader: MdsReader = Depends(get_metadata_reader)):
    """
    Retrieve detailed data for a specific trial.

    - **request**: A JSON object containing the trial UUID and other optional parameters for data retrieval.
    """

    trial = metadata_reader.get_trial_by_uuid(request.trial_uuid, request.user_id, request.user_domain)
    if not trial:
        raise HTTPException(status_code=404, detail="Trial not found")

    data = _build_trial_payload(trial, request)
    return schema.Type2Response(
        status="success",
        message="Trial data retrieved successfully.",
        data=data,
    )


@router.post(
    "/type3",
    description=Path("./app/docs/type3.md").read_text(),
    summary="Type 3: Search Trials and Get Data",
    response_model=schema.Type3Response,
)
async def search_trials_and_get_data(request: schema.Type3Request, metadata_reader: MdsReader = Depends(get_metadata_reader)):
    """
    Search for trials based on provided criteria and retrieve detailed data for matching trials.

    - **request**: A JSON object containing search criteria for trials and parameters for data retrieval.
    """

    time_start = _parse_time_value(request.time_start, "time_start")
    time_end = _parse_time_value(request.time_end, "time_end")
    _validate_time_range(time_start, time_end)

    trials = metadata_reader.find_trials(
        enterprise_id=request.enterprise_id,
        time_start=time_start,
        time_end=time_end,
        user_id=request.user_id,
        user_domain=request.user_domain,
        site=request.site,
        device=request.device,
        trial_id=request.trial_id,
        project_id=request.project_id,
        project_name=request.project_name,
        search_terms=request.search_terms,
    )

    if len(trials) == 1:
        data = _build_trial_payload(trials[0], request) # type: ignore
        return schema.Type2Response(
            status="success",
            message="Unique trial found and data retrieved.",
            data=data,
        )
        
    for trial in trials:
        for key, value in trial.items():
            if isinstance(value, datetime.datetime):
                trial[key] = value.isoformat()
                
    return schema.Type1Response(
        status="success",
        message=f"Found {len(trials)} trial(s).",
        trials=[schema.Type1Response.TrialInfo(**trial) for trial in trials],
    )


# ---------------------------------------------------------------------------
# Users: directory lookups for "pick a person" lists
# ---------------------------------------------------------------------------


def get_requester(
    x_user_id: Optional[str] = Header(None, description="Requesting user's ID"),
    x_user_domain: Optional[str] = Header(None, description="Requesting user's domain"),
) -> tuple:
    """Who is asking.

    Today the caller states it in the X-User-Id / X-User-Domain headers. With
    single sign-on, the login proxy in front of RWS sets these headers from the
    verified login (and strips any sent by the client), so this function and
    the endpoints using it do not change.
    """
    if not x_user_id or not x_user_id.strip():
        raise HTTPException(status_code=401, detail="Missing X-User-Id header (who is asking).")
    return (x_user_id.strip(), (x_user_domain or "").strip())


def _require_registered(requester: tuple, metadata_reader: MdsReader) -> None:
    """Only users registered in the metadata store may look others up."""
    if requester == ("superadmin", "superadmin"):
        return
    if metadata_reader.get_user(requester[0], requester[1]) is None:
        raise HTTPException(
            status_code=403,
            detail=f"Requester {requester[0]} / {requester[1] or '-'} is not a registered user.",
        )


@router.get(
    "/users",
    summary="Users: Search registered users",
    response_model=schema.UsersResponse,
)
async def search_users(
    search: Optional[str] = Query(None, description="Text to match in user ID, name or domain"),
    limit: int = Query(25, ge=1, le=100, description="Maximum number of users to return"),
    include_system: bool = Query(
        False, description="Also list platform accounts such as superadmin (superadmin requesters only)"
    ),
    requester: tuple = Depends(get_requester),
    metadata_reader: MdsReader = Depends(get_metadata_reader),
):
    """
    Search users registered in the metadata store, for "pick a person" lists
    (e.g. project members, "created by"). Returns only user ID, domain and name.
    Platform accounts (superadmin) are not people to pick and are left out.
    """
    _require_registered(requester, metadata_reader)
    if include_system and requester != ("superadmin", "superadmin"):
        raise HTTPException(status_code=403, detail="Only superadmin can list platform accounts.")
    users = metadata_reader.search_users(search, limit, include_system=include_system)
    return schema.UsersResponse(
        status="success",
        message=f"Found {len(users)} user(s).",
        users=[schema.UserInfo(**u) for u in users],
    )


@router.get(
    "/users/{domain}/{user_id}",
    summary="Users: Check one user",
    response_model=schema.UserResponse,
)
async def get_user(
    domain: str,
    user_id: str,
    requester: tuple = Depends(get_requester),
    metadata_reader: MdsReader = Depends(get_metadata_reader),
):
    """
    Check whether a user is registered. 404 if not.
    """
    _require_registered(requester, metadata_reader)
    user = metadata_reader.get_user(user_id, domain)
    if user is None:
        raise HTTPException(status_code=404, detail=f"User {user_id} / {domain} is not registered.")
    return schema.UserResponse(status="success", message="User found.", user=schema.UserInfo(**user))
