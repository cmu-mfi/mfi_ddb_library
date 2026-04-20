"""
Configuration Router for MFI DDB Retrieval API

...
"""

import datetime
from dateutil import parser
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import app.schema.schema as schema
from app.services.pg_mds import MdsReader
from fastapi import APIRouter, HTTPException

logger = logging.getLogger(__name__)

# FastAPI router
router = APIRouter()

# psql initialization
metadata_reader = MdsReader()

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
        "trial_uuid": trial_row.get("id"),
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
    ...
    
    return {
        "metadata": metadata,
        "data": {},  # Placeholder for actual data retrieved using DWS agent
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
        }
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
async def search_trials(request: schema.Type1Request):
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
async def get_trial_details(request: schema.Type2Request):
    """
    Retrieve detailed data for a specific trial.

    - **request**: A JSON object containing the trial UUID and other optional parameters for data retrieval.
    """

    trial = metadata_reader.get_trial_by_uuid(request.trial_uuid)
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
async def search_trials_and_get_data(request: schema.Type3Request):
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

    return schema.Type1Response(
        status="success",
        message=f"Found {len(trials)} trial(s).",
        trials=[schema.Type1Response.TrialInfo(**trial) for trial in trials],
    )
