"""
Configuration Router for MFI DDB Retrieval API

...
"""

import asyncio
import datetime
import logging
import os
import shutil
import time
import zipfile
from pathlib import Path
from typing import Any, Literal, Optional

import psutil
from dateutil import parser
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request

from app.schema import schema
from app.services.dws_agent import DwsAgent
from app.services.pg_mds import MdsReader

logger = logging.getLogger(__name__)

# FastAPI router
router = APIRouter()

# Docs are read at import time; resolve relative to this file so the service
# works regardless of the working directory it is started from.
DOCS_DIR = Path(__file__).resolve().parents[2] / "docs"

# Resource guard defaults for file downloads (type4). These are intentionally
# hardcoded for now; moving them to a configuration file is planned.
MIN_AVAILABLE_MEMORY_MB = 256
MAX_CPU_PERCENT = 90.0
RESOURCE_WAIT_TIMEOUT_S = 5.0
RESOURCE_POLL_INTERVAL_S = 0.5


# psql initialization
def get_metadata_reader(request: Request):
    """Dependency to get metadata_reader from app state."""
    return request.app.state.metadata_reader


def _parse_time_value(
    value: Optional[str], field_name: str
) -> Optional[datetime.datetime]:
    if value is None:
        return None
    try:
        time_iso = parser.parse(value).isoformat()
        return datetime.datetime.fromisoformat(time_iso)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}: {value}")


def _build_trial_payload(
    trial_row: dict[str, Any],
    request: schema.Type2Request | schema.Type4Request,
    bytes: bool = False,
) -> dict[str, Any]:
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

    # USE THE METADATA TO GET DATA USING DWS AGENT
    data_topics = trial_row.get("data_topics", [])
    try:
        data = DwsAgent.get_data(data_topics, start_value, end_value, bytes)
        logger.info(f"Retrieved data for trial {trial_row.get('uuid')}: {data}")
    except Exception as e:
        logger.error(
            f"Error retrieving data for trial {trial_row.get('trial_name')}: {e}"
        )
        data = {}

    return {
        "metadata": metadata,
        "data": data,  # Actual data retrieved using DWS agent
    }


def _validate_time_range(
    start: Optional[datetime.datetime], end: Optional[datetime.datetime]
):
    if start is None or end is None:
        return
    if end < start:
        raise HTTPException(
            status_code=400, detail="time_end must be equal to or after time_start"
        )


async def _check_system_resources() -> None:
    """
    Wait until enough memory and CPU are available to serve a file download.

    File downloads (type4) can pull large file contents from the DWS servers,
    so we refuse to start when the system is already under heavy load. If
    resources stay below the limits for longer than RESOURCE_WAIT_TIMEOUT_S
    seconds, a 503 is raised and the client is asked to retry later.
    """
    # cpu_percent(interval=None) is non-blocking but returns 0.0 on the first
    # call (no baseline yet); it becomes meaningful once the wait loop runs.
    cpu_percent = psutil.cpu_percent(interval=None)
    deadline = time.monotonic() + RESOURCE_WAIT_TIMEOUT_S
    while True:
        available_mb = psutil.virtual_memory().available / (1024 * 1024)
        if available_mb >= MIN_AVAILABLE_MEMORY_MB and cpu_percent < MAX_CPU_PERCENT:
            return

        if time.monotonic() >= deadline:
            raise HTTPException(
                status_code=503,
                detail=(
                    "Not enough system resources to serve the file download. "
                    f"Available memory: {available_mb:.0f} MB "
                    f"(minimum {MIN_AVAILABLE_MEMORY_MB} MB), "
                    f"CPU utilization: {cpu_percent:.1f}% "
                    f"(maximum {MAX_CPU_PERCENT}%). Please retry later."
                ),
            )

        await asyncio.sleep(RESOURCE_POLL_INTERVAL_S)
        cpu_percent = psutil.cpu_percent(interval=None)


@router.get(
    "/type0",
    description=(DOCS_DIR / "type0.md").read_text(),
    summary="Type 0: Get Endpoints Info",
    response_model=schema.Type0Response,
)
async def list_endpoints():
    """
    list all available endpoints.
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
            "description": "Download file/bytes data for a trial.",
        },
    ]

    return schema.Type0Response(
        status="success",
        message="Available endpoints returned.",
        endpoints=[
            schema.Type0Response.EndpointInfo(**endpoint) for endpoint in endpoints
        ],
    )


@router.post(
    "/type1",
    description=(DOCS_DIR / "type1.md").read_text(),
    summary="Type 1: Search Trials",
    response_model=schema.Type1Response,
)
async def search_trials(
    request: schema.Type1Request,
    metadata_reader: MdsReader = Depends(get_metadata_reader),
):
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
    description=(DOCS_DIR / "type2.md").read_text(),
    summary="Type 2: Get Trial Data",
    response_model=schema.Type2Response,
)
async def get_trial_details(
    request: schema.Type2Request,
    metadata_reader: MdsReader = Depends(get_metadata_reader),
):
    """
    Retrieve detailed data for a specific trial.

    - **request**: A JSON object containing the trial UUID and other optional parameters for data retrieval.
    """

    trial = metadata_reader.get_trial_by_uuid(
        request.trial_uuid, request.user_id, request.user_domain
    )
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
    description=(DOCS_DIR / "type3.md").read_text(),
    summary="Type 3: Search Trials and Get Data",
    response_model=schema.Type3Response,
)
async def search_trials_and_get_data(
    request: schema.Type3Request,
    metadata_reader: MdsReader = Depends(get_metadata_reader),
):
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
        data = _build_trial_payload(trials[0], request)  # type: ignore
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


@router.post(
    "/type4",
    description=(DOCS_DIR / "type4.md").read_text(),
    summary="Type 4: Download file/bytes data",
    # Type4Response subclasses FileResponse (a Starlette Response) and cannot
    # be a pydantic response_model; the file is returned directly.
    response_model=None,
)
async def get_file_value(
    request: schema.Type4Request,
    background_tasks: BackgroundTasks,
    metadata_reader: MdsReader = Depends(get_metadata_reader),
):
    """
    Download file/bytes data for a specific trial.

    - **request**: A JSON object containing the trial UUID and other optional parameters for data retrieval.

    Returns a single file for download, or a `.zip` archive if the trial
    contains multiple files. File metadata is returned in the
    `X-File-Metadata` response header (JSON encoded).
    """

    # Wait (within limits) until enough memory and CPU are available before
    # pulling potentially large file contents from the DWS servers.
    await _check_system_resources()

    trial = metadata_reader.get_trial_by_uuid(
        request.trial_uuid, request.user_id, request.user_domain
    )
    if not trial:
        raise HTTPException(status_code=404, detail="Trial not found")

    dataset = _build_trial_payload(trial, request, bytes=True)

    metadata = dataset["metadata"]
    data: dict[str, list[dict[Literal["timestamp", "value", "filename"], Any]]] = (
        dataset["data"]
    )

    # Collect all files; each DWS file datapoint yields one downloaded file.
    files: list[dict[str, Any]] = []
    for topic, topic_data in data.items():
        for entry in topic_data:
            filepath = entry["value"]
            if not filepath:
                continue
            files.append(
                {
                    "topic": topic,
                    "timestamp": entry["timestamp"],
                    "filename": entry.get("filename") or Path(filepath).name,
                    "filepath": filepath,
                }
            )

    if not files:
        raise HTTPException(status_code=404, detail="No files found for the request.")

    # Expose the file list (without local filepaths) in the response metadata.
    metadata["files"] = [
        {"topic": f["topic"], "timestamp": f["timestamp"], "filename": f["filename"]}
        for f in files
    ]

    # All files of one request share the same temporary directory created by
    # the DWS agent, so derive it from the filepaths. It is removed in a
    # background task once the response has been sent.
    temp_dir = Path(
        os.path.commonpath([str(Path(f["filepath"]).parent) for f in files])
    )

    if len(files) == 1:
        filepath_to_send = files[0]["filepath"]
        filename_to_send = files[0]["filename"]
    else:
        # Bundle all files in a flat zip archive. Name collisions between
        # same-named files are resolved by appending a per-file counter
        # before the extension.
        zip_name = f"{trial.get('trial_name') or request.trial_uuid}.zip"
        zip_path = temp_dir / zip_name
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            name_counts: dict[str, int] = {}
            for f in files:
                arcname = f["filename"]
                count = name_counts.get(arcname, 0)
                name_counts[arcname] = count + 1
                if count > 0:
                    stem = Path(arcname).stem
                    suffix = Path(arcname).suffix
                    arcname = f"{stem}_{count}{suffix}"
                zf.write(f["filepath"], arcname=arcname)

        filepath_to_send = str(zip_path)
        filename_to_send = zip_name

    background_tasks.add_task(shutil.rmtree, temp_dir, True)

    return schema.Type4Response(
        path=filepath_to_send, filename=filename_to_send, metadata=metadata
    )
