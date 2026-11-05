"""
MFI DDB Metadata Form - backend API.

Serves a configurable set of metadata forms and publishes each submission
through the mfi_ddb KeyValue (KV) data adapter.

Endpoints (nginx exposes them under /api):
  GET  /health                    service + configuration status
  GET  /config                    forms and target info for the UI (no secrets)
  POST /forms/{form_id}/preview   build + validate the payload, do not publish
  POST /forms/{form_id}/submit    build + validate + publish through the KV adapter
  GET  /submissions               recent submissions (if history is enabled)
  GET/POST/PUT/DELETE /builder/forms[/{id}]   forms created in the browser
"""

import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

import asyncio  # noqa: E402
from contextlib import asynccontextmanager  # noqa: E402

from fastapi import FastAPI, Query  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from fastapi.responses import JSONResponse  # noqa: E402
from pydantic import BaseModel  # noqa: E402
from starlette.concurrency import run_in_threadpool  # noqa: E402

import secrets  # noqa: E402

from fastapi import Body, Header  # noqa: E402
from pydantic import ValidationError  # noqa: E402

from app.config import ConfigManager, FormConfig, public_view  # noqa: E402
from app.custom_forms import BuilderForm, CustomFormStore  # noqa: E402
from app.history import History  # noqa: E402
from app.payload import SubmissionError, build_payload, lint_config  # noqa: E402
from app.publisher import PublishError, publish, topic_for  # noqa: E402

logger = logging.getLogger("metadata_form")

config_manager = ConfigManager()
history = History(Path(os.environ.get("FORM_HISTORY_DB", "data/submissions.db")))
custom_store = CustomFormStore()


def current_config() -> Optional[FormConfig]:
    """YAML config with the browser-built forms appended (YAML ids win on clashes)."""
    cfg = config_manager.get()
    if cfg is None or not cfg.builder.enabled:
        return cfg
    yaml_ids = {f.id for f in cfg.forms}
    custom = []
    for builder_form in custom_store.list():
        if builder_form.id in yaml_ids:
            continue
        try:
            custom.append(builder_form.to_form())
        except Exception:
            logger.warning("Skipping invalid builder form %s", builder_form.id)
    return cfg.model_copy(update={"forms": [*cfg.forms, *custom]})


@asynccontextmanager
async def lifespan(_app):
    cfg = config_manager.get()
    if cfg is None:
        logger.error("No valid form configuration: %s", config_manager.error)
    else:
        for warning in lint_config(cfg):
            logger.warning("config: %s", warning)
        logger.info(
            "Serving %d form(s); publishing to %s (dry_run=%s)",
            sum(f.enabled for f in cfg.forms),
            cfg.streamer["mqtt"]["broker_address"],
            cfg.publish.dry_run,
        )
    yield


app = FastAPI(
    title="MFI DDB - Metadata Form API",
    version="0.1.0",
    description="Configurable metadata forms published through the mfi_ddb KV data adapter.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("CORS_ALLOW_ORIGINS", "*").split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)


class SubmitBody(BaseModel):
    values: Dict[str, Any] = {}


def _error(status: int, message: str, errors: Optional[list] = None, **extra) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={
            "ok": False,
            "message": message,
            "errors": errors or [{"field": None, "message": message}],
            **extra,
        },
    )


def _require_config() -> FormConfig:
    cfg = current_config()
    if cfg is None:
        raise RuntimeError(config_manager.error or "configuration not loaded")
    return cfg


@app.get("/")
async def root() -> dict:
    return {"status": "healthy", "service": "MFI DDB: METADATA FORM"}


@app.get("/health")
async def health() -> dict:
    cfg = current_config()
    return {
        "status": "healthy" if cfg is not None else "misconfigured",
        "config_path": str(config_manager.path),
        "config_error": config_manager.error,
        "config_warnings": lint_config(cfg) if cfg else [],
        "dry_run": cfg.publish.dry_run if cfg else None,
    }


@app.get("/config")
async def get_config():
    try:
        cfg = _require_config()
    except RuntimeError as err:
        return _error(503, f"Form configuration is not available: {err}")
    view = public_view(cfg)
    view["config_error"] = config_manager.error
    view["config_warnings"] = lint_config(cfg)
    return view


def _prepare(form_id: str, body: SubmitBody):
    cfg = _require_config()
    form = cfg.get_form(form_id)
    if form is None:
        return cfg, None, None, _error(404, f"Unknown form '{form_id}'")
    try:
        payload = build_payload(form, body.values, cfg)
    except SubmissionError as err:
        return cfg, form, None, _error(422, "Please fix the highlighted fields.", err.errors)
    return cfg, form, payload, None


@app.post("/forms/{form_id}/preview")
async def preview(form_id: str, body: SubmitBody):
    try:
        cfg, form, payload, error = _prepare(form_id, body)
    except RuntimeError as err:
        return _error(503, f"Form configuration is not available: {err}")
    if error:
        return error
    return {"ok": True, "payload": payload, "topic": topic_for(cfg, form.msg_type)}


@app.post("/forms/{form_id}/submit")
async def submit(form_id: str, body: SubmitBody):
    try:
        cfg, form, payload, error = _prepare(form_id, body)
    except RuntimeError as err:
        return _error(503, f"Form configuration is not available: {err}")
    if error:
        return error

    def record(status: str, topic: Optional[str], err: Optional[str] = None) -> Optional[int]:
        if not cfg.history.enabled:
            return None
        try:
            return history.add(
                form.id, form.msg_type, status, payload, topic, err, cfg.history.max_entries
            )
        except Exception:  # history must never break a submission
            logger.exception("Failed to record submission history")
            return None

    try:
        result = await asyncio.wait_for(
            run_in_threadpool(publish, payload, cfg), timeout=cfg.publish.publish_timeout_seconds
        )
    except asyncio.TimeoutError:
        message = f"Publishing timed out after {cfg.publish.publish_timeout_seconds:g}s."
        logger.error("%s form=%s", message, form.id)
        record("failed", topic_for(cfg, form.msg_type), message)
        return _error(504, message, payload=payload)
    except PublishError as err:
        logger.error("Publish failed for form=%s: %s", form.id, err)
        record("failed", topic_for(cfg, form.msg_type), str(err))
        return _error(502, str(err), payload=payload)
    except Exception as err:
        logger.exception("Unexpected publish failure for form=%s", form.id)
        record("failed", topic_for(cfg, form.msg_type), str(err))
        return _error(500, f"Unexpected error while publishing: {err}", payload=payload)

    submission_id = record(result["status"], result["topic"])
    return {"ok": True, **result, "payload": payload, "submission_id": submission_id}


@app.get("/submissions")
async def submissions(limit: int = Query(25, ge=1, le=200)):
    cfg = config_manager.get()
    if cfg is not None and not cfg.history.enabled:
        return []
    return history.recent(limit)


# ---------------------------------------------------------------------------
# Form builder: forms created in the browser (always msg_type "data")
# ---------------------------------------------------------------------------


def _builder_guard(token: Optional[str]) -> Optional[JSONResponse]:
    cfg = config_manager.get()
    if cfg is None:
        return _error(503, "Form configuration is not available.")
    if not cfg.builder.enabled:
        return _error(403, "The form builder is disabled in the configuration.")
    expected = cfg.builder.admin_token
    if expected and not secrets.compare_digest(token or "", expected):
        return _error(401, "Admin token required to change forms.")
    return None


def _validation_errors(err: ValidationError) -> list:
    errors = []
    for e in err.errors():
        loc = [str(p) for p in e.get("loc", ())]
        message = e.get("msg", "invalid value").removeprefix("Value error, ")
        errors.append({"field": ".".join(loc) or None, "message": message})
    return errors


def _parse_builder_form(data: dict):
    try:
        return BuilderForm(**data), None
    except ValidationError as err:
        return None, _error(422, "Please fix the form definition.", _validation_errors(err))


@app.get("/builder/forms")
async def builder_list():
    return [f.model_dump() for f in custom_store.list()]


@app.post("/builder/forms")
async def builder_create(
    data: Dict[str, Any] = Body(...), x_admin_token: Optional[str] = Header(None)
):
    if (denied := _builder_guard(x_admin_token)) is not None:
        return denied
    form, error = _parse_builder_form(data)
    if error:
        return error
    taken = {f.id for f in config_manager.get().forms}
    try:
        saved = custom_store.save(form, taken_ids=taken)
    except (ValidationError, ValueError) as err:
        return _error(422, f"Invalid form: {err}")
    logger.info("Builder: created form %s", saved.id)
    return {"ok": True, "form": saved.model_dump()}


@app.put("/builder/forms/{form_id}")
async def builder_update(
    form_id: str, data: Dict[str, Any] = Body(...), x_admin_token: Optional[str] = Header(None)
):
    if (denied := _builder_guard(x_admin_token)) is not None:
        return denied
    form, error = _parse_builder_form(data)
    if error:
        return error
    try:
        saved = custom_store.save(form, taken_ids=set(), form_id=form_id)
    except KeyError:
        return _error(404, f"Form '{form_id}' was not created in the builder.")
    except (ValidationError, ValueError) as err:
        return _error(422, f"Invalid form: {err}")
    logger.info("Builder: updated form %s", form_id)
    return {"ok": True, "form": saved.model_dump()}


@app.delete("/builder/forms/{form_id}")
async def builder_delete(form_id: str, x_admin_token: Optional[str] = Header(None)):
    if (denied := _builder_guard(x_admin_token)) is not None:
        return denied
    if not custom_store.delete(form_id):
        return _error(404, f"Form '{form_id}' was not created in the builder.")
    logger.info("Builder: deleted form %s", form_id)
    return {"ok": True}
