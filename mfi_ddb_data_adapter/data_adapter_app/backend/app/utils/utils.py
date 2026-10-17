"""
Configuration Loading Utilities

Provides functions for loading and parsing YAML configurations from various sources
including file uploads and raw text input. Handles validation and error reporting.
"""

import logging
import time  # <-- 1. Added time for latency tracking
from typing import Any, Dict, List, Optional, Tuple

import yaml
from fastapi import HTTPException, UploadFile
from pydantic import ValidationError

logger = logging.getLogger(__name__)

# ==========================================
# OPENTELEMETRY CUSTOM METRICS SETUP
# ==========================================
from opentelemetry.metrics import get_meter

meter = get_meter("mfi.data_adapter.app")

config_parse_duration = meter.create_histogram(
    "mfi_adapter_config_parse_duration_seconds",
    description="Time spent parsing YAML configuration files"
)

config_parse_errors = meter.create_counter(
    "mfi_adapter_config_parse_errors_total",
    description="Total configuration parse failures"
)
# ==========================================


def load_config(file: UploadFile = None, text: str = None) -> dict:
    """
    Load and parse YAML configuration from file upload or text input.
    """
    start_time = time.perf_counter()
    source_type = "file" if file else "text"
    
    if file:
        try:
            text = file.file.read().decode("utf-8")
        except UnicodeDecodeError:
            config_parse_errors.add(1, {"source": "file", "error_type": "file_read_error"})
            raise HTTPException(400, "Uploaded configuration must use UTF-8 encoding.")
        except Exception:
            config_parse_errors.add(1, {"source": "file", "error_type": "file_read_error"})
            logger.exception("Could not read uploaded configuration")
            raise HTTPException(500, "Could not read the uploaded configuration. Please retry.")

    if not text or not text.strip():
        config_parse_errors.add(1, {"source": source_type, "error_type": "empty_payload"})
        raise HTTPException(400, "No configuration provided.")

    try:
        config_dict = yaml.safe_load(text)
        if not isinstance(config_dict, dict):
            config_parse_errors.add(1, {"source": source_type, "error_type": "invalid_type"})
            raise HTTPException(400, "Configuration must be a YAML dictionary.")
            
        # Record successful parse performance
        duration = time.perf_counter() - start_time
        config_parse_duration.record(duration, {"source": source_type, "status": "SUCCESS"})
        return config_dict

    except yaml.YAMLError as e:
        config_parse_errors.add(1, {"source": source_type, "error_type": "yaml_syntax_error"})
        mark = getattr(e, "problem_mark", None)
        detail = {"message": getattr(e, "problem", None) or "Invalid YAML syntax.", "type": "yaml_syntax"}
        if mark is not None:
            detail.update(line=mark.line + 1, column=mark.column + 1)
        raise HTTPException(400, detail=detail)
    except HTTPException:
        raise
    except Exception:
        config_parse_errors.add(1, {"source": source_type, "error_type": "unknown_error"})
        logger.exception("Configuration parsing failed")
        raise HTTPException(500, "Configuration parsing is unavailable. Please retry.")


def validate_config(schema: Any, file: UploadFile = None, text: str = None) -> dict:
    """Return field errors without serializing rejected inputs or validator context."""
    try:
        config = load_config(file, text)
    except HTTPException as error:
        if error.status_code != 400:
            raise
        detail = error.detail if isinstance(error.detail, dict) else {"message": str(error.detail)}
        issue = {"loc": [], "type": "configuration", **detail}
        raise HTTPException(400, detail={"is_valid": False, "errors": [issue]}) from error

    try:
        schema.model_validate(config)
    except ValidationError as error:
        errors = [
            {"loc": list(issue["loc"]), "message": issue["msg"], "type": issue["type"]}
            for issue in error.errors(include_url=False, include_context=False, include_input=False)
        ]
        return {"is_valid": False, "errors": errors}
    return {"is_valid": True, "errors": []}


def _decode_bytes(obj: Any) -> Any:
    """Recursively decode bytes to utf-8 in nested structures."""
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="ignore")
    if isinstance(obj, dict):
        return {k: _decode_bytes(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_decode_bytes(v) for v in obj]
    return obj
