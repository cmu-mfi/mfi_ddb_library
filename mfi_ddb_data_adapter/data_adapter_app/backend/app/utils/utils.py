"""
Configuration Loading Utilities

Provides functions for loading and parsing YAML configurations from various sources
including file uploads and raw text input. Handles validation and error reporting.
"""

import time  # <-- 1. Added time for latency tracking
from typing import Any, Dict, List, Optional, Tuple

import yaml
from fastapi import HTTPException, UploadFile

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
            # Note: Depending on your FastAPI setup, file.read() might be synchronous 
            # if using standard synchronous file wrappers, matching your original code.
            text = (file.read()).decode()
        except Exception as e:
            config_parse_errors.add(1, {"source": "file", "error_type": "file_read_error"})
            raise HTTPException(400, f"Failed to read uploaded file: {e}")

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
        raise HTTPException(400, f"YAML parse error: {e}")
    except Exception as e:
        config_parse_errors.add(1, {"source": source_type, "error_type": "unknown_error"})
        raise HTTPException(400, f"Error loading configuration: {e}")


def _decode_bytes(obj: Any) -> Any:
    """Recursively decode bytes to utf-8 in nested structures."""
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="ignore")
    if isinstance(obj, dict):
        return {k: _decode_bytes(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_decode_bytes(v) for v in obj]
    return obj