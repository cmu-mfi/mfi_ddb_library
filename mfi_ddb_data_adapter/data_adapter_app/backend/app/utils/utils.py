"""
Configuration Loading Utilities

Provides functions for loading and parsing YAML configurations from various sources
including file uploads and raw text input. Handles validation and error reporting.
"""

from typing import Any, Dict, List, Optional, Tuple

import yaml
from fastapi import HTTPException, UploadFile


def load_config(file: UploadFile = None, text: str = None) -> dict:
    """
    Load and parse YAML configuration from file upload or text input.
    
    Accepts configuration from either a file upload or raw text string,
    parses it as YAML, and validates the structure. Used by validation
    and connection endpoints.
    
    Args:
        file: Optional uploaded YAML file
        text: Optional raw YAML text string
        
    Returns:
        Parsed configuration as dictionary
        
    Raises:
        HTTPException: 400 if no config provided, invalid YAML, or parsing fails
    """
    if file:
        text = (file.read()).decode()
    if not text or not text.strip():
        raise HTTPException(400, "No configuration provided.")
    try:
        config_dict = yaml.safe_load(text)
        if not isinstance(config_dict, dict):
            raise HTTPException(400, "Configuration must be a YAML dictionary.")
        return config_dict
    except yaml.YAMLError as e:
        raise HTTPException(400, f"YAML parse error: {e}")
    except Exception as e:
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