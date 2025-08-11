"""
Configuration Loading Utilities

Provides functions for loading and parsing YAML configurations from various sources
including file uploads and raw text input. Handles validation and error reporting.
"""

import yaml
from fastapi import UploadFile, HTTPException

async def load_config(file: UploadFile = None, text: str = None) -> dict:
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
        text = (await file.read()).decode()
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

def detect_protocol(config_dict: dict, adapter_map: dict) -> str:
    """
    Detect adapter type from configuration structure.
    
    Analyzes configuration keys to automatically identify which adapter
    should be used. Looks for known adapter keys in the configuration.
    
    Args:
        config_dict: Parsed configuration dictionary
        adapter_map: Map of protocol keys to adapter classes
        
    Returns:
        Detected protocol key/adapter type
        
    Raises:
        HTTPException: 400 if no matching adapter found
    """
    """
    Detect which protocol/adapter to use based on config keys.
    Simple and clean - just match config keys to available adapters.
    """
    if not config_dict:
        raise HTTPException(400, "Empty configuration provided")
    
    # Get the keys from the config (excluding common keys)
    excluded_keys = {'topic_family', 'mqtt'}
    config_keys = set(k for k in config_dict.keys() if k not in excluded_keys)
    
    # Get available adapter keys
    available_adapters = set(adapter_map.keys())
    
    # Find matching adapter key
    matches = config_keys & available_adapters
    
    if matches:
        # Found a match
        protocol = matches.pop()
        print(f"DEBUG: Detected protocol '{protocol}' from config")
        return protocol
    
    # No match found
    raise HTTPException(
        400, 
        f"No recognized adapter found in config. "
        f"Config must contain one of these keys: {list(available_adapters)}. "
        f"Found keys: {list(config_keys)}"
    )