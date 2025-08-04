from app.utils.loader import FullConfig

"""
Configuration validation utility for data adapter protocols.

This module provides schema validation for configuration objects by attempting
to instantiate a FullConfig object and verifying that the specified protocol
block exists within the configuration.

Used to validate configuration files before processing data adapter operations.

Functions:
   run_validation: Validates config schema and ensures required protocol block exists
"""


def run_validation(config: dict, protocol: str) -> bool:
    try:
        full_cfg = FullConfig(**config)
    except Exception as e:
        raise ValueError(f"Configuration schema error: {e}")

    if getattr(full_cfg, protocol) is None:
        raise ValueError(f"Missing required '{protocol}' block")

    return True
