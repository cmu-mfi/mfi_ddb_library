"""
Configuration Validation Utilities

Provides comprehensive validation functionality for adapter configurations using Pydantic schemas.
Supports recursive validation, multiple validation approaches, and detailed error reporting.
"""

from pydantic import ValidationError, Field
from fastapi import HTTPException
from typing import Dict, List, Any, Optional, Union

class ValidationResult:
    """
    Container for validation results including errors and warnings.
    
    Accumulates validation errors and warnings during schema validation,
    providing a structured way to report validation outcomes to API clients.
    """
    def __init__(self):
        self.is_valid = True
        self.errors = []
        self.warnings = []
    
    def add_error(self, message: str):
        self.errors.append(message)
        self.is_valid = False
    
    def add_warning(self, message: str):
        self.warnings.append(message)
    
    def to_dict(self):
        return {
            "valid": self.is_valid,
            "errors": self.errors,
            "warnings": self.warnings
        }

def recursive_validate_schema(user_config: Dict[str, Any], schema_fields: Dict[str, Any], field_path: str = "") -> ValidationResult:
    validation_result = ValidationResult()
    
    # Check required fields
    for field_name, field_definition in schema_fields.items():
        current_field_path = f"{field_path}.{field_name}" if field_path else field_name
        
        field_is_required = field_definition.is_required() if hasattr(field_definition, 'is_required') else (
            field_definition.default is ... or
            (hasattr(field_definition, 'default') and field_definition.default is None and not field_definition.allow_none)
        )
        
        if field_name not in user_config:
            if field_is_required:
                validation_result.add_error(f"Required field '{current_field_path}' is missing")
            continue
        
        field_value = user_config[field_name]
        field_type = field_definition.annotation if hasattr(field_definition, 'annotation') else field_definition.type_
        
        # Handle nested models
        if hasattr(field_type, '__fields__'):
            if isinstance(field_value, dict):
                nested_validation = recursive_validate_schema(field_value, field_type.__fields__, current_field_path)
                validation_result.errors.extend(nested_validation.errors)
                validation_result.warnings.extend(nested_validation.warnings)
                if nested_validation.errors:
                    validation_result.is_valid = False
            else:
                validation_result.add_error(f"Field '{current_field_path}' should be an object, got {type(field_value).__name__}")
            continue
        
        validate_field_type(field_value, field_type, current_field_path, validation_result, field_is_required)
    
    # Check for unexpected fields
    for field_name in user_config:
        if field_name not in schema_fields:
            unexpected_field_path = f"{field_path}.{field_name}" if field_path else field_name
            validation_result.add_warning(f"Unexpected field '{unexpected_field_path}' found (will be ignored)")
    
    return validation_result

def validate_field_type(field_value: Any, expected_type: type, field_path: str, validation_result: ValidationResult, field_is_required: bool) -> bool:
    # Handle Union types (Optional fields)
    if hasattr(expected_type, '__origin__') and expected_type.__origin__ is Union:
        union_types = expected_type.__args__
        for union_type in union_types:
            if union_type is type(None) and field_value is None:
                return True
            if validate_simple_type(field_value, union_type):
                return True
        
        type_names = [getattr(t, '__name__', str(t)) for t in union_types if t is not type(None)]
        if field_is_required:
            validation_result.add_error(f"Field '{field_path}' must be one of types: {', '.join(type_names)}, got {type(field_value).__name__}")
        else:
            validation_result.add_warning(f"Field '{field_path}' should be one of types: {', '.join(type_names)}, got {type(field_value).__name__}")
        return False
    
    if not validate_simple_type(field_value, expected_type):
        expected_type_name = getattr(expected_type, '__name__', str(expected_type))
        if field_is_required:
            validation_result.add_error(f"Field '{field_path}' must be of type {expected_type_name}, got {type(field_value).__name__}")
        else:
            validation_result.add_warning(f"Field '{field_path}' should be of type {expected_type_name}, got {type(field_value).__name__}")
        return False
    
    return True

def validate_simple_type(field_value: Any, expected_type: type) -> bool:
    if expected_type is str:
        return isinstance(field_value, str)
    elif expected_type is int:
        return isinstance(field_value, int) and not isinstance(field_value, bool)
    elif expected_type is float:
        return isinstance(field_value, (int, float)) and not isinstance(field_value, bool)
    elif expected_type is bool:
        return isinstance(field_value, bool)
    elif expected_type is list:
        return isinstance(field_value, list)
    elif expected_type is dict:
        return isinstance(field_value, dict)
    else:
        # Handle generic types (List[str], Dict[str, Any], etc.)
        try:
            # Try to get the origin type (List[str] -> list)
            origin = getattr(expected_type, '__origin__', None)
            if origin is not None:
                return isinstance(field_value, origin)
            
            # For regular types, try normal isinstance
            return isinstance(field_value, expected_type)
        except TypeError:
            # If isinstance fails, assume it's valid (skip type checking)
            return True

def extract_schema_fields(schema_class):
    """Extract fields from Pydantic model (v1 or v2 compatible)"""
    if hasattr(schema_class, '__fields__'):
        return schema_class.__fields__
    elif hasattr(schema_class, 'model_fields'):
        return schema_class.model_fields
    return None

def find_best_validation_approach(user_config: Dict[str, Any], schema_fields: Dict[str, Any]) -> ValidationResult:
    """Try different config structures and return the one with fewest errors"""
    common_fields = {'topic_family', 'mqtt'}
    
    # Approach 1: Validate config directly (for direct schemas like ROS, LocalFiles)
    adapter_only_config = {k: v for k, v in user_config.items() if k not in common_fields}
    direct_validation = recursive_validate_schema(adapter_only_config, schema_fields)
    
    # If perfect match, return immediately
    if len(direct_validation.errors) == 0:
        return direct_validation
    
    best_validation = direct_validation
    min_error_count = len(direct_validation.errors)
    
    # Approach 2: Look for nested structures (for keyed schemas like MTConnect)
    for key, value in user_config.items():
        if key in common_fields or not isinstance(value, dict):
            continue
            
        # Check if this key exists in schema as nested model
        if key in schema_fields:
            nested_field_def = schema_fields[key]
            nested_type = nested_field_def.annotation if hasattr(nested_field_def, 'annotation') else nested_field_def.type_
            
            nested_schema_fields = extract_schema_fields(nested_type)
            if nested_schema_fields:
                nested_validation = recursive_validate_schema(value, nested_schema_fields, key)
                if len(nested_validation.errors) < min_error_count:
                    best_validation = nested_validation
                    min_error_count = len(nested_validation.errors)
                    
                    # Perfect nested match found
                    if min_error_count == 0:
                        return best_validation
    
    # Approach 3: Validate full config against schema (fallback)
    if min_error_count > 0:
        full_validation = recursive_validate_schema(user_config, schema_fields)
        if len(full_validation.errors) < min_error_count:
            best_validation = full_validation
    
    return best_validation

def validate_config_for_adapter(user_config: Dict[str, Any], adapter_class) -> ValidationResult:
    """
    Validate user configuration against adapter's Pydantic schema.
    
    Main validation function that extracts the schema from an adapter class
    and validates user configuration against it. Uses multiple approaches
    to handle different configuration structures.
    
    Args:
        user_config: User-provided configuration dictionary
        adapter_class: Adapter class with SCHEMA attribute
        
    Returns:
        ValidationResult with errors, warnings, and validity status
    """
    validation_result = ValidationResult()
    
    if not adapter_class:
        validation_result.add_error("Adapter class not found")
        return validation_result
    
    schema_class = getattr(adapter_class, 'SCHEMA', None)
    if not schema_class:
        validation_result.add_warning("No schema defined for this adapter - skipping validation")
        return validation_result
    
    schema_fields = extract_schema_fields(schema_class)
    if not schema_fields:
        validation_result.add_warning("Could not extract schema fields - skipping detailed validation")
        return validation_result
    
    return find_best_validation_approach(user_config, schema_fields)