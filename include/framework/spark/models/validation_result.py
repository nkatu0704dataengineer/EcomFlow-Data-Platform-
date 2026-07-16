"""
EcomFlow Framework - Validation Result Model

Description:
    Data model representing the result of dataframe validation.
"""

from dataclasses import dataclass, field

@dataclass(slots=True)
class ValidationResult : 
    is_valid: bool
    row_count: int
    duplicate_count: int
    null_count: int
    
    schema_valid : bool
    datatype_valid : bool
    error_messages: list[str] = field(default_factory=list)
    null_details: dict[str, int] = field(default_factory=dict)