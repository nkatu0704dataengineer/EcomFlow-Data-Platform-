"""
Validation Result Model.

Represents the output of the Silver structural validation process.

This model contains only framework-level validation results.
Business-specific validation belongs to individual dataset notebooks.

Author:
    EcomFlow Data Platform

Layer:
    Silver Framework
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass(slots=True)
class ValidationResult:
    """
    Result of structural data validation.

    Attributes:
        is_valid:
            Overall validation status.

        row_count:
            Total number of rows.

        column_count:
            Total number of columns.

        duplicate_count:
            Number of duplicated records.

        null_details:
            Number of null values for each column.

        schema_valid:
            Whether the DataFrame schema matches the expected structure.

        datatype_valid:
            Whether all column data types are valid.

        error_messages:
            Critical validation errors.

        warning_messages:
            Non-critical validation warnings.
    """

    is_valid: bool

    row_count: int

    column_count: int

    duplicate_count: int

    null_details: Dict[str, int]

    schema_valid: bool

    datatype_valid: bool

    error_messages: List[str] = field(default_factory=list)

    warning_messages: List[str] = field(default_factory=list)

    def has_errors(self) -> bool:
        """
        Returns True if critical validation errors exist.
        """
        return len(self.error_messages) > 0

    def has_warnings(self) -> bool:
        """
        Returns True if validation warnings exist.
        """
        return len(self.warning_messages) > 0

    def to_dict(self) -> dict:
        """
        Convert ValidationResult into a JSON-serializable dictionary.
        """

        return {
            "is_valid": self.is_valid,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "duplicate_count": self.duplicate_count,
            "null_details": self.null_details,
            "schema_valid": self.schema_valid,
            "datatype_valid": self.datatype_valid,
            "error_messages": self.error_messages,
            "warning_messages": self.warning_messages,
        }
