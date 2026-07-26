"""
Metadata Result Model.

Represents metadata generated after Silver dataset processing.

This model aggregates information from the framework components
without performing calculations or validations.

Author:
    EcomFlow Data Platform

Layer:
    Silver Framework
"""

from __future__ import annotations

from dataclasses import dataclass, field

from include.config.framework import FRAMEWORK_VERSION


@dataclass(slots=True)
class MetadataResult:
    """
    Result of Silver dataset metadata generation.

    Attributes:
        dataset:
            Dataset name.

        layer:
            Processing layer (e.g., 'silver').

        format:
            Storage format (e.g., 'delta').

        object_path:
            Storage path for the dataset.

        column_count:
            Total number of columns.

        columns:
            List of column names.

        is_valid:
            Overall validation status.

        row_count:
            Total number of rows.

        duplicate_count:
            Number of duplicate records.

        null_details:
            Null counts per column.

        schema_valid:
            Schema validity status.

        datatype_valid:
            Data type validity status.

        generated_at:
            ISO-8601 UTC timestamp when metadata was generated.

        processing_duration:
            Processing time in seconds.

        warning_messages:
            Non-critical warnings.

        error_messages:
            Critical error messages.

        framework:
            Framework name.

        framework_version:
            Framework version.
    """

    dataset: str

    layer: str

    format: str

    object_path: str

    column_count: int

    columns: list[str]

    is_valid: bool

    row_count: int

    duplicate_count: int

    null_details: dict[str, int]

    schema_valid: bool

    datatype_valid: bool

    generated_at: str

    processing_duration: float | None = None

    warning_messages: list[str] = field(default_factory=list)

    error_messages: list[str] = field(default_factory=list)

    framework: str = "EcomFlow"

    framework_version: str = field(default_factory=lambda: FRAMEWORK_VERSION)

    def to_dict(self) -> dict:
        """
        Convert MetadataResult into a JSON-serializable dictionary.
        """
        return {
            "dataset": self.dataset,
            "layer": self.layer,
            "format": self.format,
            "object_path": self.object_path,
            "column_count": self.column_count,
            "columns": self.columns,
            "is_valid": self.is_valid,
            "row_count": self.row_count,
            "duplicate_count": self.duplicate_count,
            "null_details": self.null_details,
            "schema_valid": self.schema_valid,
            "datatype_valid": self.datatype_valid,
            "generated_at": self.generated_at,
            "processing_duration": self.processing_duration,
            "warning_messages": self.warning_messages,
            "error_messages": self.error_messages,
            "framework": self.framework,
            "framework_version": self.framework_version,
        }
