"""
Metadata Result Model.

Represents metadata generated after Gold dataset processing.

This model aggregates information from analytical query execution
without performing validations or data quality checks.

Author:
    EcomFlow Data Platform

Layer:
    Gold Framework
"""

from __future__ import annotations

from dataclasses import dataclass, field

from include.config.framework import FRAMEWORK_VERSION


@dataclass(slots=True)
class MetadataResult:
    """
    Result of Gold dataset metadata generation.

    Attributes:
        query_name:
            Name of the analytical query executed.

        target_table:
            Target Gold table name (fully qualified).

        layer:
            Processing layer (e.g., 'gold').

        format:
            Storage format (e.g., 'delta').

        object_path:
            Storage path for the dataset.

        row_count:
            Total number of rows in result.

        column_count:
            Total number of columns.

        columns:
            List of column names.

        status:
            Execution status (e.g., 'SUCCESS', 'FAILED').

        generated_at:
            ISO-8601 UTC timestamp when metadata was generated.

        processing_duration:
            Processing time in seconds.

        framework:
            Framework name.

        framework_version:
            Framework version.
    """

    query_name: str

    target_table: str

    layer: str

    format: str

    object_path: str

    row_count: int

    column_count: int

    columns: list[str]

    status: str

    generated_at: str

    processing_duration: float | None = None

    framework: str = "EcomFlow"

    framework_version: str = field(default_factory=lambda: FRAMEWORK_VERSION)

    def to_dict(self) -> dict:
        """
        Convert MetadataResult into a JSON-serializable dictionary.
        """
        return {
            "query_name": self.query_name,
            "target_table": self.target_table,
            "layer": self.layer,
            "format": self.format,
            "object_path": self.object_path,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "columns": self.columns,
            "status": self.status,
            "generated_at": self.generated_at,
            "processing_duration": self.processing_duration,
            "framework": self.framework,
            "framework_version": self.framework_version,
        }
