"""
Pipeline Result Model.

Represents the output of the Silver Framework pipeline execution.

Author:
    EcomFlow Data Platform

Layer:
    Silver Framework
"""

from __future__ import annotations

from dataclasses import dataclass

from .validation_result import ValidationResult
from .metadata_result import MetadataResult


@dataclass(slots=True)
class PipelineResult:
    """
    Result of Silver Framework pipeline execution.

    Attributes:
        validation_result:
            Structural validation results.

        metadata_result:
            Generated metadata.

        processing_duration:
            Total processing time in seconds.

        success:
            Pipeline execution status.
    """

    validation_result: ValidationResult

    metadata_result: MetadataResult

    processing_duration: float

    success: bool

    def to_dict(self) -> dict:
        """
        Convert PipelineResult into a JSON-serializable dictionary.
        """
        return {
            "validation_result": self.validation_result.to_dict(),
            "metadata_result": self.metadata_result.to_dict(),
            "processing_duration": self.processing_duration,
            "success": self.success,
        }
