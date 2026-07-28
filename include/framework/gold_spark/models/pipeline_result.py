"""
Pipeline Result Model.

Represents the output of the Gold Framework pipeline execution.

Author:
    EcomFlow Data Platform

Layer:
    Gold Framework
"""

from __future__ import annotations

from dataclasses import dataclass

from .metadata_result import MetadataResult


@dataclass(slots=True)
class PipelineResult:
    """
    Result of Gold Framework pipeline execution.

    Attributes:
        metadata_result:
            Generated execution metadata.

        processing_duration:
            Total processing time in seconds.

        success:
            Pipeline execution status.
    """

    metadata_result: MetadataResult

    processing_duration: float

    success: bool

    def to_dict(self) -> dict:
        """
        Convert PipelineResult into a JSON-serializable dictionary.
        """
        return {
            "metadata_result": self.metadata_result.to_dict(),
            "processing_duration": self.processing_duration,
            "success": self.success,
        }
