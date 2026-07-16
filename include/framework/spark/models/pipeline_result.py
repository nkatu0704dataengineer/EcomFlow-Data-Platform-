from dataclasses import dataclass

from include.framework.spark.models.metadata_result import Metadata
from include.framework.spark.models.validation_result import ValidationResult


@dataclass(slots=True)
class PipelineResult:
    validation_result: ValidationResult
    metadata: Metadata
    csv_path: str
    object_path: str
