"""
EcomFlow Framework - Metadata Module

Description:
    Generate metadata for a processed dataset.

Responsibilities:
    - Generate dataset metadata.
    - Combine DataFrame information.
    - Combine validation result.
    - Return Metadata object.

This module DOES NOT:
    - Read data
    - Validate data
    - Write data
    - Upload data
"""

import sys
from pathlib import Path

# Add package root to path for direct execution
try:
    # Standard Python: use __file__
    package_root = Path(__file__).resolve().parent.parent.parent.parent
except NameError:
    # Databricks: __file__ not defined, use workspace path
    package_root = Path("/Workspace/Users/tumaxpro99@gmail.com/EcomFlow")

if str(package_root) not in sys.path:
    sys.path.insert(0, str(package_root))

from datetime import datetime

from pyspark.sql import DataFrame

from include.framework.spark.models.metadata_result import Metadata
from include.framework.spark.models.validation_result import ValidationResult

def generate_metadata(
        df: DataFrame,
        validation_result: ValidationResult,
        layer: str,
        dataset: str,
        object_path: str,
        file_format: str = "csv",
)->Metadata :
    if not isinstance(df, DataFrame):
        raise TypeError("df must be a Spark DataFrame instance")
    if not isinstance(validation_result, ValidationResult):
        raise TypeError("validation_result must be a ValidationResult instance")
    if not isinstance(layer, str):
        raise TypeError("layer must be a string")
    if not isinstance(dataset, str):
        raise TypeError("dataset must be a string")
    if not isinstance(object_path, str):
        raise TypeError("object_path must be a string")
    if not isinstance(file_format, str):
        raise TypeError("file_format must be a string")

    layer = layer.strip()
    dataset = dataset.strip()
    object_path = object_path.strip()
    file_format = file_format.strip().lower()

    if not layer:
        raise ValueError("layer cannot be empty")   
    if not dataset:
        raise ValueError("dataset cannot be empty")
    if not object_path:
        raise ValueError("object_path cannot be empty")
    if file_format not in ["csv", "parquet", "json"]:
        raise ValueError("file_format must be one of 'csv', 'parquet', or 'json'")

    metadata = Metadata(
        layer=layer,
        dataset=dataset,
        format=file_format,
        object_path=object_path,
        column_count=len(df.columns),
        columns=df.columns,
        is_valid=validation_result.is_valid,
        row_count=validation_result.row_count,
        duplicate_count=validation_result.duplicate_count,
        null_count=validation_result.null_count,
        null_details=validation_result.null_details,
        schema_valid=validation_result.schema_valid,
        datatype_valid=validation_result.datatype_valid,
        error_messages=validation_result.error_messages,
        generated_at=datetime.now().isoformat(),
    )

    return metadata