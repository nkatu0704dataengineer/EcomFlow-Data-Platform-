"""
EcomFlow Framework - Validator Module

Description:
    Validate Spark DataFrame quality.

Responsibilities:
    - Validate empty dataframe
    - Validate duplicate rows
    - Validate null values
    - Collect validation summary

This module DOES NOT:
    - Read data
    - Write data
    - Upload data
    - Generate metadata
"""

import sys
import os

# Add project root to Python path
project_root = "/Workspace/Users/tumaxpro99@gmail.com/EcomFlow"
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, sum as spark_sum

from include.framework.spark.models.validation_result import ValidationResult

def collect_dataframe_metrics(df: DataFrame) -> tuple[int, int, dict[str, int]]:
    row_count = df.count()
    duplicate_count = row_count - df.dropDuplicates().count()
    null_details = df.select([spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]).collect()[0].asDict()

    return row_count, duplicate_count, null_details

def validate_dataframe(df: DataFrame) -> ValidationResult:
    """Validate DataFrame and report data quality issues.
    
    This function is a diagnostic tool that reports data quality metrics.
    It only blocks pipeline execution if the DataFrame is completely empty.
    Other issues (nulls, duplicates) are reported but do not block processing.
    
    Args:
        df: Spark DataFrame to validate
        
    Returns:
        ValidationResult with is_valid=True unless DataFrame is empty
    """
    if not isinstance(df, DataFrame):
        raise TypeError("df must be a Spark DataFrame instance")
    
    row_count, duplicate_count, null_details = collect_dataframe_metrics(df)
    
    # Only fail validation if DataFrame is completely empty (critical condition)
    # Nulls and duplicates are reported but don't block the pipeline
    is_valid = row_count > 0
    
    error_messages = []
    if row_count == 0:
        error_messages.append("DataFrame is empty.")
    
    # Report duplicates and nulls as warnings, not errors
    if duplicate_count > 0:
        error_messages.append(f"⚠️  Warning: DataFrame contains {duplicate_count} duplicate rows.")
    if any(value > 0 for value in null_details.values()):
        null_columns = [col for col, count in null_details.items() if count > 0]
        error_messages.append(f"⚠️  Warning: DataFrame contains null values in columns: {', '.join(null_columns)}.")

    return ValidationResult(
        is_valid=is_valid,
        row_count=row_count,
        duplicate_count=duplicate_count,
        null_count=sum(null_details.values()),
        null_details={
           column: count
           for column, count in null_details.items()
           if count > 0
       },
        schema_valid=True,  # Placeholder for schema validation logic
        datatype_valid=True,  # Placeholder for datatype validation logic
        error_messages=error_messages
    )