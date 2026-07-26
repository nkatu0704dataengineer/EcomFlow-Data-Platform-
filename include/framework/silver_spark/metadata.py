"""
Metadata Generator Module for Silver Layer Processing.

This module generates metadata by aggregating information from
the validation process and DataFrame structure.

Does not perform validation or recalculation.

Author:
    EcomFlow Data Platform Team

Layer:
    Silver Framework
"""

from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), '../../..')))

from datetime import datetime, timezone
from pyspark.sql import DataFrame
import logging

from include.framework.silver_spark.models.validation_result import ValidationResult
from include.framework.silver_spark.models.metadata_result import MetadataResult

logger = logging.getLogger(__name__)


def generate_metadata(
    df: DataFrame,
    validation_result: ValidationResult,
    dataset: str,
    layer: str,
    object_path: str,
    format: str = "delta",
    processing_duration: float | None = None,
) -> MetadataResult:
    """
    Generate metadata for a processed Silver dataset.
    
    Aggregates information from DataFrame structure and ValidationResult
    without performing any recalculation or validation.
    
    Args:
        df: Processed Spark DataFrame.
        validation_result: Validation result from structural validation.
        dataset: Dataset name.
        layer: Processing layer (e.g., 'silver').
        object_path: Storage path for the dataset.
        format: Storage format. Defaults to 'delta'.
        processing_duration: Processing time in seconds. Defaults to None.
        
    Returns:
        MetadataResult containing aggregated metadata.
    
    Example:
        >>> metadata = generate_metadata(
        ...     df=processed_df,
        ...     validation_result=validation_result,
        ...     dataset="customers",
        ...     layer="silver",
        ...     object_path="s3://bucket/silver/customers",
        ...     processing_duration=12.5
        ... )
    """
    logger.info(f"Generating metadata for dataset: {dataset}")
    
    column_count = len(df.columns)
    columns = df.columns
    
    generated_at = datetime.now(timezone.utc).isoformat()
    
    metadata = MetadataResult(
        dataset=dataset,
        layer=layer,
        format=format,
        object_path=object_path,
        column_count=column_count,
        columns=columns,
        is_valid=validation_result.is_valid,
        row_count=validation_result.row_count,
        duplicate_count=validation_result.duplicate_count,
        null_details=validation_result.null_details,
        schema_valid=validation_result.schema_valid,
        datatype_valid=validation_result.datatype_valid,
        generated_at=generated_at,
        processing_duration=processing_duration,
        warning_messages=validation_result.warning_messages,
        error_messages=validation_result.error_messages,
    )
    
    logger.info(f"Metadata generated for dataset: {dataset}")
    
    return metadata
