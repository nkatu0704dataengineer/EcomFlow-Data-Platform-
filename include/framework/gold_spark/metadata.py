"""
Metadata Generator Module for Gold Layer Processing.

This module generates metadata by aggregating information from
analytical query execution and DataFrame structure.

Does not perform validation or data quality checks.

Author:
    EcomFlow Data Platform Team

Layer:
    Gold Framework
"""

from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), '../../..')))

from datetime import datetime, timezone
from pyspark.sql import DataFrame
import logging

from include.framework.gold_spark.models.metadata_result import MetadataResult

logger = logging.getLogger(__name__)


def generate_metadata(
    df: DataFrame,
    query_name: str,
    target_table: str,
    layer: str,
    object_path: str,
    status: str,
    format: str = "delta",
    processing_duration: float | None = None,
) -> MetadataResult:
    """
    Generate metadata for a processed Gold dataset.
    
    Aggregates information from DataFrame structure and query execution
    without performing any validation or data quality checks.
    
    Args:
        df: Result Spark DataFrame from analytical query execution.
        query_name: Name of the analytical query executed.
        target_table: Target Gold table name.
        layer: Processing layer (e.g., 'gold').
        object_path: Storage path for the dataset.
        status: Execution status (e.g., 'SUCCESS', 'FAILED').
        format: Storage format. Defaults to 'delta'.
        processing_duration: Processing time in seconds. Defaults to None.
        
    Returns:
        MetadataResult containing aggregated execution metadata.
    
    Example:
        >>> metadata = generate_metadata(
        ...     df=result_df,
        ...     query_name="customer_360",
        ...     target_table="ecomflow.ecom_gold.customer_360",
        ...     layer="gold",
        ...     object_path="s3://bucket/gold/customer_360",
        ...     status="SUCCESS",
        ...     processing_duration=45.2
        ... )
    """
    logger.info(f"Generating metadata for query: {query_name}")
    
    row_count = df.count()
    column_count = len(df.columns)
    columns = df.columns
    
    generated_at = datetime.now(timezone.utc).isoformat()
    
    metadata = MetadataResult(
        query_name=query_name,
        target_table=target_table,
        layer=layer,
        format=format,
        object_path=object_path,
        row_count=row_count,
        column_count=column_count,
        columns=columns,
        status=status,
        generated_at=generated_at,
        processing_duration=processing_duration,
    )
    
    logger.info(f"Metadata generated for query: {query_name} | Rows: {row_count} | Status: {status}")
    
    return metadata
