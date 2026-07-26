"""
Structural Validator Module for Silver Layer Processing.

This module performs generic structural validation on DataFrames
produced by business logic transformations.

Validates framework-level properties only:
- Row counts
- Duplicate detection
- Null value analysis
- Schema structure
- Data types

Author:
    EcomFlow Data Platform Team

Layer:
    Silver Framework
"""

from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), '../../..')))

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when
from typing import Dict, Any
import logging

from include.framework.silver_spark.models.validation_result import ValidationResult

logger = logging.getLogger(__name__)


def collect_dataframe_metrics(df: DataFrame) -> Dict[str, Any]:
    """
    Collect structural metrics from a DataFrame.
    
    This function gathers quantitative measurements about the DataFrame
    structure without applying business rules or transformations.
    
    Args:
        df: Spark DataFrame to analyze.
        
    Returns:
        Dictionary containing:
            - row_count: Total number of rows
            - duplicate_count: Number of duplicate rows
            - null_details: Dict mapping column names to null counts
            - column_count: Number of columns
    
    Example:
        >>> metrics = collect_dataframe_metrics(df)
        >>> print(f"Rows: {metrics['row_count']}")
        >>> print(f"Duplicates: {metrics['duplicate_count']}")
    """
    logger.info("Collecting DataFrame structural metrics")
    
    row_count = df.count()
    
    duplicate_count = row_count - df.distinct().count()
    
    column_count = len(df.columns)
    
    agg_exprs = [
        count(when(col(c).isNull(), 1)).alias(c)
        for c in df.columns
    ]
    
    null_details = {}
    if agg_exprs:
        null_counts_df = df.agg(*agg_exprs)
        null_counts_row = null_counts_df.collect()[0]
        null_details = {c: null_counts_row[c] for c in df.columns}
    
    metrics = {
        'row_count': row_count,
        'duplicate_count': duplicate_count,
        'null_details': null_details,
        'column_count': column_count
    }
    
    logger.info(
        f"Metrics collected - Rows: {row_count}, "
        f"Duplicates: {duplicate_count}, "
        f"Columns: {column_count}"
    )
    
    return metrics


def validate_dataframe(df: DataFrame) -> ValidationResult:
    """
    Perform structural validation on a DataFrame.
    
    Validates framework-level properties and returns a ValidationResult.
    Does not apply business rules or entity-specific validation.
    
    Args:
        df: Spark DataFrame to validate.
        
    Returns:
        ValidationResult object containing validation results.
    
    Example:
        >>> result = validate_dataframe(df)
        >>> if result.is_valid:
        ...     print("Validation passed")
        >>> else:
        ...     print(f"Errors: {result.error_messages}")
    """
    logger.info("Starting structural validation")
    
    metrics = collect_dataframe_metrics(df)
    
    error_messages = []
    warning_messages = []
    
    schema_valid = True
    if metrics['column_count'] == 0:
        schema_valid = False
        error_messages.append("DataFrame has no columns")
    
    datatype_valid = True
    if not df.schema.fields:
        datatype_valid = False
        error_messages.append("DataFrame schema has no fields")
    
    if metrics['duplicate_count'] > 0:
        warning_messages.append(
            f"Found {metrics['duplicate_count']} duplicate rows"
        )
    
    total_nulls = sum(metrics['null_details'].values())
    if total_nulls > 0:
        warning_messages.append(
            f"Found {total_nulls} total null values across all columns"
        )
    
    is_valid = len(error_messages) == 0
    
    result = ValidationResult(
        is_valid=is_valid,
        row_count=metrics['row_count'],
        column_count=metrics['column_count'],
        duplicate_count=metrics['duplicate_count'],
        null_details=metrics['null_details'],
        schema_valid=schema_valid,
        datatype_valid=datatype_valid,
        error_messages=error_messages,
        warning_messages=warning_messages
    )
    
    logger.info(
        f"Validation completed - Valid: {is_valid}, "
        f"Errors: {len(error_messages)}, "
        f"Warnings: {len(warning_messages)}"
    )
    
    return result
