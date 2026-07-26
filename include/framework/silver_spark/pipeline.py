"""
Silver Framework Pipeline Orchestrator.

This module orchestrates the Silver layer processing workflow by
coordinating Reader, Business Logic, Validator, Writer, and Metadata modules.

Does not contain business logic - transformation is injected by the caller.

Implements a clear Source → Transform → Target architecture:
    - Source: Bronze layer (source_catalog.source_schema.source_table)
    - Transform: Business logic injection via Callable
    - Target: Silver layer (target_catalog.target_schema.target_table)

Author:
    EcomFlow Data Platform Team

Layer:
    Silver Framework
"""

from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), '../../..')))

import time
from typing import Callable
from pyspark.sql import SparkSession, DataFrame
import logging

from include.framework.silver_spark.reader import read_delta_table
from include.framework.silver_spark.validator import validate_dataframe
from include.framework.silver_spark.writer import write_delta_table
from include.framework.silver_spark.metadata import generate_metadata
from include.framework.silver_spark.models.pipeline_result import PipelineResult

logger = logging.getLogger(__name__)


def run_pipeline(
    spark: SparkSession,
    source_catalog: str,
    source_schema: str,
    source_table: str,
    target_catalog: str,
    target_schema: str,
    target_table: str,
    target_volume: str,
    transform: Callable[[DataFrame], DataFrame],
) -> PipelineResult:
    """
    Execute the Silver Framework pipeline with explicit Source → Target mapping.
    
    Orchestrates the complete Silver layer processing workflow:
    1. Read from Source (Bronze layer)
    2. Execute Business Logic via injected transform function
    3. Structural Validation
    4. Write to Target (Silver layer)
    5. Generate Metadata for Target
    
    Architecture:
        Source: {source_catalog}.{source_schema}.{source_table}
          ↓
        Transform (Business Logic)
          ↓
        Target: {target_catalog}.{target_schema}.{target_table}
        
    Storage:
        Target data written to: /Volumes/{target_catalog}/{target_schema}/{target_volume}/{target_table}
    
    Args:
        spark: Active Spark session.
        source_catalog: Source Unity Catalog name (Bronze).
        source_schema: Source schema name (Bronze).
        source_table: Source table name (Bronze).
        target_catalog: Target Unity Catalog name (Silver).
        target_schema: Target schema name (Silver).
        target_table: Target table name (Silver).
        target_volume: Target volume name for Silver storage.
        transform: Business logic function that transforms the source DataFrame.
        
    Returns:
        PipelineResult containing validation, metadata, and processing info for the TARGET dataset.
    
    Raises:
        Any exception from Reader, Business Logic, Validator, Writer, or Metadata.
    
    Example:
        >>> def my_transform(df: DataFrame) -> DataFrame:
        ...     return df.filter(df.status == 'active')
        >>> 
        >>> result = run_pipeline(
        ...     spark=spark,
        ...     source_catalog="ecomflow",
        ...     source_schema="ecom_bronze",
        ...     source_table="users",
        ...     target_catalog="ecomflow",
        ...     target_schema="ecom_silver",
        ...     target_table="customers",
        ...     target_volume="silver",
        ...     transform=my_transform
        ... )
    """
    logger.info(
        f"Starting Silver pipeline: "
        f"{source_catalog}.{source_schema}.{source_table} → "
        f"{target_catalog}.{target_schema}.{target_table}"
    )
    
    start_time = time.perf_counter()
    
    # Step 1: Read from Source (Bronze)
    logger.info(f"Reading source table: {source_catalog}.{source_schema}.{source_table}")
    df_source = read_delta_table(
        spark=spark,
        catalog_name=source_catalog,
        schema_name=source_schema,
        table_name=source_table
    )
    
    # Step 2: Execute Business Transformation
    logger.info("Executing business transformation")
    df_transformed = transform(df_source)
    
    # Step 3: Structural Validation
    logger.info("Performing structural validation")
    validation_result = validate_dataframe(df_transformed)
    
    # Step 4: Construct Target Path and Write
    target_volume_path = f"/Volumes/{target_catalog}/{target_schema}/{target_volume}/{target_table}"
    logger.info(f"Target path: {target_volume_path}")
    
    logger.info("Writing to target Silver Delta table")
    write_delta_table(
        df=df_transformed,
        volume_path=target_volume_path
    )
    
    # Step 4.5: Create Unity Catalog Managed Table
    # Note: This creates a separate UC-managed table (not external table pointing to Volume)
    # Tradeoff: Duplicate storage (Volume + Table) for dual-access pattern
    logger.info(f"Creating Unity Catalog Table: {target_catalog}.{target_schema}.{target_table}")
    df_transformed.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{target_catalog}.{target_schema}.{target_table}")
    logger.info(f"Unity Catalog Table created successfully: {target_catalog}.{target_schema}.{target_table}")
    
    processing_duration = time.perf_counter() - start_time
    
    # Step 5: Generate Metadata for Target
    logger.info("Generating metadata for target dataset")
    metadata_result = generate_metadata(
        df=df_transformed,
        validation_result=validation_result,
        dataset=target_table,
        layer="silver",
        object_path=target_volume_path,
        format="delta",
        processing_duration=processing_duration
    )
    
    pipeline_result = PipelineResult(
        validation_result=validation_result,
        metadata_result=metadata_result,
        processing_duration=processing_duration,
        success=True
    )
    
    logger.info(
        f"Pipeline completed successfully: "
        f"{source_table} → {target_table} "
        f"in {processing_duration:.2f} seconds"
    )
    
    return pipeline_result
