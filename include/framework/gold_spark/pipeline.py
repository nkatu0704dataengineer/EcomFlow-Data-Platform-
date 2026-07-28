"""
Gold Framework Pipeline Orchestrator.

This module orchestrates the Gold layer processing workflow by
coordinating Query Executor, Writer, and Metadata Generator.

Does not contain business logic - analytical logic resides in SQL queries.

Implements a clear Query → Write → Metadata architecture:
    - Query: Analytical SQL execution via QueryExecutor
    - Write: Persist result to Gold Delta table via GoldWriter
    - Metadata: Generate execution metadata

Author:
    EcomFlow Data Platform Team

Layer:
    Gold Framework
"""

from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), '../../..')))

import time
from pyspark.sql import SparkSession
import logging

from include.framework.gold_spark.query import QueryExecutor
from include.framework.gold_spark.writer import GoldWriter
from include.framework.gold_spark.metadata import generate_metadata
from include.framework.gold_spark.models.pipeline_result import PipelineResult

logger = logging.getLogger(__name__)


def run_pipeline(
    spark: SparkSession,
    query_executor: QueryExecutor,
    writer: GoldWriter,
    query_name: str,
    target_catalog: str,
    target_schema: str,
    target_table: str,
    target_volume: str,
) -> PipelineResult:
    """
    Execute the Gold Framework pipeline for analytical query processing.
    
    Orchestrates the complete Gold layer processing workflow:
    1. Execute analytical SQL query via QueryExecutor
    2. Write result to Gold Delta table via GoldWriter
    3. Generate execution metadata
    
    Architecture:
        SQL Query (analytical logic)
          ↓
        DataFrame (query result)
          ↓
        Gold Delta Table: {target_catalog}.{target_schema}.{target_table}
        
    Storage:
        Volume path: /Volumes/{target_catalog}/{target_schema}/{target_volume}/{target_table}
    
    Args:
        spark: Active Spark session.
        query_executor: QueryExecutor instance for SQL execution.
        writer: GoldWriter instance for Delta table persistence.
        query_name: Name of the SQL query file (without .sql extension).
        target_catalog: Target Unity Catalog name (Gold).
        target_schema: Target schema name (Gold).
        target_table: Target table name (Gold).
        target_volume: Target volume name for Gold storage.
        
    Returns:
        PipelineResult containing metadata and processing information.
    
    Raises:
        Any exception from QueryExecutor, GoldWriter, or MetadataGenerator.
    
    Example:
        >>> query_executor = QueryExecutor(spark, Path("queries/gold"))
        >>> writer = GoldWriter()
        >>> 
        >>> result = run_pipeline(
        ...     spark=spark,
        ...     query_executor=query_executor,
        ...     writer=writer,
        ...     query_name="customer_360",
        ...     target_catalog="ecomflow",
        ...     target_schema="ecom_gold",
        ...     target_table="customer_360",
        ...     target_volume="gold"
        ... )
    """
    logger.info(
        f"Starting Gold pipeline: {query_name} → "
        f"{target_catalog}.{target_schema}.{target_table}"
    )
    
    start_time = time.perf_counter()
    
    # Step 1: Execute Analytical Query
    logger.info(f"Executing analytical query: {query_name}")
    df_result = query_executor.execute(query_name)
    
    # Step 2: Write to Gold Delta Table
    logger.info(f"Writing to Gold table: {target_catalog}.{target_schema}.{target_table}")
    writer.write(
        df=df_result,
        catalog=target_catalog,
        schema=target_schema,
        table=target_table,
        mode="overwrite"
    )
    
    processing_duration = time.perf_counter() - start_time
    
    # Step 3: Generate Execution Metadata
    target_volume_path = f"/Volumes/{target_catalog}/{target_schema}/{target_volume}/{target_table}"
    
    logger.info("Generating execution metadata")
    metadata_result = generate_metadata(
        df=df_result,
        query_name=query_name,
        target_table=f"{target_catalog}.{target_schema}.{target_table}",
        layer="gold",
        object_path=target_volume_path,
        status="SUCCESS",
        format="delta",
        processing_duration=processing_duration
    )
    
    pipeline_result = PipelineResult(
        metadata_result=metadata_result,
        processing_duration=processing_duration,
        success=True
    )
    
    logger.info(
        f"Pipeline completed successfully: {query_name} → {target_table} "
        f"| Rows: {metadata_result.row_count} "
        f"| Duration: {processing_duration:.2f}s"
    )
    
    return pipeline_result
