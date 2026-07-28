# Databricks notebook source
# DBTITLE 1,Gold Single Table Processor
"""
Gold Single Table Processor

Processes a single Gold analytical table based on parameters.
Designed for parallel execution in multi-task jobs.

Parameters:
- table_name: Name of the Gold table to process
- catalog_name: Target catalog (default: ecomflow)
- schema_name: Target schema (default: ecom_gold)
- volume_name: Target volume (default: gold)
"""

import sys
import os
from pathlib import Path
from pyspark.sql import SparkSession
import logging

# Get parameters from dbutils widgets
dbutils.widgets.text("table_name", "", "Table Name")
dbutils.widgets.text("catalog_name", "ecomflow", "Catalog Name")
dbutils.widgets.text("schema_name", "ecom_gold", "Schema Name")
dbutils.widgets.text("volume_name", "gold", "Volume Name")

table_name = dbutils.widgets.get("table_name")
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
volume_name = dbutils.widgets.get("volume_name")

if not table_name:
    raise ValueError("Parameter 'table_name' is required")

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), '../..')))

from include.framework.gold_spark.query import QueryExecutor
from include.framework.gold_spark.writer import GoldWriter
from include.framework.gold_spark.pipeline import run_pipeline

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Configuration
SQL_DIRECTORY = Path("/Workspace/Users/tumaxpro99@gmail.com/EcomFlow-Data-Platform-/notebooks/gold_job_notebook")

# Initialize Gold Framework components
query_executor = QueryExecutor(spark=spark, sql_directory=SQL_DIRECTORY)
writer = GoldWriter()

logger.info(f"{'='*80}")
logger.info(f"Processing Gold table: {table_name}")
logger.info(f"Target: {catalog_name}.{schema_name}.{table_name}")
logger.info(f"{'='*80}")

try:
    result = run_pipeline(
        spark=spark,
        query_executor=query_executor,
        writer=writer,
        query_name=table_name,
        target_catalog=catalog_name,
        target_schema=schema_name,
        target_table=table_name,
        target_volume=volume_name
    )
    
    logger.info(f"✅ SUCCESS: {table_name}")
    logger.info(f"   Rows: {result.metadata_result.row_count}")
    logger.info(f"   Duration: {result.processing_duration:.2f}s")
    logger.info(f"{'='*80}")
    
    # Output for downstream tasks
    dbutils.notebook.exit({
        "status": "SUCCESS",
        "table": table_name,
        "rows": result.metadata_result.row_count,
        "duration": result.processing_duration
    })
    
except Exception as e:
    logger.error(f"❌ FAILED: {table_name}")
    logger.error(f"   Error: {str(e)}")
    logger.error(f"{'='*80}")
    raise

# COMMAND ----------

