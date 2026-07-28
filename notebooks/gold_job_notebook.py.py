# Databricks notebook source
# DBTITLE 1,Gold Framework - Orchestrate 20 Gold Tables
"""
Gold Framework Job Notebook

Orchestrates the execution of 20 Gold analytical queries.

Author: EcomFlow Data Platform Team
"""

import sys
import os
from pathlib import Path
from pyspark.sql import SparkSession
import logging

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), '..')))

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

# Define Gold tables to process
GOLD_TABLES = [
    "brand_performance",
    "brand_reviewed",
    "campaign_performance",
    "category_metrix",
    "category_performance",
    "category_status",
    "customer_360",
    "customer_reviews",
    "customer_rfm",
    "customer_top_city",
    "customer_top_district_danang",
    "customer_top_district_hanoi",
    "customer_top_district_hcm",
    "order_360",
    "order_performance",
    "payments_analysis",
    "product_performance",
    "shipping_city_district_performance",
    "subsidiary_performance",
    "subsidiary_shipping_performance",
]

# Configuration
SQL_DIRECTORY = Path("/Workspace/Users/tumaxpro99@gmail.com/EcomFlow/notebooks/gold_job_notebook")
TARGET_CATALOG = "ecomflow"
TARGET_SCHEMA = "ecom_gold"
TARGET_VOLUME = "gold"

# Initialize Gold Framework components
query_executor = QueryExecutor(spark=spark, sql_directory=SQL_DIRECTORY)
writer = GoldWriter()

logger.info(f"Starting Gold Framework job for {len(GOLD_TABLES)} tables")

# Track results
results = []
success_count = 0
failed_count = 0

# Process each table
for table_name in GOLD_TABLES:
    try:
        logger.info(f"\n{'='*80}")
        logger.info(f"Processing table: {table_name}")
        logger.info(f"{'='*80}")
        
        result = run_pipeline(
            spark=spark,
            query_executor=query_executor,
            writer=writer,
            query_name=table_name,
            target_catalog=TARGET_CATALOG,
            target_schema=TARGET_SCHEMA,
            target_table=table_name,
            target_volume=TARGET_VOLUME
        )
        
        success_count += 1
        results.append({
            "table": table_name,
            "status": "SUCCESS",
            "rows": result.metadata_result.row_count,
            "duration": result.processing_duration
        })
        
        logger.info(f"✅ SUCCESS: {table_name} | Rows: {result.metadata_result.row_count} | Duration: {result.processing_duration:.2f}s")
        
    except Exception as e:
        failed_count += 1
        results.append({
            "table": table_name,
            "status": "FAILED",
            "error": str(e),
            "duration": None
        })
        
        logger.error(f"❌ FAILED: {table_name} | Error: {str(e)}")

# Summary
logger.info(f"\n{'='*80}")
logger.info(f"GOLD FRAMEWORK JOB SUMMARY")
logger.info(f"{'='*80}")
logger.info(f"Total Tables: {len(GOLD_TABLES)}")
logger.info(f"Success: {success_count}")
logger.info(f"Failed: {failed_count}")
logger.info(f"{'='*80}\n")

# Display results as DataFrame
import pandas as pd
results_df = pd.DataFrame(results)
print(results_df.to_string(index=False))

if failed_count > 0:
    raise Exception(f"Gold job completed with {failed_count} failures. Check logs for details.")
else:
    logger.info("🎉 Gold Framework job completed successfully!")