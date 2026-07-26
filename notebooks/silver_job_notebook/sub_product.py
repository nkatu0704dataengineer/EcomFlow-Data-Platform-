"""
Silver Sub Products Notebook

Business Logic for Sub Products Dataset

Layer:
    Silver

Dataset:
    sub_products
"""

from __future__ import annotations

import os
import sys
# Add EcomFlow to path for framework imports
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), '../..')))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from include.framework.silver_spark.pipeline import run_pipeline


# ==========================================================
# Schema Refactoring
# ==========================================================

def _refactor_schema(df: DataFrame) -> DataFrame:
    """
    Rename Bronze columns to Silver naming convention.
    
    SUB_ID -> subsidiary_id
    PRODUCT_ID -> product_id
    QUANTITY -> quantity
    """
    return df \
        .withColumnRenamed("SUB_ID", "subsidiary_id") \
        .withColumnRenamed("PRODUCT_ID", "product_id") \
        .withColumnRenamed("QUANTITY", "quantity")


# ==========================================================
# Inventory Lifecycle
# ==========================================================

def _generate_inventory_timestamps(df: DataFrame) -> DataFrame:
    """
    Generate created_at and updated_at timestamps.
    
    Business Rules:
    - created_at: When product became available in subsidiary
      Random timestamp between 2022-01-01 and 2023-12-31
    
    - updated_at: Latest inventory update
      Random timestamp between (created_at + 30 days) and 2026-06-09
      Must satisfy: created_at <= updated_at
    """
    # Step 1: Generate created_at (2022-01-01 to 2023-12-31)
    # 730 days = ~2 years
    df = df.withColumn(
        "created_at",
        (
            F.unix_timestamp(F.lit("2022-01-01 00:00:00")) +
            (F.rand() * 730 * 86400).cast("int")
        ).cast("timestamp")
    )
    
    # Step 2: Generate updated_at (created_at + 30 days to 2026-06-09)
    # Calculate days between created_at and reference date
    df = df.withColumn(
        "days_available",
        F.datediff(F.lit("2026-06-09"), F.col("created_at"))
    )
    
    # Generate updated_at ensuring it's at least 30 days after created_at
    df = df.withColumn(
        "updated_at",
        (
            F.unix_timestamp("created_at") +
            (30 * 86400) +  # Minimum 30 days
            (F.rand() * F.greatest(F.col("days_available") - 30, F.lit(1)) * 86400).cast("int")
        ).cast("timestamp")
    )
    
    return df.drop("days_available")


# ==========================================================
# Data Quality
# ==========================================================

def _validate_inventory(df: DataFrame) -> DataFrame:
    """
    Validate inventory data quality.
    
    Ensures:
    - created_at <= updated_at (business rule)
    - No null timestamps
    - Valid quantity (positive integers)
    """
    # Filter out any invalid records
    # (In production these are already clean, but good practice)
    return df.filter(
        (F.col("created_at").isNotNull()) &
        (F.col("updated_at").isNotNull()) &
        (F.col("created_at") <= F.col("updated_at")) &
        (F.col("quantity").isNotNull()) &
        (F.col("quantity") >= 0)
    )


# ==========================================================
# Final Schema Selection
# ==========================================================

def _select_final_schema(df: DataFrame) -> DataFrame:
    """
    Select final Silver schema columns in correct order.
    """
    return df.select(
        "subsidiary_id",
        "product_id",
        "quantity",
        "ingestion_time",
        "source_file",
        "created_at",
        "updated_at"
    )


# ==========================================================
# Public API
# ==========================================================

def transform(df: DataFrame) -> DataFrame:
    """
    Main transformation pipeline for Sub Products Silver Layer.
    
    Applies:
    1. Schema refactoring (column renaming)
    2. Inventory timestamp generation
    3. Data quality validation
    4. Final schema selection
    """
    df = _refactor_schema(df)
    df = _generate_inventory_timestamps(df)
    df = _validate_inventory(df)
    df = _select_final_schema(df)
    return df


# ==========================================================
# Entry Point
# ==========================================================

if __name__ == "__main__":
    result = run_pipeline(
        spark=spark,
        source_catalog="ecomflow",
        source_schema="ecom_bronze_v2",
        source_table="sub_products",
        
        target_catalog="ecomflow",
        target_schema="ecom_silver",
        target_table="sub_products",
        target_volume="silver",
        transform=transform,
    )
    print(f"\n{'=' * 80}")
    print("SUB_PRODUCTS SILVER PIPELINE COMPLETED")
    print(f"{'=' * 80}")
    print(f"Source: ecomflow.ecom_bronze_v2.sub_products")
    print(f"Target: ecomflow.ecom_silver.sub_products")
    print(f"Validation Status: {'✅ PASSED' if result.validation_result.is_valid else '❌ FAILED'}")
    print(f"Row Count: {result.validation_result.row_count}")
    print(f"Processing Duration: {result.processing_duration:.2f}s")
    print(f"{'=' * 80}\n")