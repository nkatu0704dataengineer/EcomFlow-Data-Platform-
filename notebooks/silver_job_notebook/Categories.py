"""
Silver Categories Notebook

Business Logic for Categories Dataset

Layer:
    Silver

Dataset:
    categories
"""

from __future__ import annotations

import sys
import os

# Add EcomFlow to path for framework imports
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), '../..')))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from include.framework.silver_spark.pipeline import run_pipeline


# ==========================================================
# Schema Refactoring
# ==========================================================

def _refactor_schema(df: DataFrame) -> DataFrame:
    """
    Rename columns.

    Remove unused columns.

    Reorder final schema.
    """
    return df \
        .withColumnRenamed("prcs_id", "cate_id") \
        .withColumnRenamed("prcs_name", "cate_name") \
        .drop("_c4") \
        .select(
            "cate_id",
            "cate_name",
            "created_at",
            "updated_at",
            "ingestion_time",
            "source_file"
        )


# ==========================================================
# Standardization
# ==========================================================

def _normalize_category(df: DataFrame) -> DataFrame:
    """
    Standardize category fields.

    - cate_id
    - cate_name
    """
    df = df.withColumn(
        "prcs_id",
        F.trim(F.col("prcs_id"))
    )
    
    df = df.withColumn(
        "prcs_name",
        F.regexp_replace(
            F.trim(F.col("prcs_name")),
            r"\s+",
            " "
        )
    )
    
    return df


# ==========================================================
# Datatype Conversion
# ==========================================================

def _convert_timestamps(df: DataFrame) -> DataFrame:
    """
    Convert timestamp columns.

    - created_at
    - updated_at
    """
    return df \
        .withColumn(
            "created_at",
            F.to_timestamp("created_at", "M/d/yyyy H:mm")
        ) \
        .withColumn(
            "updated_at",
            F.to_timestamp("updated_at", "M/d/yyyy H:mm")
        )


# ==========================================================
# Data Quality
# ==========================================================

def _apply_data_quality_rules(df: DataFrame) -> DataFrame:
    """
    Apply structural quality rules.

    - remove NULL cate_id
    - remove NULL cate_name
    - deduplicate cate_id
      (keep latest updated_at)
    """
    # Filter out nulls
    df = df.filter(
        F.col("prcs_id").isNotNull() & 
        F.col("prcs_name").isNotNull()
    )
    
    # Deduplicate by cate_id, keeping latest updated_at
    window_spec = Window.partitionBy("prcs_id").orderBy(F.col("updated_at").desc())
    
    df = df.withColumn(
        "row_num",
        F.row_number().over(window_spec)
    ).filter(
        F.col("row_num") == 1
    ).drop("row_num")
    
    return df


# ==========================================================
# Public API
# ==========================================================

def transform(df: DataFrame) -> DataFrame:

    df = _normalize_category(df)

    df = _convert_timestamps(df)

    df = _apply_data_quality_rules(df)

    df = _refactor_schema(df)

    return df


# ==========================================================
# Entry Point
# ==========================================================

if __name__ == "__main__":

    result = run_pipeline(
        spark=spark,
        source_catalog="ecomflow",
        source_schema="ecom_bronze_v2",
        source_table="categories",
        
        target_catalog="ecomflow",
        target_schema="ecom_silver",
        target_table="categories",
        target_volume="silver",
        transform=transform,
    )

    print(f"\n{'=' * 80}")
    print("CATEGORIES SILVER PIPELINE COMPLETED")
    print(f"{'=' * 80}")
    print(f"Source: ecomflow.ecom_bronze_v2.categories")
    print(f"Target: ecomflow.ecom_silver.categories")
    print(f"Validation Status: {'✅ PASSED' if result.validation_result.is_valid else '❌ FAILED'}")
    print(f"Row Count: {result.validation_result.row_count}")
    print(f"Processing Duration: {result.processing_duration:.2f}s")
    print(f"{'=' * 80}\n")
