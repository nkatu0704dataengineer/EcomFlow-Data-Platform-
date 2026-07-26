"""
Silver Brands Notebook

Business Logic for Brands Dataset
"""

from __future__ import annotations

import sys
import os
from pathlib import Path

# Add EcomFlow to path for framework imports
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), '../..')))

from pyspark.sql import DataFrame

from include.framework.silver_spark.pipeline import run_pipeline


# ==========================================================
# Schema Refactoring
# ==========================================================

def _refactor_schema(df: DataFrame) -> DataFrame:
    """
    Rename columns.

    Remove unused columns.

    Select final schema.
    """
    return df.select(
        "brand_id",
        "brand_name",
        "brand_country",
        "created_at",
        "updated_at",
        "ingestion_time",
        "source_file"
    )


# ==========================================================
# Standardization
# ==========================================================

def _normalize_brand_name(df: DataFrame) -> DataFrame:
    """
    Standardize brand name.
    """
    from pyspark.sql import functions as F
    
    return df.withColumn(
        "brand_name",
        F.initcap(F.trim(F.col("brand_name")))
    )


def _normalize_brand_country(df: DataFrame) -> DataFrame:
    """
    Standardize country.
    """
    from pyspark.sql import functions as F
    
    return df.withColumn(
        "brand_country",
        F.upper(F.trim(F.col("brand_country")))
    )


# ==========================================================
# Datatype Conversion
# ==========================================================

def _convert_created_at(df: DataFrame) -> DataFrame:
    """
    Convert created_at into TimestampType.
    """
    from pyspark.sql import functions as F
    
    return df.withColumn(
        "created_at",
        F.to_timestamp("created_at", "M/d/yyyy H:mm")
    )


# ==========================================================
# Missing Values
# ==========================================================

def _handle_missing_values(df: DataFrame) -> DataFrame:
    """
    Fill NULL values.
    """
    return df.fillna({
        "brand_name": "UNKNOWN",
        "brand_country": "UNKNOWN"
    })


# ==========================================================
# Data Quality
# ==========================================================

def _remove_duplicates(df: DataFrame) -> DataFrame:
    """
    Remove duplicated brand_id.
    """
    return df.dropDuplicates(["brand_id"])


# ==========================================================
# Audit
# ==========================================================

def _generate_audit_columns(df: DataFrame) -> DataFrame:
    """
    Generate updated_at.
    """
    from pyspark.sql import functions as F
    
    return df.withColumn(
        "updated_at",
        (
            F.col("created_at").cast("long")
            + (F.rand() * 180).cast("int") * 86400
        ).cast("timestamp")
    )


# ==========================================================
# Public API
# ==========================================================

def transform(df: DataFrame) -> DataFrame:

    df = _normalize_brand_name(df)

    df = _normalize_brand_country(df)

    df = _convert_created_at(df)

    df = _handle_missing_values(df)

    df = _remove_duplicates(df)

    df = _generate_audit_columns(df)

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
        source_table="brands",
        
        target_catalog="ecomflow",
        target_schema="ecom_silver",
        target_table="brands",
        target_volume="silver",
        transform=transform,
    )

    print(f"\n{'=' * 80}")
    print("BRANDS SILVER PIPELINE COMPLETED")
    print(f"{'=' * 80}")
    print(f"Source: ecomflow.ecom_bronze_v2.brands")
    print(f"Target: ecomflow.ecom_silver.brands")
    print(f"Validation Status: {'✅ PASSED' if result.validation_result.is_valid else '❌ FAILED'}")
    print(f"Row Count: {result.validation_result.row_count}")
    print(f"Processing Duration: {result.processing_duration:.2f}s")
    print(f"{'=' * 80}\n")
