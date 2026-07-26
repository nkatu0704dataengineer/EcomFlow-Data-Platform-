"""
Silver Program Sales Notebook

Business Logic for Program Sales Dataset

Layer:
    Silver

Dataset:
    program_sales
"""

from __future__ import annotations

import sys
import os

# Add EcomFlow to path for framework imports
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), '../..')))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType

from include.framework.silver_spark.pipeline import run_pipeline


# ==========================================================
# Schema Refactoring
# ==========================================================

def _refactor_schema(df: DataFrame) -> DataFrame:
    """
    Rename timestamp columns and drop obsolete columns.
    
    - sale_createdat → created_at
    - sale_updateat → updated_at
    - Drop sale_status (obsolete)
    """
    return df \
        .withColumnRenamed("sale_createdat", "created_at") \
        .withColumnRenamed("sale_updateat", "updated_at") \
        .drop("sale_status")


# ==========================================================
# Data Type Conversions
# ==========================================================

def _convert_data_types(df: DataFrame) -> DataFrame:
    """
    Convert min_ordervalue from INTEGER to DECIMAL(18,2).
    """
    return df.withColumn(
        "min_ordervalue",
        F.col("min_ordervalue").cast(DecimalType(18, 2))
    )


# ==========================================================
# Standardization
# ==========================================================

def _normalize_program_sale_name(df: DataFrame) -> DataFrame:
    """
    Transform sale names from 'salename1' to 'Program Sale 01'.
    
    Extracts the numeric suffix and formats with leading zeros
    for single-digit numbers.
    
    Examples:
    - salename1 → Program Sale 01
    - salename10 → Program Sale 10
    - salename99 → Program Sale 99
    """
    # Extract number from 'salenameN' pattern
    df = df.withColumn(
        "sale_number",
        F.regexp_extract(F.col("sale_name"), r"salename(\d+)", 1).cast("int")
    )
    
    # Format as 'Program Sale NN' with zero-padding
    df = df.withColumn(
        "sale_name",
        F.concat(
            F.lit("Program Sale "),
            F.format_string("%02d", F.col("sale_number"))
        )
    )
    
    return df.drop("sale_number")


# ==========================================================
# Date Processing
# ==========================================================

def _convert_dates(df: DataFrame) -> DataFrame:
    """
    Convert timestamp columns to date columns.
    
    - sale_startdate (timestamp) → start_date (date)
    - sale_enddate (timestamp) → end_date (date)
    """
    df = df.withColumn(
        "start_date",
        F.to_date(F.col("sale_startdate"))
    ).drop("sale_startdate")
    
    df = df.withColumn(
        "end_date",
        F.to_date(F.col("sale_enddate"))
    ).drop("sale_enddate")
    
    return df


# ==========================================================
# Data Quality
# ==========================================================

def _validate_business_rules(df: DataFrame) -> DataFrame:
    """
    Validate business rules and filter invalid records.
    
    Rules:
    - start_date < end_date
    - created_at <= start_date
    - updated_at >= created_at
    """
    return df.filter(
        (F.col("start_date") < F.col("end_date")) &
        (F.to_date(F.col("created_at")) <= F.col("start_date")) &
        (F.col("updated_at") >= F.col("created_at"))
    )


# ==========================================================
# Final Schema Selection
# ==========================================================

def _select_final_schema(df: DataFrame) -> DataFrame:
    """
    Select final Silver schema columns in correct order.
    """
    return df.select(
        "sale_id",
        "sale_name",
        "discount_value",
        "min_ordervalue",
        "start_date",
        "end_date",
        "created_at",
        "updated_at",
        "ingestion_time",
        "source_file"
    )


# ==========================================================
# Public API
# ==========================================================

def transform(df: DataFrame) -> DataFrame:
    """
    Main transformation pipeline for Program Sales Silver Layer.
    
    Applies:
    1. Schema refactoring (column renaming, dropping obsolete columns)
    2. Data type conversions
    3. Program name standardization
    4. Date conversions
    5. Business rule validation
    6. Final schema selection
    """
    df = _refactor_schema(df)
    df = _convert_data_types(df)
    df = _normalize_program_sale_name(df)
    df = _convert_dates(df)
    df = _validate_business_rules(df)
    df = _select_final_schema(df)
    return df


# ==========================================================
# Entry Point
# ==========================================================

if __name__ == "__main__":
    result=run_pipeline(
        spark=spark,
        source_catalog="ecomflow",
        source_schema="ecom_bronze_v2",
        source_table="program_sales",

        target_catalog="ecomflow",
        target_schema="ecom_silver",
        target_table="program_sales",

        target_volume="silver",

        transform=transform,
    )
    print(f"\n{'=' * 80}")
    print("PROGRAM SALES SILVER PIPELINE COMPLETED")
    print(f"{'=' * 80}")
    print(f"Source: ecomflow.ecom_bronze_v2.program_sales")
    print(f"Target: ecomflow.ecom_silver.program_sales")
    print(f"Validation Status: {'✅ PASSED' if result.validation_result.is_valid else '❌ FAILED'}")
    print(f"Row Count: {result.validation_result.row_count}")
    print(f"Processing Duration: {result.processing_duration:.2f}s")
    print(f"{'=' * 80}\n")