""" 
Silver Carts Notebook

Business Logic for Carts Dataset

Layer:
    Silver

Dataset:
    carts
"""

from __future__ import annotations

import sys
import os

# Add EcomFlow to path for framework imports
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), '../..')))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from include.framework.silver_spark.pipeline import run_pipeline


# ==========================================================
# Schema Standardization
# ==========================================================

def _standardize_cart_id(df: DataFrame) -> DataFrame:
    """
    Standardize cart_id format.
    
    Bronze: cart1, cart15, cart123
    Silver: CA000001, CA000015, CA000123
    
    Extract numeric suffix and pad with leading zeros to 6 digits.
    """
    return df.withColumn(
        "cart_id",
        F.concat(
            F.lit("CA"),
            F.lpad(
                F.regexp_extract(F.col("cart_id"), r"(\d+)", 1),
                6,
                "0"
            )
        )
    )


def _rename_columns(df: DataFrame) -> DataFrame:
    """
    Rename Bronze columns to Silver schema.
    
    cart_quantity → total_items (temporary, will recalculate from cart_items)
    """
    return df.withColumnRenamed("cart_quantity", "total_items")


# ==========================================================
# Customer Mapping
# ==========================================================

def _map_customer_account(df: DataFrame) -> DataFrame:
    """
    Map Bronze user_id to Silver acc_id.
    
    Business Logic:
    - Bronze carts.user_id: "KH001", "KH002", "KH112", ...
    - Transform directly to acc_id:
      * KH001 → ACC00000001
      * KH002 → ACC00000002
      * KH112 → ACC00000112
    
    No join with customers table - direct transformation.
    """
    # Transform user_id (KH001) to acc_id (ACC00000001)
    df = df.withColumn(
        "acc_id",
        F.concat(
            F.lit("ACC"),
            F.lpad(
                F.regexp_extract(F.col("user_id"), r"KH(\d+)", 1),
                8,
                "0"
            )
        )
    ).drop("user_id")
    
    return df


# ==========================================================
# Timestamp Generation
# ==========================================================

def _generate_created_at(df: DataFrame) -> DataFrame:
    """
    Generate created_at timestamp.
    
    Business Rule:
    - Random timestamp in 2023 (2023-01-01 00:00:00 to 2023-12-31 23:59:59)
    - Epoch timestamps: 1672531200 to 1704067199
    """
    start_epoch = 1672531200  # 2023-01-01 00:00:00
    end_epoch = 1704067199    # 2023-12-31 23:59:59
    
    return df.withColumn(
        "created_at",
        (
            F.lit(start_epoch) + 
            (F.rand() * (end_epoch - start_epoch)).cast("long")
        ).cast("timestamp")
    )


def _generate_updated_at(df: DataFrame) -> DataFrame:
    """
    Generate updated_at timestamp.
    
    Business Rule:
    - updated_at = created_at + random(0-30 days)
    """
    return df.withColumn(
        "updated_at",
        (
            F.col("created_at").cast("long") + 
            (F.rand() * 30 * 86400).cast("long")  # 0-30 days in seconds
        ).cast("timestamp")
    )


# ==========================================================
# Cart Status Generation
# ==========================================================

def _generate_cart_status(df: DataFrame) -> DataFrame:
    """
    Generate cart_status based on days since update.
    
    Business Logic:
    - Calculate days_since_update from reference date (2023-12-31)
    - Generate random probability
    - Status rules:
      * days <= 30 & rand < 0.85 → ACTIVE
      * days > 30 & <= 120 & rand >= 0.85 & < 0.95 → ABANDONED
      * days > 120 → EXPIRED
      * Otherwise fallback:
        - rand < 0.85 → ACTIVE
        - rand < 0.95 → ABANDONED
        - else → EXPIRED
    
    Distribution: ACTIVE ~85%, ABANDONED ~10%, EXPIRED ~5%
    """
    reference_date = F.lit("2023-12-31").cast("timestamp")
    
    df = df.withColumn(
        "_days_since_update",
        F.datediff(reference_date, F.col("updated_at"))
    ).withColumn(
        "_rand_status",
        F.rand()
    )
    
    df = df.withColumn(
        "cart_status",
        F.when(
            (F.col("_days_since_update") <= 30) & (F.col("_rand_status") < 0.85),
            "ACTIVE"
        ).when(
            (F.col("_days_since_update") > 30) & 
            (F.col("_days_since_update") <= 120) & 
            (F.col("_rand_status") >= 0.85) & 
            (F.col("_rand_status") < 0.95),
            "ABANDONED"
        ).when(
            F.col("_days_since_update") > 120,
            "EXPIRED"
        ).otherwise(
            F.when(F.col("_rand_status") < 0.85, "ACTIVE")
             .when(F.col("_rand_status") < 0.95, "ABANDONED")
             .otherwise("EXPIRED")
        )
    ).drop("_days_since_update", "_rand_status")
    
    return df


# ==========================================================
# Cart Items Aggregation
# ==========================================================

def _calculate_cart_aggregates(df: DataFrame) -> DataFrame:
    """
    Calculate total_items and total_amount from Silver cart_items.
    
    Business Logic:
    - total_items: COUNT DISTINCT(product_id) per cart
      NOT sum(quantity) - we count unique products only
    - total_amount: SUM(item_total_amount) per cart
    - Aggregate both metrics in a single groupBy operation
    - Left join to preserve all carts
    - Carts with no items get 0 for both metrics
    
    Data Source:
    - Read from latest Silver cart_items volume (not UC table)
    """
    # Load Silver cart_items from volumes
    cart_items = df.sparkSession.read.format("delta").load(
        "/Volumes/ecomflow/ecom_silver/silver/cart_items/"
    )
    
    # Aggregate both metrics together
    cart_aggregates = cart_items.groupBy("cart_id").agg(
        F.countDistinct("product_id").alias("_calc_total_items"),
        F.sum("item_total_amount").cast(DecimalType(18, 2)).alias("_calc_total_amount")
    )
    
    # Drop temporary total_items and total_amount
    df = df.drop("total_items", "total_amount")
    
    # Left join to preserve all carts
    df = df.join(
        cart_aggregates,
        on="cart_id",
        how="left"
    )
    
    # Rename and handle NULLs (carts with no items)
    df = df \
        .withColumn(
            "total_items",
            F.coalesce(F.col("_calc_total_items"), F.lit(0))
        ) \
        .withColumn(
            "total_amount",
            F.coalesce(F.col("_calc_total_amount"), F.lit(0.0).cast(DecimalType(18, 2)))
        ) \
        .drop("_calc_total_items", "_calc_total_amount")
    
    return df


# ==========================================================
# Metadata
# ==========================================================

def _add_metadata(df: DataFrame) -> DataFrame:
    """
    Add audit metadata columns.
    """
    return df \
        .withColumn(
            "ingestion_time",
            F.current_timestamp()
        ) \
        .withColumn(
            "source_file",
            F.lit("generated_carts")
        )


# ==========================================================
# Final Schema
# ==========================================================

def _select_final_schema(df: DataFrame) -> DataFrame:
    """
    Select final Silver schema.
    
    Column order matches notebook Cell 90.
    """
    return df.select(
        "cart_id",
        "acc_id",
        "cart_status",
        "total_items",
        "total_amount",
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
    Execute Carts Silver Transformation.
    
    Pipeline:
    1. Standardize cart_id format (cart15 → CA000015)
    2. Rename columns (cart_quantity → total_items)
    3. Map customer account (user_id → acc_id via KH extraction)
    4. Generate created_at (random timestamp in 2023)
    5. Generate updated_at (created_at + 0-30 days)
    6. Generate cart_status (ACTIVE/ABANDONED/EXPIRED based on days since update)
    7. Calculate cart aggregates from Silver cart_items:
       - total_items: COUNT DISTINCT(product_id)
       - total_amount: SUM(item_total_amount)
    8. Add metadata
    9. Select final schema
    
    Business Logic:
    - Only carts with valid customer mappings are kept (inner join)
    - Carts with no items get total_items=0, total_amount=0
    - Cart status is determined by days since update and random probability
    """
    df = _standardize_cart_id(df)
    
    df = _rename_columns(df)
    
    df = _map_customer_account(df)
    
    df = _generate_created_at(df)
    
    df = _generate_updated_at(df)
    
    df = _generate_cart_status(df)
    
    df = _add_metadata(df)
    
    df = _calculate_cart_aggregates(df)
    
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
        source_table="carts",
        
        target_catalog="ecomflow",
        target_schema="ecom_silver",
        target_table="carts",
        target_volume="silver",
        transform=transform,
    )
    
    print(f"\n{'=' * 80}")
    print("CARTS SILVER PIPELINE COMPLETED")
    print(f"{'=' * 80}")
    print(f"Source: ecomflow.ecom_bronze_v2.carts")
    print(f"Target: ecomflow.ecom_silver.carts")
    print(f"Validation Status: {'✅ PASSED' if result.validation_result.is_valid else '❌ FAILED'}")
    print(f"Row Count: {result.validation_result.row_count}")
    print(f"Processing Duration: {result.processing_duration:.2f}s")
    print(f"Business Logic: Customer mapping, timestamps, status, and cart aggregates")
    print(f"{'=' * 80}\n")
