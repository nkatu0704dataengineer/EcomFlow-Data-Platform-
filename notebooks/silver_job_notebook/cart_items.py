"""
Silver Cart Items Notebook

Business Logic for Cart Items Dataset

Layer:
    Silver

Dataset:
    cart_items
"""

from __future__ import annotations

import sys
import os

# Add EcomFlow to path for framework imports
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), '../..')))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

from include.framework.silver_spark.pipeline import run_pipeline


# ==========================================================
# Schema Refactoring
# ==========================================================

def _remove_conflicting_columns(df: DataFrame) -> DataFrame:
    """
    Remove Bronze columns that will be regenerated in Silver.
    
    Drop:
    - created_at
    - updated_at
    - ingestion_time
    - source_file
    
    These will be replaced with Silver-specific values.
    """
    return df.drop("created_at", "updated_at", "ingestion_time", "source_file")


def _standardize_cart_id(df: DataFrame) -> DataFrame:
    """
    Standardize cart_id format.
    
    Bronze: cart1, cart12, cart123
    Silver: CA000001, CA000012, CA000123
    
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


# ==========================================================
# Item Sequence Generation
# ==========================================================

def _generate_item_sequence(df: DataFrame) -> DataFrame:
    """
    Generate item_sequence for each cart.
    
    Business Rule:
    - Each cart_id has multiple products
    - Each product gets a sequential number (1, 2, 3...)
    - Composite Key: (cart_id, item_sequence)
    
    Example:
        CA000001, 1, PR066
        CA000001, 2, PR775
        CA000001, 3, PR123
    """
    window_spec = Window.partitionBy("cart_id").orderBy(F.monotonically_increasing_id())
    
    return df.withColumn(
        "item_sequence",
        F.row_number().over(window_spec)
    )


# ==========================================================
# Business Enrichment
# ==========================================================

def _generate_quantity(df: DataFrame) -> DataFrame:
    """
    Generate random quantity.
    
    Business Rule:
    - quantity: 1 to 20 (inclusive)
    - Random integer distribution
    """
    return df.withColumn(
        "quantity",
        (F.rand() * 19 + 1).cast("int")  # Random 1-20
    )


def _join_product_prices(df: DataFrame) -> DataFrame:
    """
    Join product list_price from Silver products.
    
    Business Logic:
    - Read Silver products table
    - Retrieve product_listprice, rename to list_price
    - Cast to Decimal(18, 2)
    - Inner join on product_id
    
    Inner join ensures only valid product references.
    """
    # Load Silver products
    products = df.sparkSession.table("ecomflow.ecom_silver.products").select(
        "product_id",
        F.col("product_listprice").cast(DecimalType(18, 2)).alias("list_price")
    )
    
    # Inner join to keep only valid product_id
    return df.join(
        products,
        on="product_id",
        how="inner"
    )


def _calculate_item_total_amount(df: DataFrame) -> DataFrame:
    """
    Calculate item_total_amount.
    
    Formula: quantity × list_price
    
    Returns: Decimal(18, 2)
    """
    return df.withColumn(
        "item_total_amount",
        (F.col("quantity") * F.col("list_price")).cast(DecimalType(18, 2))
    )


# ==========================================================
# Timestamp Generation
# ==========================================================

def _generate_added_at(df: DataFrame) -> DataFrame:
    """
    Generate added_at timestamp.
    
    Business Rule:
    - Read Silver carts table to get cart created_at
    - added_at = cart.created_at + random(0-24 hours)
    - Inner join on cart_id
    
    Each cart item is added within 24 hours of cart creation.
    """
    # Load Silver carts
    carts = df.sparkSession.table("ecomflow.ecom_silver.carts").select(
        F.col("cart_id").alias("_cart_id_ref"),
        F.col("created_at").alias("_cart_created_at")
    )
    
    # Join and calculate added_at
    df = df.join(
        carts,
        df["cart_id"] == carts["_cart_id_ref"],
        how="inner"
    ).withColumn(
        "added_at",
        (
            F.col("_cart_created_at").cast("long") + 
            (F.rand() * 86400).cast("long")  # 0-24 hours in seconds
        ).cast("timestamp")
    ).drop("_cart_id_ref", "_cart_created_at")
    
    return df


def _generate_updated_at(df: DataFrame) -> DataFrame:
    """
    Generate updated_at timestamp.
    
    Business Rule:
    - updated_at = added_at + random(0-7 days)
    
    Cart items can be updated up to 7 days after being added.
    """
    return df.withColumn(
        "updated_at",
        (
            F.col("added_at").cast("long") + 
            (F.rand() * 7 * 86400).cast("long")  # 0-7 days in seconds
        ).cast("timestamp")
    )


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
            F.lit("generated_cart_items")
        )


# ==========================================================
# Final Schema
# ==========================================================

def _select_final_schema(df: DataFrame) -> DataFrame:
    """
    Select final Silver schema.
    
    Composite Key: (cart_id, item_sequence)
    
    Schema:
    - cart_id: CA000001, CA000002...
    - item_sequence: 1, 2, 3... (product order in cart)
    - product_id: PR066, PR775...
    - quantity, list_price, item_total_amount
    - timestamps, metadata
    """
    return df.select(
        "cart_id",
        "item_sequence",
        "product_id",
        "quantity",
        "list_price",
        "item_total_amount",
        "added_at",
        "updated_at",
        "ingestion_time",
        "source_file"
    )


# ==========================================================
# Public API
# ==========================================================

def transform(df: DataFrame) -> DataFrame:
    """
    Execute Cart Items Silver Transformation.
    
    Pipeline:
    1. Remove conflicting Bronze columns
    2. Standardize cart_id format (cart12 → CA000012)
    3. Generate item_sequence (1, 2, 3... per cart)
    4. Generate quantity (1-20)
    5. Join product prices from Silver products (inner join)
    6. Calculate item_total_amount (quantity × list_price)
    7. Generate added_at (cart.created_at + 0-24h)
    8. Generate updated_at (added_at + 0-7 days)
    9. Add metadata
    10. Select final schema
    
    Business Logic:
    - Composite Key: (cart_id, item_sequence)
    - Each cart_id has numbered items (1, 2, 3...)
    - Only cart items with valid product_id are kept (inner join)
    - Only cart items with valid cart_id are kept (inner join)
    - list_price is referenced directly from Silver products
    - All timestamps are generated relative to cart.created_at
    """
    df = _remove_conflicting_columns(df)
    
    df = _standardize_cart_id(df)
    
    df = _generate_item_sequence(df)
    
    df = _generate_quantity(df)
    
    df = _join_product_prices(df)
    
    df = _calculate_item_total_amount(df)
    
    df = _generate_added_at(df)
    
    df = _generate_updated_at(df)
    
    df = _add_metadata(df)
    
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
        source_table="cart_items",
        
        target_catalog="ecomflow",
        target_schema="ecom_silver",
        target_table="cart_items",
        target_volume="silver",
        transform=transform,
    )
    
    print(f"\n{'=' * 80}")
    print("CART ITEMS SILVER PIPELINE COMPLETED")
    print(f"{'=' * 80}")
    print(f"Source: ecomflow.ecom_bronze_v2.cart_items")
    print(f"Target: ecomflow.ecom_silver.cart_items")
    print(f"Validation Status: {'✅ PASSED' if result.validation_result.is_valid else '❌ FAILED'}")
    print(f"Row Count: {result.validation_result.row_count}")
    print(f"Processing Duration: {result.processing_duration:.2f}s")
    print(f"Business Logic: Composite Key (cart_id, item_sequence) with product pricing")
    print(f"Key Structure: Each cart has numbered items (1, 2, 3...)")
    print(f"{'=' * 80}\n")