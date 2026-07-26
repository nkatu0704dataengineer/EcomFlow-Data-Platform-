"""
Silver Product Sales Notebook

Business Logic for Product Sales Dataset

Layer:
    Silver

Dataset:
    product_sales
"""

from __future__ import annotations

import sys
import os

# Add EcomFlow to path for framework imports
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), '../..')))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from include.framework.silver_spark.pipeline import run_pipeline


# ==========================================================
# Product Sale Relationship
# ==========================================================

def _build_product_sales(df: DataFrame) -> DataFrame:
    """
    Build the Product Sales bridge table from order_items.
    
    Extracts unique (product_id, sale_id) combinations where sale_id exists,
    and captures the earliest order date for each combination.
    
    Business Logic:
    - Filter: Only orders with sale_id (promotional orders)
    - Aggregate: Group by (product_id, sale_id)
    - Capture: Earliest created_at as earliest_order_date
    """
    return df \
        .filter(F.col("sale_id").isNotNull()) \
        .groupBy("product_id", "sale_id") \
        .agg(
            F.min(F.col("created_at")).alias("earliest_order_date")
        )


# ==========================================================
# Audit Columns
# ==========================================================

def _generate_audit_columns(df: DataFrame) -> DataFrame:
    """
    Generate audit timestamps for product-sale relationships.
    
    Business Rules (from notebook Cell 108):
    - created_at: When the product was added to the sale program
      Uses generate_created_at UDF: random timestamp 7-90 days before earliest order
    
    - updated_at: Last update to the product-sale relationship
      Uses generate_updated_at UDF: random timestamp between created_at and earliest_order_date
    
    Uses deterministic random seeds based on (product_id, sale_id)
    to ensure reproducible results.
    """
    # Define UDF: Generate created_at timestamp between 7 and 90 days before earliest order
    def generate_created_at(earliest_order_ts, seed):
        """
        Generate created_at timestamp between 7 and 90 days before earliest order
        """
        if earliest_order_ts is None:
            return None
            
        import random
        from datetime import datetime, timedelta
        
        random.seed(seed)
        
        # Random days between 7 and 90
        days_before = random.randint(7, 90)
        
        # Convert timestamp to datetime
        earliest_order = datetime.fromtimestamp(earliest_order_ts / 1000.0)
        
        # Subtract days
        created_date = earliest_order - timedelta(days=days_before)
        
        return created_date
    
    created_at_udf = F.udf(generate_created_at, "timestamp")
    
    # Define UDF: Generate updated_at timestamp between created_at and earliest order
    def generate_updated_at(created_ts, earliest_order_ts, seed):
        """
        Generate updated_at timestamp between created_at and earliest order
        """
        if created_ts is None or earliest_order_ts is None:
            return None
            
        import random
        from datetime import datetime, timedelta
        
        random.seed(seed + 12345)  # Different seed for variation
        
        created = datetime.fromtimestamp(created_ts / 1000.0)
        earliest_order = datetime.fromtimestamp(earliest_order_ts / 1000.0)
        
        # Calculate time difference in seconds
        time_diff_seconds = int((earliest_order - created).total_seconds())
        
        if time_diff_seconds <= 0:
            # Edge case: if created is somehow >= earliest_order, just use created
            return created
        
        # Random point between created and earliest_order
        random_seconds = random.randint(0, time_diff_seconds)
        
        updated = created + timedelta(seconds=random_seconds)
        
        return updated
    
    updated_at_udf = F.udf(generate_updated_at, "timestamp")
    
    # Create deterministic seed from product_id and sale_id
    df = df.withColumn(
        "seed",
        F.abs(F.hash(F.concat(F.col("product_id"), F.col("sale_id"))))
    )
    
    # Generate created_at using UDF
    df = df.withColumn(
        "created_at",
        created_at_udf(
            F.col("earliest_order_date").cast("long") * 1000,
            F.col("seed")
        )
    )
    
    # Generate updated_at using UDF
    df = df.withColumn(
        "updated_at",
        updated_at_udf(
            F.col("created_at").cast("long") * 1000,
            F.col("earliest_order_date").cast("long") * 1000,
            F.col("seed")
        )
    )
    
    # Clean up temporary columns
    return df.drop("seed", "earliest_order_date")


# ==========================================================
# Final Schema Selection
# ==========================================================

def _select_final_schema(df: DataFrame) -> DataFrame:
    """
    Select final Silver schema columns in correct order.
    """
    return df.select(
        "product_id",
        "sale_id",
        "created_at",
        "updated_at"
    )


# ==========================================================
# Public API
# ==========================================================

def transform(df: DataFrame) -> DataFrame:
    """
    Main transformation pipeline for Product Sales Silver Layer.
    
    Product Sales is a bridge table that tracks which products
    are included in which promotional sales programs.
    
    Source: order_items (Silver)
    Output: Unique (product_id, sale_id) combinations with audit timestamps
    
    Applies:
    1. Build product-sale relationships from order_items
    2. Generate audit timestamps (created_at, updated_at)
    3. Final schema selection
    """
    df = _build_product_sales(df)
    df = _generate_audit_columns(df)
    df = _select_final_schema(df)
    return df


# ==========================================================
# Entry Point
# ==========================================================

if __name__ == "__main__":
    result=run_pipeline(
        spark=spark,
        source_catalog="ecomflow",
        source_schema="ecom_silver",
        source_table="order_items",

        target_catalog="ecomflow",
        target_schema="ecom_silver",
        target_table="product_sales",

        target_volume="silver",

        transform=transform,
    )
    print(f"\n{'=' * 80}")
    print("PRODUCT SALES SILVER PIPELINE COMPLETED")
    print(f"{'=' * 80}")
    print(f"Source: ecomflow.ecom_silver.order_items")
    print(f"Target: ecomflow.ecom_silver.product_sales")
    print(f"Validation Status: {'✅ PASSED' if result.validation_result.is_valid else '❌ FAILED'}")
    print(f"Row Count: {result.validation_result.row_count}")
    print(f"Processing Duration: {result.processing_duration:.2f}s")
    print(f"{'=' * 80}\n")