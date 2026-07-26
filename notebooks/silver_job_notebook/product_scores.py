"""
Silver Product Scores Notebook

Business Logic for Product Scores Dataset

Layer:
    Silver

Dataset:
    product_scores

Purpose:
    Aggregate review metrics by product to avoid circular dependency
    between products and reviews tables.
    
    Pipeline Order:
    1. products.py  (no dependency on reviews)
    2. reviews.py   (uses products Silver)
    3. product_scores.py (THIS FILE - aggregates reviews)
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
# Product Scores Aggregation
# ==========================================================

def _aggregate_review_scores(df: DataFrame) -> DataFrame:
    """
    Aggregate review scores by product.
    
    Business Logic:
    - Read from reviews Silver table
    - Group by product_id
    - Calculate:
      * review_count: COUNT(review_id)
      * avg_rating: AVG(review_score) as FLOAT
    
    Products with no reviews will NOT appear in this table.
    Join products LEFT JOIN product_scores to get NULLs for unreviewed products.
    """
    return df.groupBy("product_id").agg(
        F.count("review_id").alias("review_count"),
        F.avg("review_score").cast("float").alias("avg_rating")
    )


# ==========================================================
# Final Schema
# ==========================================================

def _select_final_schema(df: DataFrame) -> DataFrame:
    """
    Select final Silver schema.
    """
    return df.select(
        "product_id",
        "review_count",
        "avg_rating"
    )


# ==========================================================
# Public API
# ==========================================================

def transform(df: DataFrame) -> DataFrame:
    """
    Execute Product Scores Silver Transformation.
    
    Pipeline:
    1. Aggregate review scores by product
    2. Select final schema
    
    Input:
    - reviews (Silver) with columns: product_id, review_id, review_score
    
    Output:
    - product_scores with schema: (product_id, review_count, avg_rating)
    """
    df = _aggregate_review_scores(df)
    df = _select_final_schema(df)
    return df


# ==========================================================
# Entry Point
# ==========================================================

if __name__ == "__main__":
    result = run_pipeline(
        spark=spark,
        source_catalog="ecomflow",
        source_schema="ecom_silver",
        source_table="reviews",

        target_catalog="ecomflow",
        target_schema="ecom_silver",
        target_table="product_scores",

        target_volume="silver",

        transform=transform,
    )
    print(f"\n{'=' * 80}")
    print("PRODUCT SCORES SILVER PIPELINE COMPLETED")
    print(f"{'=' * 80}")
    print(f"Source: ecomflow.ecom_silver.reviews")
    print(f"Target: ecomflow.ecom_silver.product_scores")
    print(f"Validation Status: {'✅ PASSED' if result.validation_result.is_valid else '❌ FAILED'}")
    print(f"Row Count: {result.validation_result.row_count}")
    print(f"Processing Duration: {result.processing_duration:.2f}s")
    print(f"{'=' * 80}\n")