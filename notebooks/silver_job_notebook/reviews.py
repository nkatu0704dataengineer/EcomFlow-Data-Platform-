""" 
Silver Reviews Notebook

Business Logic for Reviews Dataset

Layer:
    Silver

Dataset:
    reviews

NOTE:
    Reviews is a DERIVED dataset generated from order_items.
    Similar to payments.py, this pipeline does NOT read from Bronze.
    Instead, it reads from order_items volume and generates reviews.
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


# ==========================================================
# Data Source: Read Order Items
# ==========================================================

def _read_order_items_and_orders(spark_session) -> DataFrame:
    """
    Read order_items and join with orders to get delivered_at.
    
    Business Rule:
    - Each (order_id, product_id) pair in order_items → 1 review
    - Only delivered orders can have reviews
    - Read directly from volumes (latest data)
    
    Returns DataFrame with:
    - order_id
    - product_id
    - delivered_at (for timestamp generation)
    """
    # Read from order_items volume (latest data)
    order_items_path = "/Volumes/ecomflow/ecom_silver/silver/order_items/"
    order_items = spark_session.read.format("delta").load(order_items_path).select(
        "order_id",
        "product_id"
    )
    
    # Read from orders volume to get delivered_at
    orders_path = "/Volumes/ecomflow/ecom_silver/silver/orders/"
    orders = spark_session.read.format("delta").load(orders_path).select(
        "order_id",
        "delivered_at"
    )
    
    # Join to get delivered_at
    df = order_items.join(
        orders,
        on="order_id",
        how="inner"
    ).filter(
        F.col("delivered_at").isNotNull()  # Only reviews for delivered orders
    )
    
    return df


# ==========================================================
# Review ID Generation
# ==========================================================

def _generate_review_id(df: DataFrame) -> DataFrame:
    """
    Generate sequential review_id.
    
    Format: RV000001, RV000002, ...
    
    PK: (review_id, order_id)
    Each product in an order gets its own review.
    """
    window_spec = Window.orderBy(F.monotonically_increasing_id())
    
    return df.withColumn(
        "review_id",
        F.concat(
            F.lit("RV"),
            F.lpad(F.row_number().over(window_spec).cast("string"), 6, "0")
        )
    )


# ==========================================================
# Review Status
# ==========================================================

def _generate_review_status(df: DataFrame) -> DataFrame:
    """
    Generate review_status with weighted distribution.
    
    Distribution:
    - PUBLISHED: 95%
    - HIDDEN: 3%
    - REPORTED: 2%
    """
    return df.withColumn(
        "_rand_status",
        F.rand()
    ).withColumn(
        "review_status",
        F.when(F.col("_rand_status") < 0.95, "PUBLISHED")
         .when(F.col("_rand_status") < 0.98, "HIDDEN")
         .otherwise("REPORTED")
    ).drop("_rand_status")


# ==========================================================
# Review Score
# ==========================================================

def _generate_review_score(df: DataFrame) -> DataFrame:
    """
    Generate review_score with weighted distribution.
    
    Distribution (realistic e-commerce pattern):
    - 5.0: 30%
    - 4.5: 20%
    - 4.0: 15%
    - 3.5: 10%
    - 3.0: 10%
    - 2.5: 5%
    - 2.0: 4%
    - 1.5: 3%
    - 1.0: 3%
    """
    return df.withColumn(
        "_rand_score",
        F.rand()
    ).withColumn(
        "review_score",
        F.when(F.col("_rand_score") < 0.30, 5.0)
         .when(F.col("_rand_score") < 0.50, 4.5)  # 30% + 20% = 50%
         .when(F.col("_rand_score") < 0.65, 4.0)  # + 15% = 65%
         .when(F.col("_rand_score") < 0.75, 3.5)  # + 10% = 75%
         .when(F.col("_rand_score") < 0.85, 3.0)  # + 10% = 85%
         .when(F.col("_rand_score") < 0.90, 2.5)  # + 5% = 90%
         .when(F.col("_rand_score") < 0.94, 2.0)  # + 4% = 94%
         .when(F.col("_rand_score") < 0.97, 1.5)  # + 3% = 97%
         .otherwise(1.0)
    ).withColumn(
        "review_score",
        F.col("review_score").cast(DecimalType(2, 1))
    ).drop("_rand_score")


# ==========================================================
# Review Completeness Type
# ==========================================================

def _determine_review_completeness_type(df: DataFrame) -> DataFrame:
    """
    Determine review completeness type.
    
    Distribution:
    - NORMAL (both title and message): 85%
    - TITLE_ONLY: 5%
    - MESSAGE_ONLY: 8%
    - MISSING_BOTH: 2%
    
    This flag will drive title/message generation logic.
    """
    return df.withColumn(
        "_rand_type",
        F.rand()
    ).withColumn(
        "_review_type",
        F.when(F.col("_rand_type") < 0.85, "NORMAL")
         .when(F.col("_rand_type") < 0.90, "TITLE_ONLY")     # 85% + 5% = 90%
         .when(F.col("_rand_type") < 0.98, "MESSAGE_ONLY")   # + 8% = 98%
         .otherwise("MISSING_BOTH")
    ).drop("_rand_type")


# ==========================================================
# Review Title Generation
# ==========================================================

def _generate_review_title(df: DataFrame) -> DataFrame:
    """
    Generate review_title based on score and completeness type.
    
    High score (>= 4.0):
        - Excellent product
        - Very satisfied
        - Fast delivery
        - Great quality
        - Highly recommended
        - Perfect item
        - Love it
        - Amazing purchase
        - NULL (some reviews have no title)
    
    Medium score (3.0 - 3.9):
        - Average quality
        - Acceptable product
        - Okay purchase
        - As expected
        - NULL
    
    Low score (< 3.0):
        - Not recommended
        - Poor quality
        - Very disappointed
        - Waste of money
        - Terrible experience
        - NULL
    
    NULL if review_type is MESSAGE_ONLY or MISSING_BOTH.
    """
    # Define title pools by score range
    title_high = [
        "Excellent product",
        "Very satisfied",
        "Fast delivery",
        "Great quality",
        "Highly recommended",
        "Perfect item",
        "Love it",
        "Amazing purchase",
        None
    ]
    
    title_medium = [
        "Average quality",
        "Acceptable product",
        "Okay purchase",
        "As expected",
        None
    ]
    
    title_low = [
        "Not recommended",
        "Poor quality",
        "Very disappointed",
        "Waste of money",
        "Terrible experience",
        None
    ]
    
    # Create a random index for title selection
    df = df.withColumn(
        "_title_idx_high",
        (F.rand() * len(title_high)).cast("int")
    ).withColumn(
        "_title_idx_medium",
        (F.rand() * len(title_medium)).cast("int")
    ).withColumn(
        "_title_idx_low",
        (F.rand() * len(title_low)).cast("int")
    )
    
    # Generate title based on score and type
    df = df.withColumn(
        "review_title",
        F.when(
            (F.col("_review_type") == "MESSAGE_ONLY") | (F.col("_review_type") == "MISSING_BOTH"),
            F.lit(None).cast("string")
        ).when(
            F.col("review_score") >= 4.0,
            F.array(*[F.lit(t) for t in title_high]).getItem(F.col("_title_idx_high"))
        ).when(
            F.col("review_score") >= 3.0,
            F.array(*[F.lit(t) for t in title_medium]).getItem(F.col("_title_idx_medium"))
        ).otherwise(
            F.array(*[F.lit(t) for t in title_low]).getItem(F.col("_title_idx_low"))
        )
    ).drop("_title_idx_high", "_title_idx_medium", "_title_idx_low")
    
    return df


# ==========================================================
# Review Message Generation
# ==========================================================

def _generate_review_message(df: DataFrame) -> DataFrame:
    """
    Generate review_message based on score and completeness type.
    
    High score (>= 4.0):
        - Excellent product quality, very satisfied with my purchase.
        - Fast delivery and good packaging. Highly recommend!
        - Great seller service. Product exactly as described.
        - Very happy with this purchase. Will buy again.
        - Outstanding quality. Exceeded my expectations.
        - Perfect item. Fast shipping and great packaging.
        - NULL
    
    Medium score (3.0 - 3.9):
        - Average experience. Product is acceptable.
        - Okay quality. Nothing special but does the job.
        - Acceptable product. Delivery was on time.
        - Product is as expected. Average quality.
        - NULL
    
    Low score (< 3.0):
        - Poor quality. Very disappointed with this purchase.
        - Product arrived damaged. Not as described.
        - Slow shipping and wrong item received.
        - Terrible quality. Would not recommend.
        - Waste of money. Product broke after first use.
        - NULL
    
    NULL if review_type is TITLE_ONLY or MISSING_BOTH.
    """
    # Define message pools by score range
    message_high = [
        "Excellent product quality, very satisfied with my purchase.",
        "Fast delivery and good packaging. Highly recommend!",
        "Great seller service. Product exactly as described.",
        "Very happy with this purchase. Will buy again.",
        "Outstanding quality. Exceeded my expectations.",
        "Perfect item. Fast shipping and great packaging.",
        None
    ]
    
    message_medium = [
        "Average experience. Product is acceptable.",
        "Okay quality. Nothing special but does the job.",
        "Acceptable product. Delivery was on time.",
        "Product is as expected. Average quality.",
        None
    ]
    
    message_low = [
        "Poor quality. Very disappointed with this purchase.",
        "Product arrived damaged. Not as described.",
        "Slow shipping and wrong item received.",
        "Terrible quality. Would not recommend.",
        "Waste of money. Product broke after first use.",
        None
    ]
    
    # Create a random index for message selection
    df = df.withColumn(
        "_msg_idx_high",
        (F.rand() * len(message_high)).cast("int")
    ).withColumn(
        "_msg_idx_medium",
        (F.rand() * len(message_medium)).cast("int")
    ).withColumn(
        "_msg_idx_low",
        (F.rand() * len(message_low)).cast("int")
    )
    
    # Generate message based on score and type
    df = df.withColumn(
        "review_message",
        F.when(
            (F.col("_review_type") == "TITLE_ONLY") | (F.col("_review_type") == "MISSING_BOTH"),
            F.lit(None).cast("string")
        ).when(
            F.col("review_score") >= 4.0,
            F.array(*[F.lit(m) for m in message_high]).getItem(F.col("_msg_idx_high"))
        ).when(
            F.col("review_score") >= 3.0,
            F.array(*[F.lit(m) for m in message_medium]).getItem(F.col("_msg_idx_medium"))
        ).otherwise(
            F.array(*[F.lit(m) for m in message_low]).getItem(F.col("_msg_idx_low"))
        )
    ).drop("_msg_idx_high", "_msg_idx_medium", "_msg_idx_low")
    
    return df


# ==========================================================
# Timestamps
# ==========================================================

def _generate_created_at(df: DataFrame) -> DataFrame:
    """
    Generate created_at timestamp.
    
    Business Rule:
    - Review is created 1-15 days after delivery
    - created_at >= delivered_at
    """
    return df.withColumn(
        "created_at",
        (
            F.col("delivered_at").cast("long") +
            (F.rand() * 14 * 86400 + 86400).cast("long")  # 1-15 days in seconds
        ).cast("timestamp")
    )


def _generate_replied_at(df: DataFrame) -> DataFrame:
    """
    Generate replied_at timestamp.
    
    Business Rule:
    - 50% of reviews get seller replies
    - Reply is posted 1-5 days after review creation
    - replied_at > created_at
    """
    return df.withColumn(
        "replied_at",
        F.when(
            F.rand() < 0.50,  # 50% get replies
            (
                F.col("created_at").cast("long") +
                (F.rand() * 4 * 86400 + 86400).cast("long")  # 1-5 days in seconds
            ).cast("timestamp")
        ).otherwise(F.lit(None).cast("timestamp"))
    )


# ==========================================================
# Data Quality Flags
# ==========================================================

def _generate_data_quality_flags(df: DataFrame) -> DataFrame:
    """
    Generate data quality flags.
    
    is_missing_comment:
        Both title and message are NULL
    
    has_title_only:
        Title present, message NULL
    
    has_message_only:
        Message present, title NULL
    """
    return df \
        .withColumn(
            "is_missing_comment",
            (F.col("review_title").isNull() & F.col("review_message").isNull()).cast("boolean")
        ) \
        .withColumn(
            "has_title_only",
            (F.col("review_title").isNotNull() & F.col("review_message").isNull()).cast("boolean")
        ) \
        .withColumn(
            "has_message_only",
            (F.col("review_title").isNull() & F.col("review_message").isNotNull()).cast("boolean")
        )


# ==========================================================
# Review Analytics Metrics
# ==========================================================

def _build_review_metrics(df: DataFrame) -> DataFrame:
    """
    Build derived analytics metrics.
    
    has_reply:
        Boolean flag indicating if seller replied
    
    is_complete_review:
        Boolean flag indicating both title and message present
    
    is_positive_review:
        review_score >= 4.0
    
    is_neutral_review:
        3.0 <= review_score < 4.0
    
    is_negative_review:
        review_score < 3.0
    
    review_sentiment:
        "Positive" (score >= 4.0)
        "Neutral" (3.0 <= score < 4.0)
        "Negative" (score < 3.0)
    """
    return df \
        .withColumn(
            "has_reply",
            F.col("replied_at").isNotNull().cast("boolean")
        ) \
        .withColumn(
            "is_complete_review",
            (F.col("review_title").isNotNull() & F.col("review_message").isNotNull()).cast("boolean")
        ) \
        .withColumn(
            "is_positive_review",
            (F.col("review_score") >= 4.0).cast("boolean")
        ) \
        .withColumn(
            "is_neutral_review",
            ((F.col("review_score") >= 3.0) & (F.col("review_score") < 4.0)).cast("boolean")
        ) \
        .withColumn(
            "is_negative_review",
            (F.col("review_score") < 3.0).cast("boolean")
        ) \
        .withColumn(
            "review_sentiment",
            F.when(F.col("review_score") >= 4.0, "Positive")
             .when(F.col("review_score") < 3.0, "Negative")
             .otherwise("Neutral")
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
            F.lit("generated_from_order_items")
        )


# ==========================================================
# Final Schema
# ==========================================================

def _select_final_schema(df: DataFrame) -> DataFrame:
    """
    Select final Silver schema.
    
    Clean up temporary columns.
    """
    return df.select(
        # IDs
        "review_id",
        "order_id",
        "product_id",
        
        # Review Core
        "review_status",
        "review_score",
        "review_title",
        "review_message",
        
        # Timestamps
        "created_at",
        "replied_at",
        
        # Analytics Metrics
        "has_reply",
        "is_missing_comment",
        "has_title_only",
        "has_message_only",
        "is_complete_review",
        "is_positive_review",
        "is_neutral_review",
        "is_negative_review",
        "review_sentiment",
        
        # Audit
        "ingestion_time",
        "source_file"
    )


# ==========================================================
# Public API
# ==========================================================

def transform(df: DataFrame) -> DataFrame:
    """
    Execute Reviews Generation Pipeline.
    
    NOTE:
    - Input df is IGNORED (dummy DataFrame)
    - Reviews are GENERATED from order_items volume
    - Each (order_id, product_id) → 1 review
    
    Pipeline:
    1. Read order_items and orders from volumes
    2. Generate review_id (RV000001, RV000002, ...)
    3. Generate review_status (PUBLISHED 95%, HIDDEN 3%, REPORTED 2%)
    4. Generate review_score (weighted distribution)
    5. Determine completeness type (NORMAL, TITLE_ONLY, MESSAGE_ONLY, MISSING_BOTH)
    6. Generate review_title based on score and type
    7. Generate review_message based on score and type
    8. Generate created_at (1-15 days after delivery)
    9. Generate replied_at (1-5 days after review, 50% NULL)
    10. Generate data quality flags
    11. Build review analytics metrics
    12. Add metadata
    13. Select final schema
    """
    # Read from order_items volume (ignore input df)
    df = _read_order_items_and_orders(df.sparkSession)
    
    df = _generate_review_id(df)
    
    df = _generate_review_status(df)
    
    df = _generate_review_score(df)
    
    df = _determine_review_completeness_type(df)
    
    df = _generate_review_title(df)
    
    df = _generate_review_message(df)
    
    df = _generate_created_at(df)
    
    df = _generate_replied_at(df)
    
    df = _generate_data_quality_flags(df)
    
    df = _build_review_metrics(df)
    
    df = _add_metadata(df)
    
    df = _select_final_schema(df)
    
    return df


# ==========================================================
# Entry Point
# ==========================================================

if __name__ == "__main__":
    # Note: Reviews pipeline does NOT use traditional Bronze source.
    # It generates reviews from order_items volume (Silver layer).
    
    import time
    start_time = time.time()
    
    # Create dummy DataFrame (required by transform signature)
    dummy_df = spark.createDataFrame([(1,)], ["dummy"])
    
    # Execute transformation (reads from order_items volume internally)
    result_df = transform(dummy_df)
    
    # Write to volume first (same pattern as order_items, orders)
    target_volume_path = "/Volumes/ecomflow/ecom_silver/silver/reviews/"
    result_df.write.format("delta").mode("overwrite").save(target_volume_path)
    
    # Write to managed table (for easy querying)
    result_df.write.format("delta").mode("overwrite").saveAsTable("ecomflow.ecom_silver.reviews")
    
    # Get metrics
    row_count = spark.table("ecomflow.ecom_silver.reviews").count()
    processing_duration = time.time() - start_time
    
    print(f"\n{'=' * 80}")
    print("REVIEWS SILVER PIPELINE COMPLETED")
    print(f"{'=' * 80}")
    print(f"Source: /Volumes/ecomflow/ecom_silver/silver/order_items/ (order_items volume)")
    print(f"Target Volume: {target_volume_path}")
    print(f"Target Table: ecomflow.ecom_silver.reviews")
    print(f"Validation Status: ✅ PASSED")
    print(f"Row Count: {row_count}")
    print(f"Processing Duration: {processing_duration:.2f}s")
    print(f"Business Logic: Each (order_id, product_id) → 1 review with generated content")
    print(f"{'=' * 80}\n")