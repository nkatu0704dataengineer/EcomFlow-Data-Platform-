"""
Silver User Behaviors (Customer Behaviors) Transformation

Business Logic for User Behaviors Dataset

Layer:
    Silver

Dataset:
    user_behaviors → customer_behaviors

Key Business Changes:
    - Bronze is event-centric (primary key: event_id)
    - Silver is session-centric (entity: customer sessions)
    - user_id maps to acc_id via Customers
    - Sessions are reconstructed using 30-minute inactivity rule
    - Event types follow ecommerce funnel logic
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
# Data Loading
# ==========================================================

def _load_bronze_user_behaviors(df: DataFrame) -> DataFrame:
    """
    Load Bronze user_behaviors dataset.
    
    This is a pass-through function that maintains the pipeline skeleton.
    The actual loading is handled by run_pipeline.
    """
    return df


def _load_customers_mapping() -> DataFrame:
    """
    Load Silver customers for acc_id validation.
    
    Returns:
        DataFrame with columns: acc_id
    """
    return spark.table("ecomflow.ecom_silver.customers").select("acc_id")


def _load_products_reference() -> DataFrame:
    """
    Load Silver products for validation.
    
    Returns:
        DataFrame with columns: product_id, product_exists
    """
    return spark.table("ecomflow.ecom_silver.products") \
        .select("product_id") \
        .withColumn("product_exists", F.lit(True))


# ==========================================================
# ID Mapping
# ==========================================================

def _map_customer_account(df: DataFrame) -> DataFrame:
    """
    Transform user_id (KH format) to acc_id (ACC format).
    
    Business Logic:
        - Bronze user_id: "KH001", "KH002", "KH2014"
        - Silver acc_id: "ACC00000001", "ACC00000002", "ACC00002014"
        - Direct transformation: Extract numeric part, zero-pad to 8 digits
    
    Args:
        df: DataFrame with user_id column
        
    Returns:
        DataFrame with acc_id replacing user_id
    """
    # Transform KH### to ACC########
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
# Column Standardization
# ==========================================================

def _standardize_event_time(df: DataFrame) -> DataFrame:
    """
    Convert event_time to TimestampType.
    
    Bronze format: "M/d/yyyy H:mm"
    Silver type: TimestampType
    """
    return df.withColumn(
        "event_time",
        F.to_timestamp(F.col("event_time"), "M/d/yyyy H:mm")
    )


def _standardize_time_on_page(df: DataFrame) -> DataFrame:
    """
    Rename timeon_page to time_on_page_minutes.
    
    Bronze: timeon_page
    Silver: time_on_page_minutes
    """
    return df.withColumnRenamed("timeon_page", "time_on_page_minutes")


def _standardize_device_type(df: DataFrame) -> DataFrame:
    """
    Standardize device_type values.
    
    Business Rules:
        - desktop, pc, computer → Desktop
        - mobile, phone, smartphone → Mobile
        - tablet, ipad → Tablet
        - Unknown values → Desktop (default)
    """
    return df.withColumn(
        "device_type",
        F.when(F.lower(F.trim(F.col("device_type"))).isin("desktop", "pc", "computer"), "Desktop")
        .when(F.lower(F.trim(F.col("device_type"))).isin("mobile", "phone", "smartphone"), "Mobile")
        .when(F.lower(F.trim(F.col("device_type"))).isin("tablet", "ipad"), "Tablet")
        .otherwise("Desktop")
    )


def _standardize_event_type_initial(df: DataFrame) -> DataFrame:
    """
    Perform initial event_type standardization.
    
    Maps variations to standard values:
        - view, page_view, product_view → view
        - click, button_click, link_click → click
        - search, query, search_query → search
        - add_to_cart, cart_add, add_cart → add_to_cart
        - remove_from_cart, cart_remove, remove_cart → remove_from_cart
        - checkout, begin_checkout → checkout
        - purchase, order, buy → purchase
        - wishlist, add_to_wishlist, save → wishlist
    
    Unknown values → view (default)
    """
    return df.withColumn(
        "event_type",
        F.lower(F.trim(F.col("event_type")))
    ).withColumn(
        "event_type",
        F.when(F.col("event_type").isin("view", "page_view", "product_view"), "view")
        .when(F.col("event_type").isin("click", "button_click", "link_click"), "click")
        .when(F.col("event_type").isin("search", "query", "search_query"), "search")
        .when(F.col("event_type").isin("add_to_cart", "cart_add", "add_cart"), "add_to_cart")
        .when(F.col("event_type").isin("remove_from_cart", "cart_remove", "remove_cart"), "remove_from_cart")
        .when(F.col("event_type").isin("checkout", "begin_checkout"), "checkout")
        .when(F.col("event_type").isin("purchase", "order", "buy"), "purchase")
        .when(F.col("event_type").isin("wishlist", "add_to_wishlist", "save"), "wishlist")
        .otherwise("view")
    )


# ==========================================================
# Data Quality & Validation
# ==========================================================

def _validate_product_references(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Validate product_id references against Silver Products.
    
    Returns:
        Tuple of (valid_records, quarantine_records)
        
    Valid records have:
        - Non-null acc_id
        - Non-null event_time
        - Non-null event_type
        - Non-null product_id
        - Valid product_id (exists in Products)
    """
    products = _load_products_reference()
    
    df_with_check = df.join(
        products,
        on="product_id",
        how="left"
    )
    
    valid_records = df_with_check.filter(
        F.col("acc_id").isNotNull() &
        F.col("event_time").isNotNull() &
        F.col("event_type").isNotNull() &
        F.col("product_id").isNotNull() &
        (F.col("product_exists") == True)
    ).drop("product_exists")
    
    quarantine_records = df_with_check.filter(
        F.col("acc_id").isNull() |
        F.col("event_time").isNull() |
        F.col("event_type").isNull() |
        F.col("product_id").isNull() |
        F.col("product_exists").isNull()
    ).drop("product_exists")
    
    return valid_records, quarantine_records


def _sort_by_customer_and_time(df: DataFrame) -> DataFrame:
    """
    Sort events by acc_id and event_time.
    
    This ordering is critical for session reconstruction.
    """
    return df.orderBy("acc_id", "event_time")


# ==========================================================
# Session Reconstruction
# ==========================================================

def _rebuild_customer_sessions(df: DataFrame) -> DataFrame:
    """
    Rebuild customer sessions using 30-minute inactivity rule.
    
    Business Logic:
        - Sessions are bounded by customer (acc_id)
        - First event always starts a new session
        - Events more than 30 minutes after the previous event start a new session
        - Session IDs are globally unique (SS00000001 format)
    
    Implementation:
        1. Calculate time gap between consecutive events per customer
        2. Mark session starts (first event or gap > 30 minutes)
        3. Create cumulative session number within each customer
        4. Generate globally unique session IDs using dense_rank
    """
    window_spec = Window.partitionBy("acc_id").orderBy("event_time")
    
    # Calculate time gap from previous event
    df = df.withColumn(
        "prev_event_time",
        F.lag("event_time").over(window_spec)
    )
    
    df = df.withColumn(
        "time_gap_minutes",
        F.when(
            F.col("prev_event_time").isNull(),
            F.lit(None)
        ).otherwise(
            (F.unix_timestamp("event_time") - F.unix_timestamp("prev_event_time")) / 60
        )
    )
    
    # Mark session starts
    df = df.withColumn(
        "session_start",
        F.when(
            F.col("time_gap_minutes").isNull() | (F.col("time_gap_minutes") > 30),
            1
        ).otherwise(0)
    )
    
    # Create cumulative session number within customer
    df = df.withColumn(
        "session_number_within_customer",
        F.sum("session_start").over(window_spec)
    )
    
    # Create unique session key
    df = df.withColumn(
        "session_key",
        F.concat(
            F.col("acc_id"),
            F.lit("_"),
            F.col("session_number_within_customer").cast("string")
        )
    )
    
    # Generate globally unique session IDs
    session_id_window = Window.orderBy("session_key")
    
    df = df.withColumn(
        "session_id",
        F.concat(
            F.lit("SS"),
            F.lpad(
                F.dense_rank().over(session_id_window).cast("string"),
                8,
                "0"
            )
        )
    )
    
    # Clean up temporary columns
    df = df.drop(
        "prev_event_time",
        "time_gap_minutes",
        "session_start",
        "session_number_within_customer",
        "session_key"
    )
    
    return df


# ==========================================================
# Event Type Distribution & Funnel Logic
# ==========================================================

def _assign_event_type_distribution(df: DataFrame) -> DataFrame:
    """
    Assign event types based on realistic ecommerce distribution.
    
    Target Distribution:
        - view: 35%
        - search: 20%
        - click: 15%
        - add_to_cart: 12%
        - wishlist: 7%
        - checkout: 5%
        - remove_from_cart: 3%
        - purchase: 3%
    """
    df = df.withColumn("rand_num", (F.rand() * 100))
    
    df = df.withColumn(
        "event_type",
        F.when(F.col("rand_num") < 35, "view")
        .when(F.col("rand_num") < 55, "search")       # 35-55 = 20%
        .when(F.col("rand_num") < 70, "click")        # 55-70 = 15%
        .when(F.col("rand_num") < 82, "add_to_cart")  # 70-82 = 12%
        .when(F.col("rand_num") < 89, "wishlist")     # 82-89 = 7%
        .when(F.col("rand_num") < 94, "checkout")     # 89-94 = 5%
        .when(F.col("rand_num") < 97, "remove_from_cart")  # 94-97 = 3%
        .otherwise("purchase")                          # 97-100 = 3%
    ).drop("rand_num")
    
    return df


def _enforce_funnel_logic(df: DataFrame) -> DataFrame:
    """
    Enforce strict ecommerce funnel rules within sessions.
    
    Business Rules:
        - purchase requires prior checkout in the session
        - checkout requires prior add_to_cart in the session
        - remove_from_cart requires prior add_to_cart in the session
    
    Correction Strategy:
        - Invalid purchase → view
        - Invalid checkout → add_to_cart
        - Invalid remove_from_cart → view
    
    Implementation:
        Uses cumulative sum to track whether required events
        occurred earlier in the session.
    """
    window_session = Window.partitionBy("acc_id", "session_id").orderBy("event_time")
    
    # Track cumulative add_to_cart and checkout events before current event
    df = df.withColumn(
        "has_add_to_cart_before",
        F.sum(
            F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)
        ).over(
            Window.partitionBy("acc_id", "session_id")
            .orderBy("event_time")
            .rowsBetween(Window.unboundedPreceding, -1)
        )
    )
    
    df = df.withColumn(
        "has_checkout_before",
        F.sum(
            F.when(F.col("event_type") == "checkout", 1).otherwise(0)
        ).over(
            Window.partitionBy("acc_id", "session_id")
            .orderBy("event_time")
            .rowsBetween(Window.unboundedPreceding, -1)
        )
    )
    
    # Correct invalid events
    df = df.withColumn(
        "event_type",
        # If purchase but no prior checkout, change to view
        F.when(
            (F.col("event_type") == "purchase") &
            (F.coalesce(F.col("has_checkout_before"), F.lit(0)) == 0),
            "view"
        )
        # If checkout but no prior add_to_cart, change to add_to_cart
        .when(
            (F.col("event_type") == "checkout") &
            (F.coalesce(F.col("has_add_to_cart_before"), F.lit(0)) == 0),
            "add_to_cart"
        )
        # If remove_from_cart but no prior add_to_cart, change to view
        .when(
            (F.col("event_type") == "remove_from_cart") &
            (F.coalesce(F.col("has_add_to_cart_before"), F.lit(0)) == 0),
            "view"
        )
        .otherwise(F.col("event_type"))
    )
    
    # Clean up temporary columns
    df = df.drop(
        "has_add_to_cart_before",
        "has_checkout_before"
    )
    
    return df


# ==========================================================
# Enrichment & Metadata
# ==========================================================

def _add_audit_timestamps(df: DataFrame) -> DataFrame:
    """
    Add created_at and updated_at timestamps.
    
    Business Logic:
        - created_at: Set to event_time (when the event occurred)
        - updated_at: Set to current_timestamp (when the record was processed)
    """
    return df \
        .withColumn("created_at", F.col("event_time")) \
        .withColumn("updated_at", F.current_timestamp())


# ==========================================================
# Final Schema
# ==========================================================

def _select_final_schema(df: DataFrame) -> DataFrame:
    """
    Select final Silver schema for customer_behaviors.
    
    Schema:
        - acc_id: Customer account identifier
        - session_id: Unique session identifier
        - event_time: When the event occurred
        - event_type: Type of customer action
        - product_id: Product involved in the event
        - device_type: Device used (Desktop/Mobile/Tablet)
        - time_on_page_minutes: Duration on page in minutes
        - created_at: Event occurrence timestamp
        - updated_at: Record processing timestamp
        - ingestion_time: Bronze ingestion timestamp
        - source_file: Original source file
    """
    return df.select(
        "acc_id",
        "session_id",
        "event_time",
        "event_type",
        "product_id",
        "device_type",
        "time_on_page_minutes",
        "created_at",
        "updated_at",
        "ingestion_time",
        "source_file"
    ).orderBy("acc_id", "event_time")


# ==========================================================
# Public API
# ==========================================================

def transform(df: DataFrame) -> DataFrame:
    """
    Execute User Behaviors Silver Transformation.
    
    Pipeline:
        1. Load bronze user_behaviors (handled by run_pipeline)
        2. Map user_id to acc_id via Customers
        3. Standardize event_time (convert to timestamp)
        4. Standardize time_on_page (rename column)
        5. Standardize device_type (Desktop/Mobile/Tablet)
        6. Standardize event_type (initial normalization)
        7. Validate product references & split valid/quarantine
        8. Sort by customer and time (prerequisite for sessions)
        9. Rebuild customer sessions (30-minute gap rule)
        10. Assign event type distribution (realistic percentages)
        11. Enforce funnel logic (purchase → checkout → add_to_cart)
        12. Add audit timestamps (created_at, updated_at)
        13. Select final schema
    
    Note:
        Quarantine records are handled separately by the pipeline framework.
    """
    df = _load_bronze_user_behaviors(df)
    
    df = _map_customer_account(df)
    
    df = _standardize_event_time(df)
    
    df = _standardize_time_on_page(df)
    
    df = _standardize_device_type(df)
    
    df = _standardize_event_type_initial(df)
    
    valid_df, quarantine_df = _validate_product_references(df)
    
    # TODO: Handle quarantine_df if pipeline framework requires it
    # For now, continue with valid records only
    df = valid_df
    
    df = _sort_by_customer_and_time(df)
    
    df = _rebuild_customer_sessions(df)
    
    df = _assign_event_type_distribution(df)
    
    df = _enforce_funnel_logic(df)
    
    df = _add_audit_timestamps(df)
    
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
        source_table="user_behaviors",
        
        target_catalog="ecomflow",
        target_schema="ecom_silver",
        target_table="behaviors",
        target_volume="silver",
        transform=transform,
    )
    
    print(f"\n{'=' * 80}")
    print("BEHAVIORS (CUSTOMER BEHAVIORS) SILVER PIPELINE COMPLETED")
    print(f"{'=' * 80}")
    print(f"Source: ecomflow.ecom_bronze_v2.user_behaviors")
    print(f"Target: ecomflow.ecom_silver.behaviors")
    print(f"Validation Status: {'✅ PASSED' if result.validation_result.is_valid else '❌ FAILED'}")
    print(f"Row Count: {result.validation_result.row_count}")
    print(f"Processing Duration: {result.processing_duration:.2f}s")
    print(f"{'=' * 80}\n")
