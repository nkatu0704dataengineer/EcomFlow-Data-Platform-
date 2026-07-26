"""
Silver Order Items Notebook

Business Logic for Order Items Dataset

Layer:
    Silver

Dataset:
    order_items
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

def _refactor_schema(df: DataFrame) -> DataFrame:
    """
    Rename Bronze columns to Silver schema.

    Drop obsolete Bronze price columns (will be recalculated).
    Rename product_quantity to quantity.
    """
    return df \
        .drop("product_listprice", "product_saleprice", "product_total_price") \
        .withColumnRenamed("product_quantity", "quantity")


# ==========================================================
# ID Generation
# ==========================================================

def _generate_order_item_id(df: DataFrame) -> DataFrame:
    """
    Generate order_item_id as sequence within each order.

    Uses Window function partitioned by order_id.
    Returns sequential integer starting from 1 per order.
    """
    order_item_window = Window.partitionBy("order_id").orderBy(F.monotonically_increasing_id())

    return df.withColumn(
        "order_item_id",
        F.row_number().over(order_item_window)
    )


# ==========================================================
# Business Assignment
# ==========================================================

def _assign_subsidiary(df: DataFrame) -> DataFrame:
    """
    Assign subsidiary_id randomly from subsidiaries table.

    Each order item is assigned to a random subsidiary.
    Uses element_at with random index selection.
    """
    subsidiaries_df = spark.table("ecomflow.ecom_silver.subsidiaries").select("sub_id")
    subsidiaries_list = [row.sub_id for row in subsidiaries_df.collect()]

    return df.withColumn(
        "subsidiary_id",
        F.element_at(
            F.array([F.lit(sub_id) for sub_id in subsidiaries_list]),
            (F.floor(F.rand() * len(subsidiaries_list)) + 1).cast("int")
        )
    )


def _assign_sale_program(df: DataFrame) -> DataFrame:
    """
    Assign sale_id from program_sales with realistic distribution.

    Business Logic:
    - 70% of items have a sale program
    - 30% of items have NULL (no discount)

    Returns sale_id and discount_rate.
    """
    # Load program sales
    program_sales_df = spark.table("ecomflow.ecom_bronze_v2.program_sales").select(
        "sale_id",
        "discount_value"
    )

    # Collect sale programs into list
    sale_programs = [row.sale_id for row in program_sales_df.select("sale_id").collect()]

    # Assign sale_id (single column operation before join)
    df = df.withColumn(
        "sale_id",
        F.when(
            F.rand() < 0.70,  # 70% get a sale program
            F.element_at(
                F.array([F.lit(sale_id) for sale_id in sale_programs]),
                (F.floor(F.rand() * len(sale_programs)) + 1).cast("int")
            )
        ).otherwise(F.lit(None).cast("string"))
    )

    # Join to get discount_value
    df = df.join(
        program_sales_df,
        on="sale_id",
        how="left"
    )

    # Generate discount_rate and drop discount_value in one operation
    return df.withColumn(
        "discount_rate",
        F.when(
            F.col("discount_value").isNotNull(),
            F.round(F.col("discount_value"), 2)
        ).otherwise(F.lit(0.0))
    ).drop("discount_value")


# ==========================================================
# Location Intelligence
# ==========================================================

def _join_subsidiary_location(df: DataFrame) -> DataFrame:
    """
    Join with subsidiaries to get branch location.

    Retrieves branch_city and branch_district for shipping distance calculation.
    """
    subsidiaries = spark.table("ecomflow.ecom_silver.subsidiaries").select(
        F.col("sub_id").alias("subsidiary_id"),
        F.col("sub_district").alias("branch_district"),
        F.col("sub_city").alias("branch_city")
    )

    return df.join(
        subsidiaries,
        on="subsidiary_id",
        how="left"
    )


def _join_customer_location(df: DataFrame) -> DataFrame:
    """
    Join with orders to get customer shipping location.

    Retrieves customer_city and customer_district for shipping distance calculation.
    """
    orders = spark.table("ecomflow.ecom_silver.orders").select(
        "order_id",
        F.col("target_district").alias("customer_district"),
        F.col("target_city").alias("customer_city")
    )

    return df.join(
        orders,
        on="order_id",
        how="left"
    )


# ==========================================================
# Shipping Calculation
# ==========================================================

def _calculate_shipping_distance(df: DataFrame) -> DataFrame:
    """
    Calculate shipping distance based on location matching.

    Business Rules:
    - Same city AND same district: 3-20 km
    - Same city, different district: 10-35 km
    - Different cities: 50-1200 km

    Returns: DECIMAL(18, 2) in kilometers
    """
    return df.withColumn(
        "shipping_distance_km",
        F.when(
            # Same city AND same district
            (F.col("branch_city") == F.col("customer_city")) & 
            (F.col("branch_district") == F.col("customer_district")),
            F.round((F.rand() * 17 + 3), 2)  # Random 3-20 km
        ).when(
            # Same city, different district
            F.col("branch_city") == F.col("customer_city"),
            F.round((F.rand() * 25 + 10), 2)  # Random 10-35 km
        ).otherwise(
            # Different cities
            F.round((F.rand() * 1150 + 50), 2)  # Random 50-1200 km
        )
    )


def _calculate_shipping_fee(df: DataFrame) -> DataFrame:
    """
    Calculate item-level shipping fee.

    Viettel Post / Shopee Logistics Pricing Model:
    Formula: 15000 + (weight/1000)*4000 + (volume/10000)*2500 + distance*300

    Components:
    - Base fee: 15,000 VND
    - Weight fee: 4,000 VND per kg
    - Volume fee: 2,500 VND per 10,000 cm³
    - Distance fee: 300 VND per km

    Minimum: 15,000 VND
    Returns: DECIMAL(18, 2) in VND
    """
    # Calculate raw shipping fee
    df = df.withColumn(
        "shipping_fee_calculated",
        F.lit(15000) +
        (F.coalesce(F.col("product_weight_g"), F.lit(0)) / 1000) * 4000 +
        (F.coalesce(F.col("product_volume_cm3"), F.lit(0)) / 10000) * 2500 +
        F.col("shipping_distance_km") * 300
    )

    # Apply minimum fee and round
    df = df.withColumn(
        "shipping_fee",
        F.round(
            F.greatest(
                F.col("shipping_fee_calculated"),
                F.lit(15000)
            ),
            2
        )
    )

    return df.drop("shipping_fee_calculated")


def _cleanup_location_columns(df: DataFrame) -> DataFrame:
    """
    Drop temporary location columns used for shipping calculation.

    Removes branch_city, branch_district, customer_city, customer_district.
    Also removes product_weight_g and product_volume_cm3 after shipping fee calculation.
    These are intermediate columns not needed in final Silver schema.
    """
    return df.drop(
        "branch_city",
        "branch_district",
        "customer_city",
        "customer_district",
        "product_weight_g",
        "product_volume_cm3"
    )


# ==========================================================
# Pricing Calculation
# ==========================================================

def _calculate_list_price(df: DataFrame) -> DataFrame:
    """
    Get list_price from products table.

    Uses the latest version for each product.
    Also retrieves product_weight_g and product_volume_cm3 for calculations.

    Returns DECIMAL(18, 2).
    """
    # Get the latest version for each product
    products_window = Window.partitionBy("product_id").orderBy(F.col("product_version").desc())

    products_df = spark.table("ecomflow.ecom_silver.products") \
        .withColumn("rn", F.row_number().over(products_window)) \
        .filter(F.col("rn") == 1) \
        .select(
            "product_id",
            F.col("product_listprice").alias("list_price"),
            "product_weight_g",
            "product_volume_cm3"
        )

    df = df.join(
        products_df,
        on="product_id",
        how="left"
    )

    # Convert list_price to DECIMAL(18,2)
    return df.withColumn("list_price", F.col("list_price").cast(DecimalType(18, 2)))


def _calculate_pricing_and_weight(df: DataFrame) -> DataFrame:
    """
    Calculate sale_price, total_price, and item_total_weight_g in one operation.
    
    OPTIMIZED: Uses .withColumns() to add multiple columns at once,
    reducing execution plan depth and improving Spark Connect performance.
    
    Formulas:
    - sale_price: list_price × (1 - discount_rate)
    - total_price: quantity × sale_price
    - item_total_weight_g: quantity × product_weight_g
    
    Returns: DECIMAL(18, 2) for all calculated fields
    """
    return df.withColumns({
        # Sale price after discount
        "sale_price": F.round(
            F.col("list_price") * (F.lit(1.0) - F.col("discount_rate")),
            2
        ).cast(DecimalType(18, 2)),
        
        # Total price for the item
        "total_price": F.round(
            F.col("quantity") * F.round(
                F.col("list_price") * (F.lit(1.0) - F.col("discount_rate")),
                2
            ),
            2
        ).cast(DecimalType(18, 2)),
        
        # Item total weight
        "item_total_weight_g": F.round(
            F.col("quantity") * F.col("product_weight_g"),
            2
        ).cast(DecimalType(18, 2))
    })


# ==========================================================
# Timestamps
# ==========================================================

def _join_order_timestamp(df: DataFrame) -> DataFrame:
    """
    Join with orders table to get created_at.

    Order items inherit the order creation timestamp.
    """
    orders_df = spark.table("ecomflow.ecom_silver.orders").select(
        "order_id",
        "created_at"
    )

    return df.join(
        orders_df,
        on="order_id",
        how="left"
    )


def _generate_updated_at(df: DataFrame) -> DataFrame:
    """
    Generate updated_at timestamp.

    Business Logic:
    - Based on created_at
    - Random offset: 0 to 180 days after creation

    Returns: TimestampType
    """
    return df.withColumn(
        "updated_at",
        (
            F.col("created_at").cast("long")
            + (F.rand() * 180).cast("int") * 86400
        ).cast("timestamp")
    )


# ==========================================================
# Final Schema
# ==========================================================

def _select_final_schema(df: DataFrame) -> DataFrame:
    """
    Select final Silver schema.

    Column order matches business requirements.
    """
    return df.select(
        # IDs
        "order_id",
        "order_item_id",
        "product_id",
        "subsidiary_id",
        "sale_id",

        # Quantities
        "quantity",

        # Pricing
        "list_price",
        "discount_rate",
        "sale_price",
        "total_price",

        # Weight
        "item_total_weight_g",

        # Shipping
        "shipping_distance_km",
        "shipping_fee",

        # Audit
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
    Execute Order Items Silver Transformation.

    Pipeline:
    1. Refactor schema (drop old prices, rename quantity)
    2. Generate order_item_id (sequence within order)
    3. Assign subsidiary_id (random from subsidiaries)
    4. Assign sale_id and discount_rate (70% assigned, 30% NULL)
    5. Calculate list_price from products (latest version, includes volume)
    6. Calculate sale_price, total_price, item_weight (BATCHED for performance)
    7. Join subsidiary location (branch_city, branch_district)
    8. Join customer location (customer_city, customer_district)
    9. Calculate shipping_distance_km (based on location matching)
    10. Calculate shipping_fee (Viettel Post pricing model)
    11. Cleanup temporary location columns
    12. Join order created_at timestamp
    13. Generate updated_at timestamp
    14. Select final schema
    """
    df = _refactor_schema(df)

    df = _generate_order_item_id(df)

    df = _assign_subsidiary(df)

    df = _assign_sale_program(df)

    df = _calculate_list_price(df)

    # OPTIMIZED: Batch pricing and weight calculations
    df = _calculate_pricing_and_weight(df)

    df = _join_subsidiary_location(df)

    df = _join_customer_location(df)

    df = _calculate_shipping_distance(df)

    df = _calculate_shipping_fee(df)

    df = _cleanup_location_columns(df)

    df = _join_order_timestamp(df)

    df = _generate_updated_at(df)

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
        source_table="order_items",

        target_catalog="ecomflow",
        target_schema="ecom_silver",
        target_table="order_items",
        target_volume="silver",
        transform=transform,
    )

    print(f"\n{'=' * 80}")
    print("ORDER ITEMS SILVER PIPELINE COMPLETED")
    print(f"{'=' * 80}")
    print(f"Source: ecomflow.ecom_bronze_v2.order_items")
    print(f"Target: ecomflow.ecom_silver.order_items")
    print(f"Validation Status: {'✅ PASSED' if result.validation_result.is_valid else '❌ FAILED'}")
    print(f"Row Count: {result.validation_result.row_count}")
    print(f"Processing Duration: {result.processing_duration:.2f}s")
    print(f"{'=' * 80}\n")
