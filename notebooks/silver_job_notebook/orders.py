"""
Silver Orders Notebook

Business Logic for Orders Dataset

Layer:
    Silver

Dataset:
    orders
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

    Drop unused columns.

    Changes:
    - estimated_delivery_at → estimated_delivery_date
    - Drop: total_order (will be calculated from order_items), payment_id, shipping_address
    """
    return df \
        .withColumnRenamed("estimated_delivery_at", "estimated_delivery_date") \
        .drop("total_order", "payment_id", "shipping_address")


# ==========================================================
# Status Derivation & Standardization
# ==========================================================

def _derive_order_status(df: DataFrame) -> DataFrame:
    """
    Derive order_status from timestamp logic.

    Business Rules (timestamp-based):
    - DELIVERED: delivered_at IS NOT NULL
    - SHIPPED: confirmed_at IS NOT NULL AND delivered_at IS NULL
    - CONFIRMED: confirmed_at IS NOT NULL (fallback if above conditions don't match)
    - PENDING: Only created_at exists

    This replaces Bronze status values with derived status based on actual timeline.
    Ensures status always matches the order's actual lifecycle stage.

    Note: CANCELLED and RETURNED statuses are not derived here as Bronze data
    does not contain the necessary fields (cancel_date, return_date).
    """
    return df.withColumn(
        "order_status",
        F.when(F.col("delivered_at").isNotNull(), "DELIVERED")
         .when(
             (F.col("confirmed_at").isNotNull()) & 
             (F.col("delivered_at").isNull()),
             "SHIPPED"
         )
         .when(F.col("confirmed_at").isNotNull(), "CONFIRMED")
         .otherwise("PENDING")
    )


def _convert_timestamps(df: DataFrame) -> DataFrame:
    """
    Convert timestamp columns from Bronze format to TimestampType.

    Bronze format: "M/d/yyyy H:mm"

    Columns:
    - created_at
    - confirmed_at
    - estimated_delivery_date
    - delivered_at
    - updated_at
    """
    return df \
        .withColumn("created_at", F.to_timestamp("created_at", "M/d/yyyy H:mm")) \
        .withColumn("confirmed_at", F.to_timestamp("confirmed_at", "M/d/yyyy H:mm")) \
        .withColumn("estimated_delivery_date", F.to_timestamp("estimated_delivery_date", "M/d/yyyy H:mm")) \
        .withColumn("delivered_at", F.to_timestamp("delivered_at", "M/d/yyyy H:mm")) \
        .withColumn("updated_at", F.to_timestamp("updated_at", "M/d/yyyy H:mm"))


# ==========================================================
# Order Totals Aggregation
# ==========================================================

def _aggregate_order_totals(df: DataFrame, spark_session) -> DataFrame:
    """
    Aggregate total_amount and total_shipfee from order_items volume.

    Business Logic:
    - total_amount = SUM(order_items.total_price)
    - total_shipfee = SUM(order_items.shipping_fee)
    - Both aggregated in single operation for consistency
    - Read directly from volume to ensure latest data
    - Orders with no items get 0.0 for both

    This replaces Bronze total_order with actual calculated totals.
    """
    # Read order_items from volume (latest data)
    order_items_path = "/Volumes/ecomflow/ecom_silver/silver/order_items/"
    order_items = spark_session.read.format("delta").load(order_items_path)

    # Aggregate both totals in one operation
    order_totals = order_items.groupBy("order_id").agg(
        F.round(F.sum("total_price"), 2).cast(DecimalType(18, 2)).alias("total_amount"),
        F.round(F.sum("shipping_fee"), 2).alias("total_shipfee")
    )

    # Left join to preserve all orders
    df = df.join(
        order_totals,
        on="order_id",
        how="left"
    )

    # Fill null values with 0 for orders with no items
    return df.fillna({"total_amount": 0.0, "total_shipfee": 0.0})


# ==========================================================
# Shipping Address
# ==========================================================

def _generate_shipping_address(df: DataFrame) -> DataFrame:
    """
    Generate realistic target addresses.

    Creates:
    - target_street
    - target_district
    - target_city

    Uses master address list covering:
    - Hà Nội (Ba Đình, Đống Đa, Hoàn Kiếm, Cầu Giấy, Thanh Xuân)
    - Đà Nẵng (Hải Châu, Thanh Khê, Liên Chiểu, Sơn Trà, Ngũ Hành Sơn)
    - TP.HCM (Quận 1, 3, 5, Bình Thạnh, Gò Vấp)

    Addresses are randomly but evenly distributed across orders.
    """
    # Master address list
    shipping_addresses = [
        # HÀ NỘI
        ("125 Kim Mã", "Ba Đình", "Hà Nội"),
        ("82 Nguyễn Chí Thanh", "Ba Đình", "Hà Nội"),
        ("98 Tây Sơn", "Đống Đa", "Hà Nội"),
        ("45 Chùa Bộc", "Đống Đa", "Hà Nội"),
        ("15 Đinh Tiên Hoàng", "Hoàn Kiếm", "Hà Nội"),
        ("62 Hàng Đào", "Hoàn Kiếm", "Hà Nội"),
        ("188 Xuân Thủy", "Cầu Giấy", "Hà Nội"),
        ("66 Trần Duy Hưng", "Cầu Giấy", "Hà Nội"),
        ("250 Nguyễn Trãi", "Thanh Xuân", "Hà Nội"),
        ("88 Lê Văn Lương", "Thanh Xuân", "Hà Nội"),
        
        # ĐÀ NẴNG
        ("22 Bạch Đằng", "Hải Châu", "Đà Nẵng"),
        ("105 Hùng Vương", "Hải Châu", "Đà Nẵng"),
        ("89 Nguyễn Văn Linh", "Thanh Khê", "Đà Nẵng"),
        ("300 Điện Biên Phủ", "Thanh Khê", "Đà Nẵng"),
        ("510 Tôn Đức Thắng", "Liên Chiểu", "Đà Nẵng"),
        ("12 Võ Nguyên Giáp", "Sơn Trà", "Đà Nẵng"),
        ("99 Lê Văn Hiến", "Ngũ Hành Sơn", "Đà Nẵng"),
        
        # TP.HCM
        ("25 Nguyễn Huệ", "Quận 1", "TP.HCM"),
        ("88 Đồng Khởi", "Quận 1", "TP.HCM"),
        ("199 Nguyễn Thị Minh Khai", "Quận 3", "TP.HCM"),
        ("455 Cách Mạng Tháng Tám", "Quận 3", "TP.HCM"),
        ("550 Điện Biên Phủ", "Bình Thạnh", "TP.HCM"),
        ("302 Xô Viết Nghệ Tĩnh", "Bình Thạnh", "TP.HCM"),
        ("190 Quang Trung", "Gò Vấp", "TP.HCM"),
        ("355 Phan Văn Trị", "Gò Vấp", "TP.HCM"),
        ("188 Trần Hưng Đạo", "Quận 5", "TP.HCM"),
        ("320 Nguyễn Trãi", "Quận 5", "TP.HCM")
    ]

    # Create target address DataFrame
    shipping_df = spark.createDataFrame(
        shipping_addresses,
        ["target_street", "target_district", "target_city"]
    )

    # Randomize and assign unique IDs
    shipping_window = Window.orderBy(F.monotonically_increasing_id())
    shipping_df = shipping_df \
        .orderBy(F.rand()) \
        .withColumn("addr_id", F.row_number().over(shipping_window))

    # Assign address IDs to orders (cycling through addresses)
    order_window = Window.orderBy(F.monotonically_increasing_id())
    df = df.withColumn(
        "addr_id",
        ((F.row_number().over(order_window) - 1) % len(shipping_addresses)) + 1
    )

    # Join shipping addresses
    df = df.join(shipping_df, on="addr_id", how="left")

    # Drop temporary column
    return df.drop("addr_id")


# ==========================================================
# Customer Mapping
# ==========================================================

def _map_customer_account(df: DataFrame) -> DataFrame:
    """
    Map Bronze user_id to Silver acc_id.

    Business Logic:
    - Bronze orders.user_id contains values like "KH001", "KH002"
    - Bronze users table provides the mapping source
    - Silver customers table provides acc_id (ordered by cus_id)
    - Both tables have 2402 rows in corresponding order
    - Create lookup: user_id → acc_id via row_number join
    - Replace orders.user_id with acc_id from customers

    Inner join ensures only valid customer mappings.
    """
    # Load Bronze users and create row numbers
    bronze_users = spark.table("ecomflow.ecom_bronze_v2.users").select(
        F.trim(F.col("user_id")).alias("user_id")
    ).orderBy("user_id").withColumn(
        "row_num",
        F.row_number().over(Window.orderBy("user_id"))
    )

    # Load Silver customers and create row numbers
    silver_customers = spark.table("ecomflow.ecom_silver.customers").select(
        F.col("cus_id"),
        F.col("acc_id")
    ).orderBy("cus_id").withColumn(
        "row_num",
        F.row_number().over(Window.orderBy("cus_id"))
    ).drop("cus_id")

    # Join by row number to create user_id → acc_id mapping
    user_to_acc_mapping = bronze_users.join(
        silver_customers,
        on="row_num",
        how="inner"
    ).drop("row_num")

    # Trim user_id in orders to match Bronze users format
    df = df.withColumn("user_id", F.trim(F.col("user_id")))

    # Join orders with mapping and replace user_id with acc_id
    return df.join(
        user_to_acc_mapping,
        on="user_id",
        how="inner"
    ).drop("user_id")


# ==========================================================
# Data Quality
# ==========================================================

def _filter_valid_orders(df: DataFrame) -> DataFrame:
    """
    Filter out invalid orders.

    Business Rule:
    - Orders with total_amount = 0 and total_shipfee = 0 are invalid
    - These orders have no items in order_items table
    - Only keep orders with actual items

    This ensures orders table only contains valid, complete orders.
    """
    return df.filter(
        (F.col("total_amount") > 0) | (F.col("total_shipfee") > 0)
    )


# ==========================================================
# Order Timeline
# ==========================================================

def _generate_order_timeline(df: DataFrame) -> DataFrame:
    """
    Generate order lifecycle timestamps.

    shipped_at:
    - Only for SHIPPED and DELIVERED orders
    - Between confirmed_at and 30% toward estimated_delivery_date
    - NULL for other statuses

    cancel_date:
    - Only for CANCELLED orders
    - Within 3 days (259,200 seconds) after created_at
    - NULL for other statuses
    """
    # Generate shipped_at
    df = df.withColumn(
        "shipped_at",
        F.when(
            F.col("order_status").isin(["SHIPPED", "DELIVERED"]),
            (
                F.col("confirmed_at").cast("long") +
                (F.rand() * 
                 (F.col("estimated_delivery_date").cast("long") - F.col("confirmed_at").cast("long")) * 0.3
                ).cast("long")
            ).cast("timestamp")
        ).otherwise(F.lit(None).cast("timestamp"))
    )

    # Generate cancel_date
    df = df.withColumn(
        "cancel_date",
        F.when(
            F.col("order_status") == "CANCELLED",
            (
                F.col("created_at").cast("long") +
                (F.rand() * 86400 * 3).cast("long")
            ).cast("timestamp")
        ).otherwise(F.lit(None).cast("timestamp"))
    )

    return df





# ==========================================================
# Final Schema
# ==========================================================

def _select_final_schema(df: DataFrame) -> DataFrame:
    """
    Select final Silver schema.

    Column order matches production requirements.
    """
    return df.select(
        # IDs
        "order_id",
        "acc_id",
        "employee_id",

        # Order Info
        "order_status",
        "total_amount",
        "total_shipfee",

        # Target Address
        "target_street",
        "target_district",
        "target_city",

        # Timestamps
        "created_at",
        "confirmed_at",
        "shipped_at",
        "estimated_delivery_date",
        "delivered_at",
        "cancel_date",
        "updated_at",

        # Audit
        "ingestion_time",
        "source_file"
    )


# ==========================================================
# Public API
# ==========================================================

def transform(df: DataFrame) -> DataFrame:
    """
    Execute Orders Silver Transformation.

    Pipeline:
    1. Refactor schema (rename columns, drop unused including total_order)
    2. Convert timestamps (Bronze format → TimestampType)
    3. Derive order_status (timestamp-based logic, not from Bronze)
    4. Aggregate order totals (total_amount & total_shipfee from order_items volume)
    5. Filter valid orders (remove orders with no items)
    6. Generate shipping address (realistic Vietnamese addresses)
    7. Map customer account (user_id → acc_id)
    8. Generate order timeline (shipped_at, cancel_date)
    9. Select final schema

    Key Business Rules:
    - order_status: Derived from timestamps (DELIVERED if delivered_at exists, SHIPPED if only confirmed_at, etc.)
    - total_amount: SUM(order_items.total_price) from volume
    - total_shipfee: SUM(order_items.shipping_fee) from volume
    - Data Quality: Filter out orders with total_amount = 0 AND total_shipfee = 0 (no items)
    - shipped_at: Only for SHIPPED/DELIVERED orders
    - cancel_date: Only for CANCELLED orders
    - acc_id: Mapped from customers table via row_number join
    """
    df = _refactor_schema(df)

    df = _convert_timestamps(df)

    df = _derive_order_status(df)

    df = _aggregate_order_totals(df, df.sparkSession)

    df = _filter_valid_orders(df)

    df = _generate_shipping_address(df)

    df = _map_customer_account(df)

    df = _generate_order_timeline(df)

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
        source_table="orders",

        target_catalog="ecomflow",
        target_schema="ecom_silver",
        target_table="orders",
        target_volume="silver",
        transform=transform,
    )

    print(f"\n{'=' * 80}")
    print("ORDERS SILVER PIPELINE COMPLETED")
    print(f"{'=' * 80}")
    print(f"Source: ecomflow.ecom_bronze_v2.orders")
    print(f"Target: ecomflow.ecom_silver.orders")
    print(f"Validation Status: {'✅ PASSED' if result.validation_result.is_valid else '❌ FAILED'}")
    print(f"Row Count: {result.validation_result.row_count}")
    print(f"Processing Duration: {result.processing_duration:.2f}s")
    print(f"{'=' * 80}\n")
