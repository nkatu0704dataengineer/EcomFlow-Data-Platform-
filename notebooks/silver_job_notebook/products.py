"""
Silver Products Notebook

Business Logic for Products Dataset

Layer:
    Silver

Dataset:
    products
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
# Schema Refactoring
# ==========================================================

def _refactor_schema(df: DataFrame) -> DataFrame:
    """
    Rename Bronze columns to Silver schema.

    Drop unused columns.
    """
    return df \
        .withColumnRenamed("prcs_id", "category_id") \
        .drop("product_specs")


# ==========================================================
# Product Information
# ==========================================================

def _normalize_product_name(df: DataFrame) -> DataFrame:
    """
    Standardize product name format.

    Bronze: product1, product15, product123
    Silver: Product 01, Product 15, Product 123

    Extract numeric suffix and pad with leading zeros.
    """
    return df.withColumn(
        "product_name",
        F.concat(
            F.lit("Product "),
            F.lpad(
                F.regexp_extract(F.col("product_name"), r"product(\d+)", 1),
                2,
                "0"
            )
        )
    )


def _generate_product_version(df: DataFrame) -> DataFrame:
    """
    Generate realistic product version.

    Bronze: vs2020
    Silver: Version 2020

    Random version years: 2018 - 2026
    """
    return df.withColumn(
        "product_version",
        F.concat(
            F.lit("Version "),
            (F.floor(F.rand() * 9) + 2018).cast("string")
        )
    )


def _generate_product_description(df: DataFrame) -> DataFrame:
    """
    Generate realistic product description.

    Uses multiple templates for variety:
    - Premium {product} with excellent features
    - Designed for professional use
    - Built for durability and performance
    - High-quality {product} at affordable price
    - Ideal choice for everyday needs
    """
    # Create random template selector (0-4)
    df = df.withColumn("_template_idx", (F.rand() * 5).cast("int"))

    # Generate description based on template
    df = df.withColumn(
        "product_description",
        F.when(
            F.col("_template_idx") == 0,
            F.concat(
                F.lit("Premium "),
                F.lower(F.col("product_name")),
                F.lit(" with excellent features and modern design")
            )
        ).when(
            F.col("_template_idx") == 1,
            F.concat(
                F.lit("Designed for professional use. "),
                F.initcap(F.col("product_name")),
                F.lit(" delivers outstanding performance")
            )
        ).when(
            F.col("_template_idx") == 2,
            F.concat(
                F.lit("Built for durability and long-lasting performance. "),
                F.initcap(F.col("product_name")),
                F.lit(" exceeds industry standards")
            )
        ).when(
            F.col("_template_idx") == 3,
            F.concat(
                F.lit("High-quality "),
                F.lower(F.col("product_name")),
                F.lit(" at an affordable price point")
            )
        ).otherwise(
            F.concat(
                F.lit("Ideal choice for everyday needs. "),
                F.initcap(F.col("product_name")),
                F.lit(" combines quality and value")
            )
        )
    )

    return df.drop("_template_idx")


# ==========================================================
# Inventory Intelligence
# ==========================================================

def _calculate_product_status(df: DataFrame) -> DataFrame:
    """
    Calculate product status from SubProducts inventory.

    Business Logic:
    - ACTIVE: Total inventory (QUANTITY) > 0
    - INACTIVE: No inventory or not found in SubProducts

    Replaces Bronze fake random status with real inventory-based logic.
    """
    # Load and aggregate SubProducts inventory
    sub_products = spark.table("ecomflow.ecom_bronze_v2.sub_products")

    inventory_agg = sub_products.groupBy("PRODUCT_ID").agg(
        F.sum("QUANTITY").alias("total_inventory")
    )

    # Rename to match product_id for join
    inventory_agg = inventory_agg.withColumnRenamed("PRODUCT_ID", "product_id")

    # Join with products
    df = df.join(
        inventory_agg,
        on="product_id",
        how="left"
    )

    # Determine status based on inventory
    df = df.withColumn(
        "product_status",
        F.when(
            (F.col("total_inventory").isNotNull()) & (F.col("total_inventory") > 0),
            F.lit("ACTIVE")
        ).otherwise(F.lit("INACTIVE"))
    )

    return df.drop("total_inventory")


# ==========================================================
# Product Specifications
# ==========================================================

def _generate_product_dimensions(df: DataFrame) -> DataFrame:
    """
    Generate realistic product dimensions.

    Ranges:
    - weight: 100g - 4600g
    - length: 10cm - 90cm
    - height: 5cm - 65cm
    - width: 8cm - 78cm

    All values are FLOAT.
    """
    return df \
        .withColumn(
            "product_weight_g",
            (F.rand() * 4500 + 100).cast("float")
        ) \
        .withColumn(
            "product_length_cm",
            (F.rand() * 80 + 10).cast("float")
        ) \
        .withColumn(
            "product_height_cm",
            (F.rand() * 60 + 5).cast("float")
        ) \
        .withColumn(
            "product_width_cm",
            (F.rand() * 70 + 8).cast("float")
        )


def _calculate_product_volume(df: DataFrame) -> DataFrame:
    """
    Calculate product volume.

    Formula: length × height × width

    Returns: DECIMAL(18, 2)
    """
    return df.withColumn(
        "product_volume_cm3",
        (
            F.col("product_length_cm") *
            F.col("product_height_cm") *
            F.col("product_width_cm")
        ).cast(DecimalType(18, 2))
    )


def _calculate_product_density(df: DataFrame) -> DataFrame:
    """
    Calculate product density.

    Formula: weight / volume

    Returns: DECIMAL(18, 4)
    Handles division by zero (returns 0).
    """
    return df.withColumn(
        "product_density_g_cm3",
        F.when(
            F.col("product_volume_cm3") > 0,
            (F.col("product_weight_g") / F.col("product_volume_cm3")).cast(DecimalType(18, 4))
        ).otherwise(F.lit(0).cast(DecimalType(18, 4)))
    )


# ==========================================================
# Media
# ==========================================================

def _generate_product_media(df: DataFrame) -> DataFrame:
    """
    Generate product media URLs and photo count.

    product_photos_link:
        https://cdn.ecomstore.com/products/{product_id}/{version}.jpg

    product_photos_qty:
        Random count between 3 and 10
    """
    return df \
        .withColumn(
            "product_photos_link",
            F.concat(
                F.lit("https://cdn.ecomstore.com/products/"),
                F.col("product_id"),
                F.lit("/"),
                F.col("product_version"),
                F.lit(".jpg")
            )
        ) \
        .withColumn(
            "product_photos_qty",
            (F.floor(F.rand() * 8) + 3).cast("int")
        )


# ==========================================================
# Standardization
# ==========================================================

def _normalize_strings(df: DataFrame) -> DataFrame:
    """
    Normalize string columns.

    - product_name: Initcap, trim
    - product_status: Upper, trim
    """
    return df \
        .withColumn(
            "product_name",
            F.initcap(F.trim(F.col("product_name")))
        ) \
        .withColumn(
            "product_status",
            F.upper(F.trim(F.col("product_status")))
        )


def _convert_timestamps(df: DataFrame) -> DataFrame:
    """
    Convert timestamp columns.

    Bronze format: "M/d/yyyy H:mm"
    Silver type: TimestampType
    """
    return df \
        .withColumn(
            "created_at",
            F.to_timestamp("created_at", "M/d/yyyy H:mm")
        ) \
        .withColumn(
            "updated_at",
            F.to_timestamp("updated_at", "M/d/yyyy H:mm")
        )


# ==========================================================
# Final Schema
# ==========================================================

def _select_final_schema(df: DataFrame) -> DataFrame:
    """
    Select final Silver schema.

    Removes obsolete columns:
    - product_name_length
    - product_description_length

    These metrics no longer belong in Silver layer.
    """
    return df.select(
        # IDs
        "product_id",
        "product_version",
        "brand_id",
        "category_id",

        # Product Info
        "product_name",
        "product_description",
        "product_status",
        "product_listprice",

        # Dimensions
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "product_volume_cm3",
        "product_density_g_cm3",

        # Media
        "product_photos_link",
        "product_photos_qty",

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
    Execute Products Silver Transformation.

    Pipeline:
    1. Refactor schema (rename columns)
    2. Normalize product name (Product 01 format)
    3. Generate product version (Version 2018-2026)
    4. Generate product description (multiple templates)
    5. Calculate product status from SubProducts inventory
    6. Generate product dimensions (weight, length, height, width)
    7. Calculate product volume
    8. Calculate product density
    9. Generate product media (photos link, qty)
    10. Normalize strings
    11. Convert timestamps
    12. Select final schema
    
    NOTE:
    - Product rating and review count are now in separate table: product_scores
    - This eliminates circular dependency with reviews table
    """
    df = _refactor_schema(df)

    df = _normalize_product_name(df)

    df = _generate_product_version(df)

    df = _generate_product_description(df)

    df = _calculate_product_status(df)

    df = _generate_product_dimensions(df)

    df = _calculate_product_volume(df)

    df = _calculate_product_density(df)

    df = _generate_product_media(df)

    df = _normalize_strings(df)

    df = _convert_timestamps(df)

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
        source_table="products",

        target_catalog="ecomflow",
        target_schema="ecom_silver",
        target_table="products",
        target_volume="silver",
        transform=transform,
    )

    print(f"\n{'=' * 80}")
    print("PRODUCTS SILVER PIPELINE COMPLETED")
    print(f"{'=' * 80}")
    print(f"Source: ecomflow.ecom_bronze_v2.products")
    print(f"Target: ecomflow.ecom_silver.products")
    print(f"Validation Status: {'✅ PASSED' if result.validation_result.is_valid else '❌ FAILED'}")
    print(f"Row Count: {result.validation_result.row_count}")
    print(f"Processing Duration: {result.processing_duration:.2f}s")
    print(f"{'=' * 80}\n")
