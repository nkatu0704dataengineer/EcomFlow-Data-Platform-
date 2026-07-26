"""
Silver Subsidiaries Notebook

Business Logic for Subsidiaries Dataset

Layer:
    Silver

Dataset:
    subsidiaries
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
# Schema Refactoring
# ==========================================================

def _refactor_schema(df: DataFrame) -> DataFrame:
    """
    Rename columns.

    Remove unused columns.

    Reorder final Silver schema.
    """
    return df.select(
        "sub_id",
        "sub_name",
        "phone_number",
        "establishment_date",
        "sub_street",
        "sub_district",
        "sub_city",
        "created_at",
        "updated_at",
        "ingestion_time",
        "source_file"
    )


# ==========================================================
# Standardization
# ==========================================================

def _normalize_phone_number(df: DataFrame) -> DataFrame:
    """
    Standardize phone number.

    - convert to string
    - prepend leading zero
    """
    return df.withColumn(
        "phone_number",
        F.concat(F.lit("0"), F.col("sub_phone").cast("string"))
    )


def _normalize_name(df: DataFrame) -> DataFrame:
    """
    Standardize subsidiary name.
    
    Transform from "subsi[number]" to "Subsidiary [number]"
    Example: "subsi30" -> "Subsidiary 30"
    """
    return df.withColumn(
        "sub_name",
        F.concat(
            F.lit("Subsidiary "),
            F.regexp_extract(F.col("sub_name"), r"subsi(\d+)", 1)
        )
    )


# ==========================================================
# Address Engineering
# ==========================================================

def _generate_subsidiary_address(df: DataFrame) -> DataFrame:
    """
    Randomly assign realistic Vietnamese address fields.
    
    For each subsidiary:
    - Randomly pick one city (Hà Nội, TP.HCM, Đà Nẵng)
    - Randomly pick one district matching that city
    - Randomly pick one street matching that city
    
    Generates three columns directly: sub_street, sub_district, sub_city
    """
    # Address data by city
    ha_noi_streets = [
        "Nguyễn Phong Sắc", "Nguyễn Lương Bằng", "Khuất Duy Tiến", 
        "Lê Văn Lương", "Hàng Đào", "Tràng Tiền", "Chùa Bộc", 
        "Trần Duy Hưng", "Nguyễn Chí Thanh", "Xã Đàn", "Kim Mã", 
        "Hoàng Quốc Việt", "Nguyễn Xiển", "Đội Cấn", "Xuân Thủy", "Hàng Ngang"
    ]
    ha_noi_districts = ["Ba Đình", "Cầu Giấy", "Đống Đa", "Hoàn Kiếm", "Thanh Xuân"]
    
    hcm_streets = [
        "Lê Văn Sỹ", "Quang Trung", "Nam Kỳ Khởi Nghĩa", "Nguyễn Huệ", 
        "Cách Mạng Tháng Tám", "Điện Biên Phủ", "Nguyễn Trãi", "Hồng Bàng", 
        "Nguyễn Gia Trí", "Nguyễn Thị Minh Khai", "Bạch Đằng", 
        "Xô Viết Nghệ Tĩnh", "Lê Đức Thọ", "Nguyễn Oanh", "Hàm Nghi", "An Dương Vương"
    ]
    hcm_districts = ["Bình Thạnh", "Gò Vấp", "Quận 1", "Quận 3", "Quận 5"]
    
    da_nang_streets = [
        "Lê Duẩn", "Hàm Nghi", "Phạm Văn Đồng", "Ngô Văn Sở", "Hùng Vương", 
        "Lê Văn Hiến", "Điện Biên Phủ", "Ngô Quyền", "Nguyễn Lương Bằng", 
        "Phan Châu Trinh", "Ngô Thì Sỹ", "Nguyễn Văn Linh", "Võ Nguyên Giáp", 
        "Lê Đức Thọ", "Nguyễn Văn Thoại", "Tôn Đức Thắng"
    ]
    da_nang_districts = ["Hải Châu", "Liên Chiểu", "Ngũ Hành Sơn", "Sơn Trà", "Thanh Khê"]
    
    # Step 1: Generate random city index (0, 1, 2)
    df = df.withColumn("_city_idx", F.floor(F.rand() * 3).cast("int"))
    
    # Step 2: Assign city based on index
    df = df.withColumn(
        "sub_city",
        F.when(F.col("_city_idx") == 0, F.lit("Hà Nội"))
        .when(F.col("_city_idx") == 1, F.lit("TP.HCM"))
        .otherwise(F.lit("Đà Nẵng"))
    )
    
    # Step 3: Generate random district index (1-5 for element_at which is 1-indexed)
    df = df.withColumn("_district_idx", F.floor(F.rand() * 5).cast("int") + 1)
    df = df.withColumn(
        "sub_district",
        F.when(
            F.col("_city_idx") == 0, 
            F.element_at(F.array([F.lit(d) for d in ha_noi_districts]), F.col("_district_idx"))
        )
        .when(
            F.col("_city_idx") == 1, 
            F.element_at(F.array([F.lit(d) for d in hcm_districts]), F.col("_district_idx"))
        )
        .otherwise(
            F.element_at(F.array([F.lit(d) for d in da_nang_districts]), F.col("_district_idx"))
        )
    )
    
    # Step 4: Generate random street index (1-16 for element_at which is 1-indexed)
    df = df.withColumn("_street_idx", F.floor(F.rand() * 16).cast("int") + 1)
    df = df.withColumn(
        "sub_street",
        F.when(
            F.col("_city_idx") == 0, 
            F.element_at(F.array([F.lit(s) for s in ha_noi_streets]), F.col("_street_idx"))
        )
        .when(
            F.col("_city_idx") == 1, 
            F.element_at(F.array([F.lit(s) for s in hcm_streets]), F.col("_street_idx"))
        )
        .otherwise(
            F.element_at(F.array([F.lit(s) for s in da_nang_streets]), F.col("_street_idx"))
        )
    )
    
    # Step 5: Drop temporary columns
    df = df.drop("_city_idx", "_district_idx", "_street_idx")
    
    return df


# ==========================================================
# Datatype Conversion
# ==========================================================

def _convert_timestamps(df: DataFrame) -> DataFrame:
    """
    Convert timestamp columns.

    String -> TimestampType or DateType
    """
    return df \
        .withColumn(
            "created_at",
            F.to_date(
                F.to_timestamp("created_at", "M/d/yyyy H:mm")
            )
        ) \
        .withColumn(
            "updated_at",
            F.to_date(
                F.to_timestamp("updated_at", "M/d/yyyy H:mm")
            )
        )


# ==========================================================
# Public API
# ==========================================================

def transform(df: DataFrame) -> DataFrame:

    df = _normalize_phone_number(df)

    df = _normalize_name(df)

    df = _generate_subsidiary_address(df)

    df = _convert_timestamps(df)

    df = _refactor_schema(df)

    return df


# ==========================================================
# Entry Point
# ==========================================================

if __name__ == "__main__":

    result = run_pipeline(
        spark=spark,
        source_catalog="ecomflow",
        source_schema="ecom_bronze_v2",
        source_table="subsidiaries",
        
        target_catalog="ecomflow",
        target_schema="ecom_silver",
        target_table="subsidiaries",
        target_volume="silver",
        transform=transform,
    )

    print(f"\n{'=' * 80}")
    print("SUBSIDIARIES SILVER PIPELINE COMPLETED")
    print(f"{'=' * 80}")
    print(f"Source: ecomflow.ecom_bronze_v2.subsidiaries")
    print(f"Target: ecomflow.ecom_silver.subsidiaries")
    print(f"Validation Status: {'✅ PASSED' if result.validation_result.is_valid else '❌ FAILED'}")
    print(f"Row Count: {result.validation_result.row_count}")
    print(f"Processing Duration: {result.processing_duration:.2f}s")
    print(f"{'=' * 80}\n")
