"""
Silver Employees Notebook

Business Logic for Employees Dataset

Layer:
    Silver

Dataset:
    employees
"""

from __future__ import annotations

import sys
import os

# Add EcomFlow to path for framework imports
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), '../..')))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

from include.framework.silver_spark.pipeline import run_pipeline


# ==========================================================
# Schema Refactoring
# ==========================================================

def _refactor_schema(df: DataFrame) -> DataFrame:
    """
    Rename Bronze columns to Silver schema.

    Remove unused columns.

    Select final Silver schema.
    """
    return df \
        .withColumnRenamed("emp_id", "employee_id") \
        .withColumnRenamed("emp_gender", "gender") \
        .withColumnRenamed("emp_phone", "phone") \
        .withColumnRenamed("emp_born", "born") \
        .withColumnRenamed("emp_account", "username") \
        .withColumnRenamed("emp_password", "password") \
        .withColumnRenamed("emp_role", "role") \
        .withColumnRenamed("sub_id", "subsidiary_id") \
        .drop("emp_name", "emp_email", "emp_address")


# ==========================================================
# Employee Identity
# ==========================================================

def _generate_employee_identity(df: DataFrame) -> DataFrame:
    """
    Generate realistic Vietnamese employee names.

    Creates:
    - last_name (surname + middle name)
    - first_name

    Gender determines first_name selection.
    """
    # Vietnamese name components
    surnames = [
        "Nguyễn", "Trần", "Lê", "Phạm", "Hoàng",
        "Huỳnh", "Phan", "Vũ", "Võ", "Đặng"
    ]
    
    middle_names = [
        "Văn", "Thị", "Hữu", "Minh",
        "Công Minh", "Văn Tuấn", "Thị Hương", "Hoàng Mai"
    ]
    
    male_first_names = [
        "An", "Bảo", "Cường", "Dũng", "Đức", "Hải", "Hùng", "Khoa",
        "Long", "Nam", "Phúc", "Quân", "Tài", "Thành", "Tuấn",
        "Việt", "Vinh", "Toàn", "Trung", "Tâm"
    ]
    
    female_first_names = [
        "Anh", "Chi", "Hà", "Hằng", "Hoa", "Hương", "Lan", "Linh",
        "Mai", "Nga", "Nhung", "Phương", "Quỳnh", "Thu", "Thảo",
        "Trang", "Uyên", "Vân", "Yến", "My"
    ]
    
    # Generate random indices
    df = df \
        .withColumn("_surname_idx", (F.rand() * len(surnames)).cast("int")) \
        .withColumn("_middle_idx", (F.rand() * len(middle_names)).cast("int")) \
        .withColumn("_male_idx", (F.rand() * len(male_first_names)).cast("int")) \
        .withColumn("_female_idx", (F.rand() * len(female_first_names)).cast("int"))
    
    # Map indices to names using element_at (1-indexed)
    df = df \
        .withColumn(
            "_surname",
            F.element_at(F.array([F.lit(s) for s in surnames]), F.col("_surname_idx") + 1)
        ) \
        .withColumn(
            "_middle",
            F.element_at(F.array([F.lit(m) for m in middle_names]), F.col("_middle_idx") + 1)
        ) \
        .withColumn(
            "_male_first",
            F.element_at(F.array([F.lit(m) for m in male_first_names]), F.col("_male_idx") + 1)
        ) \
        .withColumn(
            "_female_first",
            F.element_at(F.array([F.lit(f) for f in female_first_names]), F.col("_female_idx") + 1)
        )
    
    # Select first_name based on gender
    df = df.withColumn(
        "first_name",
        F.when(
            F.lower(F.trim(F.col("gender"))).isin(["male", "m"]),
            F.col("_male_first")
        ).otherwise(F.col("_female_first"))
    )
    
    # Create last_name (surname + middle)
    df = df.withColumn(
        "last_name",
        F.concat(F.col("_surname"), F.lit(" "), F.col("_middle"))
    )
    
    # Drop temporary columns
    df = df.drop(
        "_surname_idx", "_middle_idx", "_male_idx", "_female_idx",
        "_surname", "_middle", "_male_first", "_female_first"
    )
    
    return df


# ==========================================================
# Standardization
# ==========================================================

def _generate_company_email(df: DataFrame) -> DataFrame:
    """
    Generate company email from Vietnamese name.

    Format: {first_name}{surname}{employee_number}@ecomflow.vn

    Example:
        Nguyễn Văn An (EP0001) → annguyen0001@ecomflow.vn

    Removes Vietnamese diacritics.
    """
    # Vietnamese diacritics mapping
    vietnamese_map = {
        'à': 'a', 'á': 'a', 'ả': 'a', 'ã': 'a', 'ạ': 'a',
        'ă': 'a', 'ằ': 'a', 'ắ': 'a', 'ẳ': 'a', 'ẵ': 'a', 'ặ': 'a',
        'â': 'a', 'ầ': 'a', 'ấ': 'a', 'ẩ': 'a', 'ẫ': 'a', 'ậ': 'a',
        'đ': 'd',
        'è': 'e', 'é': 'e', 'ẻ': 'e', 'ẽ': 'e', 'ẹ': 'e',
        'ê': 'e', 'ề': 'e', 'ế': 'e', 'ể': 'e', 'ễ': 'e', 'ệ': 'e',
        'ì': 'i', 'í': 'i', 'ỉ': 'i', 'ĩ': 'i', 'ị': 'i',
        'ò': 'o', 'ó': 'o', 'ỏ': 'o', 'õ': 'o', 'ọ': 'o',
        'ô': 'o', 'ồ': 'o', 'ố': 'o', 'ổ': 'o', 'ỗ': 'o', 'ộ': 'o',
        'ơ': 'o', 'ờ': 'o', 'ớ': 'o', 'ở': 'o', 'ỡ': 'o', 'ợ': 'o',
        'ù': 'u', 'ú': 'u', 'ủ': 'u', 'ũ': 'u', 'ụ': 'u',
        'ư': 'u', 'ừ': 'u', 'ứ': 'u', 'ử': 'u', 'ữ': 'u', 'ự': 'u',
        'ỳ': 'y', 'ý': 'y', 'ỷ': 'y', 'ỹ': 'y', 'ỵ': 'y'
    }
    
    @F.udf(returnType=StringType())
    def remove_diacritics(text):
        """Remove Vietnamese diacritics from text."""
        if text is None:
            return None
        text_lower = text.lower()
        for viet_char, latin_char in vietnamese_map.items():
            text_lower = text_lower.replace(viet_char, latin_char)
        return text_lower
    
    # Extract surname (first word of last_name)
    df = df.withColumn(
        "_surname",
        F.split(F.col("last_name"), " ").getItem(0)
    )
    
    # Remove diacritics from first_name and surname
    df = df \
        .withColumn("_first_ascii", remove_diacritics(F.col("first_name"))) \
        .withColumn("_surname_ascii", remove_diacritics(F.col("_surname")))
    
    # Extract employee number (remove "EP" prefix)
    df = df.withColumn(
        "_emp_num",
        F.regexp_replace(F.col("employee_id"), "EP", "")
    )
    
    # Create email: {first}{surname}{number}@ecomflow.vn
    df = df.withColumn(
        "email",
        F.concat(
            F.col("_first_ascii"),
            F.col("_surname_ascii"),
            F.col("_emp_num"),
            F.lit("@ecomflow.vn")
        )
    )
    
    # Drop temporary columns
    df = df.drop("_surname", "_first_ascii", "_surname_ascii", "_emp_num")
    
    return df


def _normalize_gender(df: DataFrame) -> DataFrame:
    """
    Standardize gender values.

    Maps:
    - male, m → Male
    - female, f → Female
    - default → Male
    """
    return df.withColumn(
        "gender",
        F.when(
            F.lower(F.trim(F.col("gender"))).isin(["male", "m"]), "Male"
        ).when(
            F.lower(F.trim(F.col("gender"))).isin(["female", "f"]), "Female"
        ).otherwise("Male")
    )


def _normalize_phone_number(df: DataFrame) -> DataFrame:
    """
    Standardize phone number.

    - Convert to string
    - Pad to 9 digits
    - Prepend leading zero

    Example: 123456789 → 0123456789
    """
    return df.withColumn(
        "phone",
        F.concat(
            F.lit("0"),
            F.lpad(F.col("phone").cast(StringType()), 9, "0")
        )
    )


# ==========================================================
# Datatype Conversion
# ==========================================================

def _convert_dates(df: DataFrame) -> DataFrame:
    """
    Convert date columns.

    born is already DATE type in Bronze, keep as is.
    """
    # born column is already DATE type from Bronze
    return df


# ==========================================================
# Address Engineering
# ==========================================================

def _generate_employee_address(df: DataFrame) -> DataFrame:
    """
    Generate realistic employee address.

    Replace Bronze fake address with curated Vietnamese cities.

    Uses top 15 richest/most developed cities in Vietnam.
    """
    # Top 15 richest/most developed cities in Vietnam
    cities = [
        "Hà Nội",
        "TP Hồ Chí Minh",
        "Hải Phòng",
        "Đà Nẵng",
        "Cần Thơ",
        "Bình Dương",
        "Đồng Nai",
        "Quảng Ninh",
        "Bắc Ninh",
        "Thanh Hóa",
        "Nghệ An",
        "Khánh Hòa",
        "Hải Dương",
        "Bà Rịa - Vũng Tàu",
        "Thừa Thiên Huế"
    ]
    
    # Generate random city index for each employee
    df = df.withColumn(
        "_city_idx",
        (F.rand() * len(cities)).cast("int")
    )
    
    # Map index to city name using element_at (1-indexed)
    df = df.withColumn(
        "address",
        F.element_at(F.array([F.lit(c) for c in cities]), F.col("_city_idx") + 1)
    )
    
    # Drop temporary column
    df = df.drop("_city_idx")
    
    return df


# ==========================================================
# Feature Engineering
# ==========================================================

def _generate_recruited_at(df: DataFrame) -> DataFrame:
    """
    Generate recruited_at.

    Recruitment period: 2022-01-01 → 2022-12-31

    Randomly assigns recruitment dates within the year 2022.
    """
    return df.withColumn(
        "recruited_at",
        F.expr("date_add(date('2022-01-01'), cast(rand() * 365 as int))")
    )


# ==========================================================
# Data Quality
# ==========================================================

def _apply_data_quality_rules(df: DataFrame) -> DataFrame:
    """
    Apply structural quality rules.

    Filters:
    - Remove NULL employee_id
    - Remove NULL first_name
    - Remove NULL gender
    - Remove NULL phone
    - Remove NULL role
    - Remove NULL subsidiary_id
    """
    return df.filter(
        F.col("employee_id").isNotNull() &
        F.col("first_name").isNotNull() &
        F.col("gender").isNotNull() &
        F.col("phone").isNotNull() &
        F.col("role").isNotNull() &
        F.col("subsidiary_id").isNotNull()
    )


def _select_final_schema(df: DataFrame) -> DataFrame:
    """
    Select final Silver schema.

    Orders columns for final output.
    """
    return df.select(
        "employee_id",
        "last_name",
        "first_name",
        "gender",
        "phone",
        "email",
        "born",
        "address",
        "recruited_at",
        "username",
        "password",
        "role",
        "subsidiary_id",
        "ingestion_time",
        "source_file"
    )


# ==========================================================
# Public API
# ==========================================================

def transform(df: DataFrame) -> DataFrame:
    """
    Execute Employees Silver Transformation.

    Pipeline:
    1. Refactor schema (rename columns)
    2. Generate employee identity (Vietnamese names)
    3. Generate company email
    4. Normalize gender
    5. Normalize phone number
    6. Convert dates
    7. Generate employee address
    8. Generate recruited_at
    9. Apply data quality rules
    10. Select final schema
    """
    df = _refactor_schema(df)

    df = _generate_employee_identity(df)

    df = _generate_company_email(df)

    df = _normalize_gender(df)

    df = _normalize_phone_number(df)

    df = _convert_dates(df)

    df = _generate_employee_address(df)

    df = _generate_recruited_at(df)

    df = _apply_data_quality_rules(df)

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
        source_table="employees",
        
        target_catalog="ecomflow",
        target_schema="ecom_silver",
        target_table="employees",
        target_volume="silver",
        transform=transform,
    )

    print(f"\n{'=' * 80}")
    print("EMPLOYEES SILVER PIPELINE COMPLETED")
    print(f"{'=' * 80}")
    print(f"Source: ecomflow.ecom_bronze_v2.employees")
    print(f"Target: ecomflow.ecom_silver.employees")
    print(f"Validation Status: {'✅ PASSED' if result.validation_result.is_valid else '❌ FAILED'}")
    print(f"Row Count: {result.validation_result.row_count}")
    print(f"Processing Duration: {result.processing_duration:.2f}s")
    print(f"{'=' * 80}\n")
