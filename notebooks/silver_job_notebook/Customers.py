"""
Silver Customers Notebook

Business Logic for Customers Dataset

Layer:
    Silver

Dataset:
    customers
"""

from __future__ import annotations

import sys
import os

# Add EcomFlow to path for framework imports
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), '../..')))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import unicodedata

from include.framework.silver_spark.pipeline import run_pipeline


# ==========================================================
# Schema Refactoring
# ==========================================================

def _refactor_schema(df: DataFrame) -> DataFrame:
    """
    Rename columns to match silver schema.
    
    Args:
        df: Bronze users DataFrame.
        
    Returns:
        DataFrame with renamed columns.
    """
    return df \
        .withColumnRenamed("user_name", "full_name") \
        .withColumnRenamed("user_email", "email") \
        .withColumnRenamed("user_phone", "phone_number") \
        .withColumnRenamed("user_gender", "gender") \
        .withColumnRenamed("user_born", "birth_date") \
        .withColumnRenamed("user_address", "full_address") \
        .withColumnRenamed("user_account", "account_name") \
        .withColumnRenamed("user_ranked", "customer_rank") \
        .withColumnRenamed("user_createdat", "created_at")


# ==========================================================
# Customer Identity
# ==========================================================

def _split_customer_id(df: DataFrame) -> DataFrame:
    """
    Generate acc_id and cus_id from customer data.
    
    acc_id: Account identifier (ACC00000001, ACC00000002, ...)
    cus_id: Customer identifier (CUS000001, CUS000002, ...)
    
    Args:
        df: DataFrame with customer data.
        
    Returns:
        DataFrame with acc_id and cus_id columns.
    """
    # Generate acc_id using monotonically increasing ID
    df = df.withColumn(
        "acc_id",
        F.concat(
            F.lit("ACC"),
            F.lpad(
                (F.monotonically_increasing_id() + 1).cast("string"),
                8,
                "0"
            )
        )
    )
    
    # Generate customer_identity for grouping
    df = df.withColumn(
        "customer_identity",
        F.concat_ws(
            "_",
            F.coalesce(F.col("email"), F.lit("unknown")),
            F.coalesce(F.col("phone_number"), F.lit("unknown"))
        )
    )
    
    # Generate cus_id using dense_rank over customer_identity
    customer_window = Window.orderBy("customer_identity")
    
    df = df.withColumn(
        "cus_seq",
        F.dense_rank().over(customer_window)
    )
    
    df = df.withColumn(
        "cus_id",
        F.concat(
            F.lit("CUS"),
            F.lpad(
                F.col("cus_seq").cast("string"),
                6,
                "0"
            )
        )
    )
    
    # Drop temporary columns
    df = df.drop("customer_identity", "cus_seq")
    
    return df


def _generate_customer_name(df: DataFrame) -> DataFrame:
    """
    Generate Vietnamese names and derive gender from first name.
    
    Creates last_name (surname + middle name) and first_name.
    Gender is derived from first_name.
    
    Args:
        df: DataFrame with customer data.
        
    Returns:
        DataFrame with last_name, first_name, and derived gender.
    """
    # Vietnamese name components
    surnames = ['Nguyễn', 'Trần', 'Lê', 'Phạm', 'Hoàng', 'Huỳnh', 'Phan', 'Vũ', 'Đặng', 'Bùi','Nghiêm','Ngô','Lý']
    one_word_middles = ['Văn', 'Thị', 'Minh', 'Ngọc', 'Thanh', 'Quốc', 'Gia', 'Anh', 'Đức', 'Hữu']
    two_word_middles = ['Thị Minh', 'Ngọc Anh', 'Thanh Tâm', 'Quốc Bảo', 'Gia Hân','Văn Võ','Hồng Vũ']
    male_first_names = ['An', 'Bảo', 'Đức', 'Huy', 'Khang', 'Long','Tú','Quân','Tuấn','Hải','Hùng','Hoàng']
    female_first_names = ['Anh', 'Chi', 'Linh', 'Trang', 'Hương', 'Ngân', 'Vy', 'Thảo', 'Nhi','Hường','Ly','Như','Nhã','Ngọc','Thy','My']
    all_first_names = male_first_names + female_first_names
    
    # Generate random indices for name components
    df = df \
        .withColumn("surname_idx", (F.rand() * len(surnames)).cast("int")) \
        .withColumn("middle_type", (F.rand() * 2).cast("int")) \
        .withColumn("middle_idx_1", (F.rand() * len(one_word_middles)).cast("int")) \
        .withColumn("middle_idx_2", (F.rand() * len(two_word_middles)).cast("int")) \
        .withColumn("first_name_idx", (F.rand() * len(all_first_names)).cast("int"))
    
    # UDFs for name mapping
    @F.udf(returnType=StringType())
    def get_surname(idx):
        return surnames[idx]
    
    @F.udf(returnType=StringType())
    def get_middle_name(middle_type, idx1, idx2):
        if middle_type == 0:
            return one_word_middles[idx1]
        else:
            return two_word_middles[idx2]
    
    @F.udf(returnType=StringType())
    def get_first_name(idx):
        return all_first_names[idx]
    
    @F.udf(returnType=StringType())
    def derive_gender(first_name):
        if first_name in male_first_names:
            return 'Male'
        elif first_name in female_first_names:
            return 'Female'
        else:
            return 'Unknown'
    
    # Apply mappings
    df = df \
        .withColumn("surname", get_surname(F.col("surname_idx"))) \
        .withColumn("middle_name", get_middle_name(F.col("middle_type"), F.col("middle_idx_1"), F.col("middle_idx_2"))) \
        .withColumn("first_name", get_first_name(F.col("first_name_idx"))) \
        .withColumn("last_name", F.concat(F.col("surname"), F.lit(" "), F.col("middle_name"))) \
        .withColumn("gender", derive_gender(F.col("first_name")))
    
    # Drop temporary columns
    df = df.drop(
        "surname_idx", "middle_type", "middle_idx_1", "middle_idx_2",
        "first_name_idx", "surname", "middle_name", "full_name"
    )
    
    return df


def _generate_account_name(df: DataFrame) -> DataFrame:
    """
    Generate account_name from first_name + acc_id.
    
    Format: {first_name_ascii}_{acc_id_suffix}
    Example: duc_00000001
    
    Args:
        df: DataFrame with first_name and acc_id.
        
    Returns:
        DataFrame with account_name column.
    """
    @F.udf(returnType=StringType())
    def remove_accents(text):
        if text is None:
            return None
        nfd = unicodedata.normalize('NFD', text)
        return ''.join(c for c in nfd if unicodedata.category(c) != 'Mn')
    
    # Extract numeric suffix from acc_id and create account_name
    df = df \
        .withColumn("first_ascii", F.lower(remove_accents(F.col("first_name")))) \
        .withColumn("acc_suffix", F.substring(F.col("acc_id"), 4, 8)) \
        .withColumn("account_name", F.concat(F.col("first_ascii"), F.lit("_"), F.col("acc_suffix"))) \
        .drop("first_ascii", "acc_suffix")
    
    return df


# ==========================================================
# Standardization
# ==========================================================

def _clean_email(df: DataFrame) -> DataFrame:
    """
    Generate standardized email from first_name and last_name.
    
    Format: {first_name}.{last_name}{random_3_digits}@gmail.com
    All lowercase, accents removed, spaces removed.
    
    Args:
        df: DataFrame with first_name and last_name.
        
    Returns:
        DataFrame with generated email column.
    """
    @F.udf(returnType=StringType())
    def remove_accents(text):
        if text is None:
            return None
        nfd = unicodedata.normalize('NFD', text)
        return ''.join(c for c in nfd if unicodedata.category(c) != 'Mn')
    
    df = df \
        .withColumn("first_ascii", F.lower(remove_accents(F.col("first_name")))) \
        .withColumn("last_ascii", F.lower(remove_accents(F.regexp_replace(F.col("last_name"), " ", "")))) \
        .withColumn("random_suffix", F.lpad((F.rand() * 999).cast("int").cast("string"), 3, "0")) \
        .withColumn(
            "email",
            F.concat(
                F.col("first_ascii"),
                F.lit("."),
                F.col("last_ascii"),
                F.col("random_suffix"),
                F.lit("@gmail.com")
            )
        ) \
        .drop("first_ascii", "last_ascii", "random_suffix")
    
    return df


def _normalize_phone(df: DataFrame) -> DataFrame:
    """
    Format phone number as string with leading zero.
    
    Converts phone_number to string and adds leading '0'.
    Handles null values by setting to 'UNKNOWN'.
    
    Args:
        df: DataFrame with phone_number column.
        
    Returns:
        DataFrame with normalized phone_number.
    """
    return df.withColumn(
        "phone_number",
        F.when(
            F.col("phone_number").isNotNull(),
            F.concat(F.lit("0"), F.col("phone_number").cast("string"))
        ).otherwise("UNKNOWN")
    )

def _convert_birth_date(df: DataFrame) -> DataFrame:
    """
    Convert birth_date to DateType.
    
    Args:
        df: DataFrame with birth_date string column in ISO format (yyyy-MM-dd).
        
    Returns:
        DataFrame with birth_date as DateType.
    """
    return df.withColumn(
        "birth_date",
        F.to_date(F.col("birth_date"), "yyyy-MM-dd")
    )


# ==========================================================
# Address Processing
# ==========================================================

def _parse_address(df: DataFrame) -> DataFrame:
    """
    Generate random Vietnamese address with district and city.
    
    Cities (random selection):
        - Hà Nội
        - Đà Nẵng
        - TP.HCM
    
    Districts (depends on city):
        - Hà Nội: Ba Đình, Cầu Giấy, Đống Đa, Hoàn Kiếm, Thanh Xuân
        - Đà Nẵng: Hải Châu, Thanh Khê, Liên Chiểu, Sơn Trà, Ngũ Hành Sơn
        - TP.HCM: Quận 1, Quận 3, Bình Thạnh, Gò Vấp, Thủ Đức
    
    Args:
        df: DataFrame with full_address column.
        
    Returns:
        DataFrame with district and city columns, full_address dropped.
    """
    # Generate random city index (0, 1, 2)
    df = df.withColumn("city_idx", (F.rand() * 3).cast("int"))
    
    # Generate random district index (0, 1, 2, 3, 4) - 5 districts per city
    df = df.withColumn("district_idx", (F.rand() * 5).cast("int"))
    
    # Map city_idx to city name
    df = df.withColumn(
        "city",
        F.when(F.col("city_idx") == 0, F.lit("Hà Nội"))
        .when(F.col("city_idx") == 1, F.lit("Đà Nẵng"))
        .otherwise(F.lit("TP.HCM"))
    )
    
    # Map (city_idx, district_idx) to district name
    df = df.withColumn(
        "district",
        # Hà Nội districts
        F.when(
            F.col("city_idx") == 0,
            F.when(F.col("district_idx") == 0, F.lit("Ba Đình"))
            .when(F.col("district_idx") == 1, F.lit("Cầu Giấy"))
            .when(F.col("district_idx") == 2, F.lit("Đống Đa"))
            .when(F.col("district_idx") == 3, F.lit("Hoàn Kiếm"))
            .otherwise(F.lit("Thanh Xuân"))
        )
        # Đà Nẵng districts
        .when(
            F.col("city_idx") == 1,
            F.when(F.col("district_idx") == 0, F.lit("Hải Châu"))
            .when(F.col("district_idx") == 1, F.lit("Thanh Khê"))
            .when(F.col("district_idx") == 2, F.lit("Liên Chiểu"))
            .when(F.col("district_idx") == 3, F.lit("Sơn Trà"))
            .otherwise(F.lit("Ngũ Hành Sơn"))
        )
        # TP.HCM districts
        .otherwise(
            F.when(F.col("district_idx") == 0, F.lit("Quận 1"))
            .when(F.col("district_idx") == 1, F.lit("Quận 3"))
            .when(F.col("district_idx") == 2, F.lit("Bình Thạnh"))
            .when(F.col("district_idx") == 3, F.lit("Gò Vấp"))
            .otherwise(F.lit("Thủ Đức"))
        )
    )
    
    # Drop temporary columns and full_address
    df = df.drop("city_idx", "district_idx", "full_address")
    
    return df


# ==========================================================
# Feature Engineering
# ==========================================================

def _generate_age(df: DataFrame) -> DataFrame:
    """
    Calculate customer age from birth_date.
    
    Args:
        df: DataFrame with birth_date column.
        
    Returns:
        DataFrame with age column.
    """
    return df.withColumn(
        "age",
        F.floor(
            F.datediff(
                F.current_date(),
                F.col("birth_date")
            ) / 365
        )
    )


def _generate_account_age(df: DataFrame) -> DataFrame:
    """
    Calculate account_age_days from created_at.
    
    Args:
        df: DataFrame with created_at string column (format: M/d/yyyy H:mm).
        
    Returns:
        DataFrame with account_age_days column.
    """
    # Convert created_at string to timestamp temporarily for calculation
    df = df.withColumn(
        "created_at_temp",
        F.to_timestamp(F.col("created_at"), "M/d/yyyy H:mm")
    )
    
    # Calculate account age in days
    df = df.withColumn(
        "account_age_days",
        F.datediff(F.current_date(), F.col("created_at_temp"))
    )
    
    # Drop temporary column, keep original created_at for later processing
    df = df.drop("created_at_temp")
    
    return df


# ==========================================================
# Business Intelligence
# ==========================================================

def _calculate_customer_rank(df: DataFrame) -> DataFrame:
    """
    Calculate customer rank based on total spending from orders.
    
    Ranks:
        - COPPER: < 50M VND
        - SILVER: 50M - 120M VND
        - GOLD: 120M - 250M VND
        - PLATINUM: 250M - 450M VND
        - DIAMOND: 450M - 780M VND
        - VIP: >= 780M VND
    
    If orders table doesn't exist yet, defaults all customers to COPPER.
    
    Args:
        df: DataFrame with acc_id.
        
    Returns:
        DataFrame with customer_rank column.
    """
    # Get SparkSession from DataFrame
    spark = df.sparkSession
    
    # Check if orders table exists
    try:
        orders_exists = spark.catalog.tableExists("ecomflow.ecom_silver.orders")
    except:
        orders_exists = False
    
    if not orders_exists:
        # Orders table doesn't exist yet - default to COPPER
        df = df.withColumn("customer_rank", F.lit("COPPER"))
        return df
    
    # Calculate total_spent from orders table
    total_spent = spark.sql("""
        SELECT 
            o.acc_id,
            COALESCE(SUM(o.total_amount), 0) as total_spent
        FROM ecomflow.ecom_silver.orders o
        GROUP BY o.acc_id
    """)
    
    # Left join to keep all customers
    df = df.join(total_spent, on="acc_id", how="left").fillna({"total_spent": 0})
    
    # Calculate rank from total_spent
    df = df.withColumn(
        "customer_rank",
        F.when(F.col("total_spent") < 50000000, "COPPER")
        .when((F.col("total_spent") >= 50000000) & (F.col("total_spent") < 120000000), "SILVER")
        .when((F.col("total_spent") >= 120000000) & (F.col("total_spent") < 250000000), "GOLD")
        .when((F.col("total_spent") >= 250000000) & (F.col("total_spent") < 450000000), "PLATINUM")
        .when((F.col("total_spent") >= 450000000) & (F.col("total_spent") < 780000000), "DIAMOND")
        .when(F.col("total_spent") >= 780000000, "VIP")
        .otherwise("COPPER")
    )
    
    # Drop total_spent helper column
    df = df.drop("total_spent")
    
    return df


def _calculate_is_active(df: DataFrame) -> DataFrame:
    """
    Set account active status.
    
    All customers are considered active by default.
    
    Args:
        df: DataFrame with customer data.
        
    Returns:
        DataFrame with is_active column.
    """
    return df.withColumn("is_active", F.lit(True))


# ==========================================================
# Audit
# ==========================================================

def _generate_audit_columns(df: DataFrame) -> DataFrame:
    """
    Generate audit timestamps.
    
    - created_at: Converted from bronze timestamp
    - updated_at: Random date within 365 days after created_at
    
    Args:
        df: DataFrame with created_at column.
        
    Returns:
        DataFrame with created_at and updated_at.
    """
    df = df \
        .withColumn(
            "created_at",
            F.to_timestamp(F.col("created_at"), "M/d/yyyy H:mm")
        ) \
        .withColumn("rand_days", (F.rand() * 365).cast("int")) \
        .withColumn(
            "updated_at",
            (F.col("created_at").cast("long") + F.col("rand_days") * 86400).cast("timestamp")
        ) \
        .drop("rand_days")
    
    return df


# ==========================================================
# Public API
# ==========================================================

def transform(df: DataFrame) -> DataFrame:
    """
    Execute Customers Silver Transformation.
    
    Orchestrates the complete transformation pipeline:
    1. Rename schema
    2. Generate customer identity (acc_id, cus_id)
    3. Generate Vietnamese names and derive gender
    4. Generate account_name
    5. Generate email
    6. Normalize phone
    7. Normalize gender
    8. Convert birth_date
    9. Generate address (district, city)
    10. Calculate age
    11. Calculate account_age_days
    12. Calculate customer_rank from orders
    13. Set is_active
    14. Generate audit timestamps
    
    Args:
        df: Bronze users DataFrame.
        
    Returns:
        Transformed silver customers DataFrame.
    """
    df = _refactor_schema(df)
    
    df = _split_customer_id(df)
    
    df = _generate_customer_name(df)
    
    df = _generate_account_name(df)
    
    df = _clean_email(df)
    
    df = _normalize_phone(df)
    
    df = _convert_birth_date(df)
    
    df = _parse_address(df)
    
    df = _generate_age(df)
    
    df = _generate_account_age(df)
    
    df = _calculate_customer_rank(df)
    
    df = _calculate_is_active(df)
    
    df = _generate_audit_columns(df)
    
    # Drop unnecessary columns
    df = df.drop("user_password", "user_id")
    
    # Final column selection
    df = df.select(
        # Keys
        "acc_id",
        "cus_id",
        
        # Names
        "last_name",
        "first_name",
        
        # Contact
        "email",
        "phone_number",
        
        # Demographics
        "gender",
        "birth_date",
        "age",
        
        # Address
        "district",
        "city",
        
        # Account
        "account_name",
        "customer_rank",
        "is_active",
        "account_age_days",
        
        # Timestamps
        "created_at",
        "updated_at",
        
        # Metadata
        "ingestion_time",
        "source_file"
    )
    
    return df


# ==========================================================
# Entry Point
# ==========================================================

if __name__ == "__main__":
    
    result = run_pipeline(
        spark=spark,
        source_catalog="ecomflow",
        source_schema="ecom_bronze_v2",
        source_table="users",
        
        target_catalog="ecomflow",
        target_schema="ecom_silver",
        target_table="customers",
        target_volume="silver",
        transform=transform,
    )
    
    print(f"\n{'=' * 80}")
    print("CUSTOMERS SILVER PIPELINE COMPLETED")
    print(f"{'=' * 80}")
    print(f"Source: ecomflow.ecom_bronze_v2.users")
    print(f"Target: ecomflow.ecom_silver.customers")
    print(f"Validation Status: {'✅ PASSED' if result.validation_result.is_valid else '❌ FAILED'}")
    print(f"Row Count: {result.validation_result.row_count}")
    print(f"Processing Duration: {result.processing_duration:.2f}s")
    print(f"{'=' * 80}\n")
