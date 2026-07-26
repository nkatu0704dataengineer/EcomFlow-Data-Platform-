"""
Bronze Data Reader Module for Silver Layer Processing

This module provides a simple interface for reading Bronze Delta Tables
from Unity Catalog into Spark DataFrames for Silver layer transformations.

Author: EcomFlow Data Platform Team
"""

from pyspark.sql import SparkSession, DataFrame
import logging

logger = logging.getLogger(__name__)


def read_delta_table(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    table_name: str
) -> DataFrame:
    """
    Read a Bronze Delta Table from Unity Catalog.
    
    Args:
        spark: Active Spark session
        catalog_name: Unity Catalog name
        schema_name: Bronze schema name  
        table_name: Table name
    
    Returns:
        Spark DataFrame
    
    Raises:
        AnalysisException: If table does not exist or cannot be accessed
    
    Example:
        >>> df = read_delta_table(spark, "ecom_catalog", "bronze", "customers")
    """
    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    
    logger.info(f"Reading Bronze table: {full_table_name}")
    
    df = spark.read.table(full_table_name)
    
    logger.info(f"Successfully loaded {full_table_name}")
    
    return df
