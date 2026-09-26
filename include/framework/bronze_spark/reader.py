"""EcomFlow Spark reader for Delta Lake tables in Databricks Unity Catalog.

This module reads Delta tables from Databricks Unity Catalog. It is called
from within Databricks runtime and expects a SparkSession created by Databricks.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def read_delta_table(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    table_name: str,
) -> DataFrame:
    """Read a Delta table from Databricks Unity Catalog.
    
    Args:
        spark: SparkSession (created by Databricks runtime)
        catalog_name: Unity Catalog catalog name
        schema_name: Unity Catalog schema name
        table_name: Delta table name
    
    Returns:
        Spark DataFrame with table data
    """
    # Accept both SparkSession and DatabricksSession (used in serverless/Databricks Connect)
    if spark is None:
        raise TypeError("spark cannot be None")
    if not hasattr(spark, 'read') or not hasattr(spark, 'sql'):
        raise TypeError(f"spark must be a SparkSession or compatible session, got {type(spark).__name__}")
    if not isinstance(catalog_name, str):
        raise TypeError("catalog_name must be a string")
    if not isinstance(schema_name, str):
        raise TypeError("schema_name must be a string")
    if not isinstance(table_name, str):
        raise TypeError("table_name must be a string")

    catalog_name = catalog_name.strip()
    schema_name = schema_name.strip()
    table_name = table_name.strip()

    if not catalog_name:
        raise ValueError("catalog_name cannot be empty")
    if not schema_name:
        raise ValueError("schema_name cannot be empty")
    if not table_name:
        raise ValueError("table_name cannot be empty")

    full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
    dataframe = spark.read.format("delta").table(full_table_name)
    return dataframe
