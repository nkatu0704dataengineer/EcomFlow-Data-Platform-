# Databricks notebook source
# DBTITLE 1,Configure Parameters
# Create widgets for job parameters
dbutils.widgets.text("layer", "silver", "Layer")
dbutils.widgets.text("catalog_name", "ecomflow", "Catalog Name")
dbutils.widgets.text("schema_name", "ecom_silver", "Schema Name")
dbutils.widgets.text("table_name", "", "Table Name")
dbutils.widgets.text("bucket_name", "silver-bucket", "Bucket Name")

# Get parameter values
layer = dbutils.widgets.get("layer")
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
table_name = dbutils.widgets.get("table_name")
bucket_name = dbutils.widgets.get("bucket_name")

print(f"Running {layer} pipeline for table: {catalog_name}.{schema_name}.{table_name}")

# COMMAND ----------

# DBTITLE 1,Setup Python Path
import sys
from pathlib import Path

# Add package root to Python path
package_root = Path("/Workspace/Users/tumaxpro99@gmail.com/EcomFlow")
if str(package_root) not in sys.path:
    sys.path.insert(0, str(package_root))

print(f"Package root added to path: {package_root}")

# COMMAND ----------

# DBTITLE 1,Run Pipeline
from include.framework.spark.pipeline import run_pipeline

# Run the pipeline using the global spark session
# Note: 'spark' is automatically available in Databricks notebooks
result = run_pipeline(
    spark=spark,
    catalog_name=catalog_name,
    schema_name=schema_name,
    table_name=table_name,
    layer=layer,
    bucket_name=bucket_name,
)

print("=" * 80)
print("Pipeline completed successfully!")
print(f"Validation: {'✓ Passed' if result.validation_result.is_valid else '✗ Failed'}")
print(f"CSV Path: {result.csv_path}")
print(f"Object Path: {result.object_path}")
print("=" * 80)

# COMMAND ----------

