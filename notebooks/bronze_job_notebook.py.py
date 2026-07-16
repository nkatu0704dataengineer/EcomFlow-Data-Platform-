# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Configure Parameters
# Create widgets for job parameters
dbutils.widgets.text("layer", "bronze", "Layer")
dbutils.widgets.text("catalog_name", "ecomflow", "Catalog Name")
dbutils.widgets.text("schema_name", "ecom_bronze_v2", "Schema Name")
dbutils.widgets.text("table_name", "", "Table Name")
dbutils.widgets.text("volume_name", "pipeline_data", "Volume Name")

# Get parameter values
layer = dbutils.widgets.get("layer")
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
table_name = dbutils.widgets.get("table_name")
volume_name = dbutils.widgets.get("volume_name")

# Debug: Print all parameter values
print(f"=== Parameters Received ===")
print(f"layer: '{layer}' (length={len(layer)})")
print(f"catalog_name: '{catalog_name}' (length={len(catalog_name)})")
print(f"schema_name: '{schema_name}' (length={len(schema_name)})")
print(f"table_name: '{table_name}' (length={len(table_name)})")
print(f"volume_name: '{volume_name}' (length={len(volume_name)})")
print(f"=========================")

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

# If table_name is empty, process all tables in the schema
if not table_name or table_name.strip() == "":
    print("No specific table provided - processing all tables in schema")
    print("=" * 80)
    
    # Get all tables in the schema
    tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").collect()
    table_names = [row.tableName for row in tables]
    
    print(f"Found {len(table_names)} tables to process: {', '.join(table_names)}")
    print("=" * 80)
    
    results = []
    failed_tables = []
    
    for idx, current_table in enumerate(table_names, 1):
        print(f"\n[{idx}/{len(table_names)}] Processing table: {current_table}")
        print("-" * 80)
        
        try:
            result = run_pipeline(
                spark=spark,
                catalog_name=catalog_name,
                schema_name=schema_name,
                table_name=current_table,
                layer=layer,
                volume_name=volume_name,
            )
            results.append((current_table, result))
            print(f"✓ {current_table}: Success")
            print(f"  Validation: {'✓ Passed' if result.validation_result.is_valid else '✗ Failed'}")
            print(f"  CSV Path: {result.csv_path}")
            
        except Exception as e:
            failed_tables.append((current_table, str(e)))
            print(f"✗ {current_table}: Failed - {str(e)}")
            # Continue processing other tables instead of failing the entire job
            continue
    
    # Summary
    print("\n" + "=" * 80)
    print("PIPELINE SUMMARY")
    print("=" * 80)
    print(f"Total tables: {len(table_names)}")
    print(f"Successful: {len(results)}")
    print(f"Failed: {len(failed_tables)}")
    
    if failed_tables:
        print("\nFailed tables:")
        for table, error in failed_tables:
            print(f"  - {table}: {error}")
    
    print("=" * 80)
    
    # Fail the job if any table failed
    if failed_tables:
        raise Exception(f"{len(failed_tables)} table(s) failed to process. See details above.")
        
else:
    # Process single table
    print(f"Processing single table: {table_name}")
    print("=" * 80)
    
    result = run_pipeline(
        spark=spark,
        catalog_name=catalog_name,
        schema_name=schema_name,
        table_name=table_name,
        layer=layer,
        volume_name=volume_name,
    )
    
    print("=" * 80)
    print("Pipeline completed successfully!")
    print(f"Validation: {'✓ Passed' if result.validation_result.is_valid else '✗ Failed'}")
    print(f"CSV Path: {result.csv_path}")
    print(f"Object Path: {result.object_path}")
    print("=" * 80)

# COMMAND ----------

