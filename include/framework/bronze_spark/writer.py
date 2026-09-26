"""
EcomFlow Framework - Writer Module

Description:
    Write Spark DataFrame to CSV format.

Responsibilities:
    - Write Spark DataFrame to CSV.
    - Create output directory automatically.
    - Return generated CSV directory path.

This module DOES NOT:
    - Read data
    - Validate data
    - Upload data
    - Generate metadata
"""


from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame

def write_csv(
        df: DataFrame,
        layer: str,
        dataset: str,
        mode: str = "overwrite",
        catalog_name: str = None,
        schema_name: str = None,
        volume_name: str = None,
)-> str:
    if not isinstance(df, DataFrame):
        raise TypeError("df must be a Spark DataFrame instance")
    if not isinstance(layer, str):
        raise TypeError("layer must be a string")
    if not isinstance(dataset, str):
        raise TypeError("dataset must be a string")
    if not isinstance(mode, str):
        raise TypeError("mode must be a string")
    if catalog_name is not None and not isinstance(catalog_name, str):
        raise TypeError("catalog_name must be a string or None")
    if schema_name is not None and not isinstance(schema_name, str):
        raise TypeError("schema_name must be a string or None")
    if volume_name is not None and not isinstance(volume_name, str):
        raise TypeError("volume_name must be a string or None")
    
    layer=layer.strip()
    dataset=dataset.strip()
    mode=mode.strip().lower()

    if not layer:
        raise ValueError("layer cannot be empty")   
    if not dataset:
        raise ValueError("dataset cannot be empty")
    if mode not in ["overwrite", "append", "ignore", "error"]:
        raise ValueError("mode must be one of 'overwrite', 'append', 'ignore', or 'error'")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # If UC Volume parameters provided, write to staging area in UC Volume
    # (required for Databricks Serverless where DBFS is disabled)
    if catalog_name and schema_name and volume_name:
        output_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/staging/{layer}/{dataset}/{timestamp}"
        df.write.mode(mode).option("header", "true").csv(output_path)
        return output_path
    else:
        # Fallback for non-serverless environments (backward compatibility)
        output_path = Path(f"/tmp/ecomflow/{layer}/{dataset}/{timestamp}")
        output_path.mkdir(parents=True, exist_ok=True)
        df.write.mode(mode).option("header", "true").csv(str(output_path))
        return str(output_path)



    