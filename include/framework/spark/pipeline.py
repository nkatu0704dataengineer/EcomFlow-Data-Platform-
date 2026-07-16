"""EcomFlow Framework - Pipeline Orchestrator

Coordinates the reader, validator, writer, uploader, and metadata generator
to execute a full Spark dataset pipeline within Databricks runtime.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from include.framework.spark.metadata import generate_metadata
from include.framework.spark.models.pipeline_result import PipelineResult
from include.framework.spark.reader import read_delta_table
from include.framework.spark.uploader import upload_directory_to_volume, UploadError
from include.framework.spark.validator import validate_dataframe
from include.framework.spark.writer import write_csv


def run_pipeline(
    spark: SparkSession,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    layer: str,
    volume_name: str = "pipeline_data",
) -> PipelineResult:
    """Run the framework pipeline for a Delta table dataset.
    
    This function is the main orchestrator for EcomFlow ETL. It is invoked
    from Databricks runtime by bronze_runner.py and coordinates all stages
    of the pipeline: read → validate → write → upload → metadata.
    
    Args:
        spark: SparkSession (created by Databricks runtime)
        catalog_name: Unity Catalog catalog name (e.g., "ecom_catalog")
        schema_name: Unity Catalog schema name (e.g., "bronze")
        table_name: Delta table name to process
        layer: Data layer (e.g., "bronze", "silver", "gold")
        volume_name: Unity Catalog volume name for output
    
    Returns:
        PipelineResult containing validation, metadata, paths, and upload location
    
    Raises:
        TypeError: If any parameter has incorrect type
        ValueError: If any parameter is empty or validation fails
    """
    # Accept both SparkSession and DatabricksSession (used in serverless/Databricks Connect)
    # Check if spark has required methods instead of strict type checking
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
    if not isinstance(layer, str):
        raise TypeError("layer must be a string")
    if not isinstance(volume_name, str):
        raise TypeError("volume_name must be a string")

    catalog_name = catalog_name.strip()
    schema_name = schema_name.strip()
    table_name = table_name.strip()
    layer = layer.strip()
    volume_name = volume_name.strip()

    if not catalog_name:
        raise ValueError("catalog_name cannot be empty")
    if not schema_name:
        raise ValueError("schema_name cannot be empty")
    if not table_name:
        raise ValueError("table_name cannot be empty")
    if not layer:
        raise ValueError("layer cannot be empty")
    if not volume_name:
        raise ValueError("volume_name cannot be empty")

    # Stage 1: Read from Delta Lake
    dataframe = read_delta_table(
        spark=spark,
        catalog_name=catalog_name,
        schema_name=schema_name,
        table_name=table_name,
    )

    # Stage 2: Validate data quality (diagnostic only - reports issues but doesn't block)
    validation_result = validate_dataframe(dataframe)
    if not validation_result.is_valid:
        # Only raise error if DataFrame is empty (critical condition)
        error_msg = " ".join(validation_result.error_messages)
        raise ValueError(f"Validation failed: {error_msg}")
    
    # Log any data quality warnings (nulls, duplicates) but continue processing
    if validation_result.error_messages:
        print("\n" + "="*60)
        print("📊 Data Quality Report:")
        for msg in validation_result.error_messages:
            print(f"   {msg}")
        print("="*60 + "\n")

    # Stage 3: Write to CSV in working directory
    csv_path = write_csv(
        df=dataframe,
        layer=layer,
        dataset=table_name,
        mode="overwrite",
        catalog_name=catalog_name,
        schema_name=schema_name,
        volume_name=volume_name,
    )

    # Stage 4: Upload to Unity Catalog Volume
    object_path = None
    try:
        object_path = upload_directory_to_volume(
            spark=spark,
            directory_path=csv_path,
            catalog_name=catalog_name,
            schema_name=schema_name,
            volume_name=volume_name,
            layer=layer,
            dataset=table_name,
        )
    except UploadError as e:
        print(f"⚠️  Warning: Failed to upload to UC Volume: {str(e)}")
        print(f"    CSV files still available at: {csv_path}")
        object_path = None

    # Stage 5: Generate metadata
    metadata = generate_metadata(
        df=dataframe,
        validation_result=validation_result,
        layer=layer,
        dataset=table_name,
        object_path=object_path if object_path else "N/A (Volume upload failed)",
        file_format="csv",
    )

    return PipelineResult(
        validation_result=validation_result,
        metadata=metadata,
        csv_path=csv_path,
        object_path=object_path,
    )
