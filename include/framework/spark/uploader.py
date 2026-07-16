"""EcomFlow Framework - Unity Catalog Volume Uploader

This module handles all upload operations to Unity Catalog Volumes.
It encapsulates volume path construction, upload verification, and error handling.
"""

from __future__ import annotations

import datetime
from typing import Optional

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession


class UploadError(Exception):
    """Raised when upload to Unity Catalog Volume fails."""
    pass


def upload_directory_to_volume(
    spark: SparkSession,
    directory_path: str,
    catalog_name: str,
    schema_name: str,
    volume_name: str,
    layer: str,
    dataset: str,
    timestamp: Optional[str] = None,
) -> str:
    """Upload a local directory to Unity Catalog Volume.
    
    This function encapsulates all logic for uploading data to UC Volumes:
    - Validates all input parameters
    - Constructs timestamped volume path
    - Performs upload using DBUtils
    - Verifies upload success
    - Returns the final volume path
    
    Args:
        spark: SparkSession (required for DBUtils access)
        directory_path: Local file system path to upload (e.g., "/tmp/bronze/table")
        catalog_name: Unity Catalog catalog name
        schema_name: Unity Catalog schema name
        volume_name: Unity Catalog volume name
        layer: Data layer (e.g., "bronze", "silver", "gold")
        dataset: Dataset name (typically table name)
        timestamp: Optional custom timestamp (format: YYYYMMDD_HHMMSS).
                   If None, current timestamp is generated automatically.
    
    Returns:
        str: Full volume path where data was uploaded
             (e.g., "/Volumes/catalog/schema/volume/layer/dataset/20240315_143022")
    
    Raises:
        TypeError: If any parameter has incorrect type
        ValueError: If any parameter is empty/invalid
        UploadError: If upload or verification fails
    
    Example:
        >>> object_path = upload_directory_to_volume(
        ...     spark=spark,
        ...     directory_path="/tmp/bronze/customers",
        ...     catalog_name="ecom_catalog",
        ...     schema_name="bronze",
        ...     volume_name="pipeline_data",
        ...     layer="bronze",
        ...     dataset="customers"
        ... )
        >>> print(object_path)
        /Volumes/ecom_catalog/bronze/pipeline_data/bronze/customers/20240315_143022
    """
    # Input validation - Type checks
    if spark is None:
        raise TypeError("spark cannot be None")
    if not hasattr(spark, 'read') or not hasattr(spark, 'sql'):
        raise TypeError(f"spark must be a SparkSession or compatible session, got {type(spark).__name__}")
    
    if not isinstance(directory_path, str):
        raise TypeError(f"directory_path must be a string, got {type(directory_path).__name__}")
    if not isinstance(catalog_name, str):
        raise TypeError(f"catalog_name must be a string, got {type(catalog_name).__name__}")
    if not isinstance(schema_name, str):
        raise TypeError(f"schema_name must be a string, got {type(schema_name).__name__}")
    if not isinstance(volume_name, str):
        raise TypeError(f"volume_name must be a string, got {type(volume_name).__name__}")
    if not isinstance(layer, str):
        raise TypeError(f"layer must be a string, got {type(layer).__name__}")
    if not isinstance(dataset, str):
        raise TypeError(f"dataset must be a string, got {type(dataset).__name__}")
    if timestamp is not None and not isinstance(timestamp, str):
        raise TypeError(f"timestamp must be a string or None, got {type(timestamp).__name__}")
    
    # Strip whitespace
    directory_path = directory_path.strip()
    catalog_name = catalog_name.strip()
    schema_name = schema_name.strip()
    volume_name = volume_name.strip()
    layer = layer.strip()
    dataset = dataset.strip()
    if timestamp is not None:
        timestamp = timestamp.strip()
    
    # Input validation - Empty checks
    if not directory_path:
        raise ValueError("directory_path cannot be empty")
    if not catalog_name:
        raise ValueError("catalog_name cannot be empty")
    if not schema_name:
        raise ValueError("schema_name cannot be empty")
    if not volume_name:
        raise ValueError("volume_name cannot be empty")
    if not layer:
        raise ValueError("layer cannot be empty")
    if not dataset:
        raise ValueError("dataset cannot be empty")
    if timestamp is not None and not timestamp:
        raise ValueError("timestamp cannot be empty string")
    
    # Generate timestamp if not provided
    if timestamp is None:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Construct volume path
    volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{layer}/{dataset}/{timestamp}"
    
    # Perform upload
    try:
        # Initialize DBUtils
        dbutils = DBUtils(spark)
        
        # Check if source is already in UC Volume (staging area)
        if directory_path.startswith("/Volumes/"):
            # Move from staging to final location within UC Volume
            print(f"Moving {directory_path} to {volume_path}...")
            dbutils.fs.mv(directory_path, volume_path, recurse=True)
        else:
            # Upload from local filesystem to volume
            print(f"Uploading {directory_path} to {volume_path}...")
            source_path = f"file:{directory_path}"
            dbutils.fs.cp(source_path, volume_path, recurse=True)
        
        # Verify upload
        uploaded_files = dbutils.fs.ls(volume_path)
        file_count = len(uploaded_files)
        
        if file_count == 0:
            raise UploadError(
                f"Upload verification failed: No files found at {volume_path}"
            )
        
        print(f"✓ Uploaded to UC Volume: {volume_path}")
        print(f"✓ Verified: {file_count} file(s) uploaded")
        
        return volume_path
        
    except UploadError:
        # Re-raise our custom errors
        raise
    except Exception as e:
        # Wrap any other exception in UploadError
        raise UploadError(
            f"Failed to upload {directory_path} to {volume_path}: {str(e)}"
        ) from e
