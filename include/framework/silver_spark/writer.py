"""
Delta Writer Module for Silver Layer Processing.

This module is responsible for writing processed Spark DataFrames
to the Silver Delta Volume.

Responsibilities:
    - Write DataFrame as Delta format
    - Support configurable write mode
    - Log write operations

Does NOT:
    - Validate data
    - Generate metadata
    - Apply business logic
    - Handle exceptions

Author:
    EcomFlow Data Platform Team

Layer:
    Silver Framework
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def write_delta_table(
    df: DataFrame,
    volume_path: str,
    mode: str = "overwrite",
) -> None:
    """
    Write a Spark DataFrame to a Silver Delta Volume.

    Args:
        df:
            Processed Spark DataFrame.

        volume_path:
            Destination Delta Volume path.

            Example:
                /Volumes/ecom_catalog/ecom_silver/customers

        mode:
            Spark write mode.

            Supported values:
                - overwrite
                - append
                - error
                - ignore

            Defaults to "overwrite".

    Returns:
        None

    Example:
        >>> write_delta_table(
        ...     df=df,
        ...     volume_path="/Volumes/ecom_catalog/ecom_silver/customers",
        ... )
    """

    logger.info(
        "Writing Delta dataset to Silver Volume: %s",
        volume_path,
    )

    (
        df.write
        .format("delta")
        .mode(mode)
        .save(volume_path)
    )

    logger.info(
        "Successfully wrote Delta dataset to Silver Volume: %s",
        volume_path,
    )