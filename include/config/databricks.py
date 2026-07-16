"""EcomFlow Databricks configuration for Serverless.

This module configures Databricks Serverless-compatible job submission.
It avoids Existing Cluster, Connect, and other Free-Edition incompatibilities.
"""

from __future__ import annotations

import os


def get_databricks_conn_id() -> str:
    """Get Airflow connection ID for Databricks.
    
    Expected connection type: Databricks
    - Host: Databricks workspace URL (e.g., https://dbc-xxx.cloud.databricks.com)
    - Token: Personal access token or service principal token
    """
    return os.getenv("DATABRICKS_CONN_ID", "databricks_default").strip()


def get_databricks_notebook(layer: str) -> str:
    """Get Databricks notebook workspace path for a pipeline layer.
    Args:
        layer:
            Pipeline layer (bronze, silver, gold).
    Returns:
        Workspace path of the corresponding Databricks notebook.
    Raises:
        TypeError:
            If layer is not a string.
        ValueError:
            If layer is empty, unsupported, or the environment variable is missing.
    """
    if not isinstance(layer, str):
        raise TypeError("layer must be a string")
    layer = layer.strip().lower()
    if not layer:
        raise ValueError("layer cannot be empty")
    supported_layers = {"bronze", "silver", "gold"}
    if layer not in supported_layers:
        raise ValueError(
            f"Unsupported layer '{layer}'. "
            f"Supported layers: {', '.join(sorted(supported_layers))}"
        )

    env_name = f"DATABRICKS_{layer.upper()}_NOTEBOOK"
    value = os.getenv(env_name)
    if not value:
        raise ValueError(
            f"{env_name} not set. "
            f"Set it to the Databricks workspace path of {layer}_job_notebook.py"
        )
    return value.strip()
