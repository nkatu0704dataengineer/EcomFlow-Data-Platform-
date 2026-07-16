"""
Databricks task group for the EcomFlow Bronze layer.

This module creates Airflow tasks that trigger the existing Databricks
Bronze Job. Airflow is responsible only for orchestration.

Databricks is responsible for:
    - Serverless compute
    - Python Script execution
    - Runtime environment
    - Libraries
    - Spark execution

The Bronze Job receives dataset-specific runtime parameters and delegates
all business logic to the EcomFlow framework.
"""

from __future__ import annotations

from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

from include.config.databricks import get_databricks_conn_id
from include.databricks.jobs import get_bronze_job_id


def create_databricks_tasks(
    dataset_configs: list[dict[str, str]],
) -> list[DatabricksRunNowOperator]:
    """
    Create Databricks RunNow tasks for every Bronze dataset.

    Args:
        dataset_configs:
            Dataset runtime configuration.

    Returns:
        List of DatabricksRunNowOperator tasks.
    """

    conn_id = get_databricks_conn_id()
    job_id = get_bronze_job_id()

    tasks: list[DatabricksRunNowOperator] = []

    for dataset in dataset_configs:
        task = DatabricksRunNowOperator(
            task_id=dataset["task_id"],
            databricks_conn_id=conn_id,
            job_id=job_id,
            python_params=[
                "--layer",
                dataset["layer"],
                "--catalog_name",
                dataset["catalog_name"],
                "--schema_name",
                dataset["schema_name"],
                "--table_name",
                dataset["table_name"],
                "--volume_name",
                dataset["volume_name"],
            ],
            wait_for_termination=True,
            polling_period_seconds=30,
            do_xcom_push=False,
        )

        tasks.append(task)

    return tasks