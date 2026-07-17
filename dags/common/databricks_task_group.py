"""
Databricks task creation for EcomFlow.

Airflow is responsible only for orchestration.

Databricks is responsible for:
    - Workflow execution
    - Task dependency
    - Notebook execution
    - Spark runtime
    - Business logic
"""

from __future__ import annotations

from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

from include.config.databricks import get_databricks_conn_id
from include.databricks.jobs import get_bronze_job_id


def create_bronze_workflow_task() -> DatabricksRunNowOperator:
    """
    Create a Databricks RunNowOperator that triggers the Bronze Workflow.

    Returns:
        Configured DatabricksRunNowOperator.
    """

    return DatabricksRunNowOperator(
        task_id="run_bronze_workflow",
        databricks_conn_id=get_databricks_conn_id(),
        job_id=get_bronze_job_id(),
        wait_for_termination=True,
        polling_period_seconds=30,
        do_xcom_push=False,
    )