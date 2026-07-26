"""
EcomFlow Silver DAG
Triggers the Databricks Silver Workflow.

Airflow is responsible only for orchestration.

Databricks is responsible for:
    - Workflow execution
    - Task dependency
    - Notebook execution
    - Spark runtime
    - Business logic
"""

from datetime import timedelta

import pendulum
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator

from dags.silver.silver_task_groups import silver_task_group
from include.config.airflow_config import get_default_args, TIMEZONE

DEFAULT_ARGS = get_default_args()
START_DATE = pendulum.datetime(2026, 1, 1, tz=TIMEZONE)

with DAG(
    dag_id="ecomflow_silver",
    description="Trigger the EcomFlow Silver Workflow on Databricks",
    default_args=get_default_args(),
    schedule="0 7,19 * * *",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=5),
    tags=["ecomflow", "silver", "databricks"],
) as dag:

    start = EmptyOperator(task_id="start")
    silver = silver_task_group()
    end = EmptyOperator(task_id="end")

    start >> silver >> end