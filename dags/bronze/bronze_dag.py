"""
EcomFlow Bronze DAG.

Triggers the Databricks Bronze Workflow.

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

from dags.bronze.bronze_task_groups import bronze_task_group
from include.config.airflow_config import get_default_args, TIMEZONE

DEFAULT_ARGS = get_default_args()
START_DATE = pendulum.datetime(2026, 1, 1, tz=TIMEZONE)

with DAG(
    dag_id="ecomflow_bronze",
    description="Trigger the EcomFlow Bronze Workflow on Databricks",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=5),
    tags=["ecomflow", "bronze", "databricks"],
) as dag:

    start = EmptyOperator(task_id="start")
    bronze = bronze_task_group()
    end = EmptyOperator(task_id="end")

    start >> bronze >> end
