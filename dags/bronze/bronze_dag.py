"""EcomFlow Bronze DAG.

Orchestrates the Bronze layer by triggering Databricks Bronze Jobs.

Airflow is responsible only for orchestration.
Spark execution and business logic are delegated to Databricks.
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
    description="Orchestrate EcomFlow Bronze datasets via Databricks Serverless",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    tags=["ecomflow", "bronze", "databricks"],
) as dag:

    start = EmptyOperator(task_id="start")
    bronze = bronze_task_group()
    end = EmptyOperator(task_id="end")

    start >> bronze >> end
