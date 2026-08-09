"""
EcomFlow Gold DAG
Triggers the Databricks Gold Workflow.

Airflow is responsible only for orchestration.

Databricks is responsible for:
    - Workflow execution
    - Task dependency
    - Notebook execution
    - Spark runtime
    - Business query logic
"""

from datetime import timedelta

import pendulum
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator

from dags.gold.gold_task_groups import gold_task_group
from include.config.airflow_config import get_default_args, TIMEZONE

DEFAULT_ARGS = get_default_args()
START_DATE = pendulum.datetime(2026, 1, 1, tz=TIMEZONE)

with DAG(
    dag_id="ecomflow_gold",
    description="Trigger the EcomFlow Gold Workflow on Databricks",
    default_args=get_default_args(),
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=5),
    tags=["ecomflow", "gold", "databricks"],
) as dag:

    start = EmptyOperator(task_id="start")
    gold = gold_task_group()
    end = EmptyOperator(task_id="end")

    start >> gold >> end
