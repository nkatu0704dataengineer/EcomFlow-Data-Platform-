"""
EcomFlow Master DAG.

Orchestrates the complete EcomFlow data pipeline.

Airflow is responsible only for orchestration.

Execution flow:
    Master DAG
        ↓
    Bronze DAG
        ↓
    Silver DAG
        ↓
    Gold DAG

The existing Bronze, Silver, and Gold DAGs remain independent
and are not modified by this master DAG.
"""

from datetime import timedelta

import pendulum
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from include.config.airflow_config import get_default_args, TIMEZONE


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_ARGS = get_default_args()
START_DATE = pendulum.datetime(2026, 1, 1, tz=TIMEZONE)

BRONZE_DAG_ID = "ecomflow_bronze"
SILVER_DAG_ID = "ecomflow_silver"
GOLD_DAG_ID = "ecomflow_gold"


# ---------------------------------------------------------------------------
# Master DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="ecomflow_master",
    description="Orchestrates the complete EcomFlow Bronze-Silver-Gold pipeline",
    default_args=DEFAULT_ARGS,
    schedule="0 7,19 * * *",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=15),
    tags=["ecomflow", "master", "orchestration"],
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    # -----------------------------------------------------------------------
    # Bronze
    # -----------------------------------------------------------------------

    trigger_bronze = TriggerDagRunOperator(
        task_id="trigger_bronze",
        trigger_dag_id=BRONZE_DAG_ID,
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=False,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # -----------------------------------------------------------------------
    # Silver
    # -----------------------------------------------------------------------

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id=SILVER_DAG_ID,
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=False,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # -----------------------------------------------------------------------
    # Gold
    # -----------------------------------------------------------------------

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold",
        trigger_dag_id=GOLD_DAG_ID,
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=False,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    end = EmptyOperator(
        task_id="end",
    )

    # -----------------------------------------------------------------------
    # Pipeline dependency
    # -----------------------------------------------------------------------

    start >> trigger_bronze >> trigger_silver >> trigger_gold >> end