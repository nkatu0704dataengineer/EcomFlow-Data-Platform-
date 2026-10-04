from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'stockflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# The environment variables needed for StockFlow scripts to run inside Airflow Docker
# Using direct container names since Airflow is now attached to stockflow_net
docker_env = {
    "POSTGRES_HOST": "stockflow-postgres",
    "POSTGRES_PORT": "5432",
    "KAFKA_BOOTSTRAP_SERVERS": "stockflow-kafka:29092",
}

with DAG(
    'stockflow_daily_batch',
    default_args=default_args,
    description='Runs Yahoo Finance daily batch and 1m healer for StockFlow',
    schedule='0 2 * * *', # 2:00 AM every day
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['stockflow'],
) as dag1:

    yahoo_batch_task = BashOperator(
        task_id='run_yahoo_batch',
        bash_command='cd /usr/local/airflow/include/stockflow && python -m src.ingestion.yahoo_batch',
        env=docker_env,
        append_env=True
    )

with DAG(
    'stockflow_1m_healer',
    default_args=default_args,
    description='Runs the 1m healer script every 4 hours',
    schedule='0 */8 * * *', # Every 8 hours
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['stockflow'],
) as dag2:

    patch_missing_task = BashOperator(
        task_id='run_patch_1m',
        bash_command='cd /usr/local/airflow/include/stockflow && python -m src.ingestion.patch_missing_1m',
        env=docker_env,
        append_env=True
    )
