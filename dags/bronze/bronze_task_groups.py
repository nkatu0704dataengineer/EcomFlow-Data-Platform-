"""Bronze Airflow task group.

Orchestrates Bronze dataset processing via Databricks jobs.
Each task submits the bronze_runner.py with dataset-specific parameters.
"""

from airflow.decorators import task_group

from dags.common.databricks_task_group import create_databricks_tasks
from include.config.bronze import BRONZE_DATASETS


@task_group(group_id="bronze_task_group")
def bronze_task_group():
    """Create Bronze task group.
    
    Submits Databricks jobs for each Bronze dataset. Each task will:
    1. Submit bronze_runner.py to Databricks Serverless
    2. Pass dataset parameters as command-line arguments
    3. Wait for job to complete
    4. Return job status
    """
    create_databricks_tasks(BRONZE_DATASETS)
