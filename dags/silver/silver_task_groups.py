"""
Silver Airflow task group.

This task group triggers the Databricks Silver Workflow.

Airflow is responsible only for orchestration.

Databricks is responsible for:
    - Workflow execution
    - Task dependency
    - Notebook execution
    - Spark runtime
    - Business logic
"""

from airflow.decorators import task_group
from dags.common.databricks_task_group import create_silver_workflow_task

@task_group(group_id="silver_task_group")
def silver_task_group():
    """
    Trigger the Databricks Silver Workflow.

    The workflow itself is responsible for executing all Silver notebooks
    according to the dependency graph defined in Databricks.
    """

    create_silver_workflow_task()