"""
Bronze Airflow task group.

This task group triggers the Databricks Bronze Workflow.

Airflow is responsible only for orchestration.

Databricks is responsible for:
    - Workflow execution
    - Task dependency
    - Notebook execution
    - Spark runtime
    - Business logic
"""

from airflow.decorators import task_group

from dags.common.databricks_task_group import create_bronze_workflow_task


@task_group(group_id="bronze_task_group")
def bronze_task_group():
    """
    Trigger the Databricks Bronze Workflow.

    The workflow itself is responsible for executing all Bronze notebooks
    according to the dependency graph defined in Databricks.
    """

    create_bronze_workflow_task()