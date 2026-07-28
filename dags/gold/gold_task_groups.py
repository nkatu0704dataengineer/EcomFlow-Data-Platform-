"""
Gold Airflow task group
This task group triggers the Databricks Gold Workflow.
Airflow is responsible only for orchestration.

Databricks is responsible for:
    - Workflow execution
    - Task dependency
    - Notebook execution
    - Spark runtime
    - Business query logic

"""
from airflow.decorators import task_group
from dags.common.databricks_task_group import create_gold_workflow_task

@task_group(group_id="gold_task_group")
def gold_task_group():
    """
    Trigger the Databricks Gold Workflow.

    The workflow itself is responsible for executing all Gold notebooks
    according to the dependency graph defined in Databricks.
    """

    create_gold_workflow_task()
