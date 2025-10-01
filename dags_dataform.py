from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "eng-archery-473819-h2"
PROJECT_NUMBER = "1515952121"
REGION = "southamerica-west1"
REPOSITORY_ID = "dataform-repository"
WORKSPACE_ID = "repository-etl"

INCLUDED_TAGS = ["external", "raw", "staging", "dim", "fact", "gold", "assert_crit"]

with DAG(
    dag_id="dataform_dags",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=INCLUDED_TAGS,
) as dag:
    
    start_task = DummyOperator(
        task_id='start_task',
    ) 
    
    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create_compilation_result",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "git_commitish": "workspace-hites",
            "code_compilation_config": { 
                "default_database": PROJECT_ID,
                "default_schema": "stg",
                "default_location": REGION},
           # "workspace": (
           #     f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY_ID}/"
           #     f"workspaces/{WORKSPACE_ID}"
           # ),
        },
    )
    
    worflow_external_raw = DataformCreateWorkflowInvocationOperator(
        task_id='worflow_external_raw',
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}",
            "invocation_config": {
            "included_tags": ["external", "raw"],
            "transitive_dependencies_included": True
            },
        },
    )
    
    worflow_staging_stg = DataformCreateWorkflowInvocationOperator(
        task_id='worflow_staging_stg',
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}",
            "invocation_config": {
            "included_tags": ["staging"],
            "transitive_dependencies_included": True
            },
        },
    )
    
    worflow_fact_gold = DataformCreateWorkflowInvocationOperator(
        task_id='worflow_fact_gold',
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}",
            "invocation_config": {
            "included_tags": ["fact", "gold"],
            "transitive_dependencies_included": True
            },
        },
    )
    
    worflow_dim_gold = DataformCreateWorkflowInvocationOperator(
        task_id='worflow_dim_gold',
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}",
            "invocation_config": {
            "included_tags": ["dim", "gold"],
            "transitive_dependencies_included": True
            },
        },
    )
    
    assertions_workflow = DataformCreateWorkflowInvocationOperator(
        task_id='assertions_workflow',
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}",
            "invocation_config": {
            "included_tags": ["assert_crit"],
            "transitive_dependencies_included": True
            },
        },
    )
    
    end_task = DummyOperator(
        task_id='end_task',
    )


start_task >> create_compilation_result >> worflow_external_raw >> worflow_staging_stg >> [worflow_fact_gold, worflow_dim_gold] >> assertions_workflow >> end_task
