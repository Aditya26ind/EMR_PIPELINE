from datetime import datetime
from airflow import DAG

from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor


# 1️⃣ EMR cluster configuration (simple)
JOB_FLOW_OVERRIDES = {
    "Name": "sales-emr-cluster",
    "ReleaseLabel": "emr-6.6.0",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}


# 2️⃣ Spark step (what EMR should run)
SPARK_STEPS = [
    {
        "Type": "Spark",
        "Name": "Run Sales ETL",
        "ActionOnFailure": "CONTINUE",
        "Args": [
            "s3://emr-aditya-salabh/dataplatforms/main.py",
            "sales"
        ],
    }
]


with DAG(
    dag_id="sales_emr_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Create EMR cluster
    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
    )

    # Add Spark job
    add_step = EmrAddStepsOperator(
        task_id="add_sales_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster') }}",
        steps=SPARK_STEPS,
    )

    # Wait for Spark job to finish
    watch_step = EmrStepSensor(
        task_id="watch_sales_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster') }}",
        step_id="{{ task_instance.xcom_pull('add_sales_step')[0] }}",
    )

    # Terminate EMR cluster
    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster') }}",
    )

    create_cluster >> add_step >> watch_step >> terminate_cluster
