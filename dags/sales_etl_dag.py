from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor


# -------------------------
# EMR cluster configuration
# -------------------------
JOB_FLOW_OVERRIDES = {
    "Name": "sales-emr-cluster",
    "ReleaseLabel": "emr-6.6.0",
    "LogUri": "s3://emr-aditya-salabh/emr-logs/",
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


# -------------------------
# Spark step configuration
# -------------------------
SPARK_STEPS = [
    {
        "Name": "Run Sales ETL",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "--py-files",
                "s3://emr-aditya-salabh/dataplatforms/dataplatforms.zip",
                "s3://emr-aditya-salabh/dataplatforms/main.py",
                "sales",
            ],
        },
    }
]



# -------------------------
# DAG definition (EXPLICIT)
# -------------------------
dag = DAG(
    dag_id="sales_emr_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
)


# -------------------------
# Tasks
# -------------------------
create_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    dag=dag,
)

add_step = EmrAddStepsOperator(
    task_id="add_sales_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster') }}",
    steps=SPARK_STEPS,
    dag=dag,
)

watch_step = EmrStepSensor(
    task_id="watch_sales_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster') }}",
    step_id="{{ task_instance.xcom_pull('add_sales_step')[0] }}",
    dag=dag,
)

terminate_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster') }}",
    dag=dag,
)

# -------------------------
# Task dependencies
# -------------------------
create_cluster >> add_step >> watch_step >> terminate_cluster
