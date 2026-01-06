from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor


JOB_FLOW_OVERRIDES = {
    "Name": "user-activity-emr-cluster",
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


SPARK_STEPS = [
    {
        "Name": "Run User Activity ETL",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "s3://emr-aditya-salabh/dataplatforms/main.py",
                "user_activity",
            ],
        },
    }
]



dag = DAG(
    dag_id="user_activity_emr_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
)


create_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    dag=dag,
)

add_step = EmrAddStepsOperator(
    task_id="add_user_activity_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster') }}",
    steps=SPARK_STEPS,
    dag=dag,
)

watch_step = EmrStepSensor(
    task_id="watch_user_activity_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster') }}",
    step_id="{{ task_instance.xcom_pull('add_user_activity_step')[0] }}",
    dag=dag,
)

terminate_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster') }}",
    dag=dag,
)

create_cluster >> add_step >> watch_step >> terminate_cluster
