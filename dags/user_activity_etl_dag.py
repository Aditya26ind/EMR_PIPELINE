# user_activity_emr_taskflow_dag.py
import pendulum
from airflow.sdk import dag, task
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
            {"Name": "Master", "Market": "ON_DEMAND", "InstanceRole": "MASTER", "InstanceType": "m5.xlarge", "InstanceCount": 1},
            {"Name": "Core",   "Market": "ON_DEMAND", "InstanceRole": "CORE",   "InstanceType": "m5.xlarge", "InstanceCount": 2},
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

DEFAULT_PY_FILES = "s3://emr-aditya-salabh/dataplatforms/dataplatforms.zip"
MAIN_PY = "s3://emr-aditya-salabh/dataplatforms/main.py"
PIPELINE_ARG = "user_activity"


@dag(
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["emr", "spark", "user_activity"],
)
def user_activity_emr_pipeline():
    """
    TaskFlow-style DAG for the user_activity pipeline.
    """

    @task()
    def prepare_spark_step_args() -> list:
        args = ["spark-submit", "--deploy-mode", "cluster"]
        if DEFAULT_PY_FILES:
            args += ["--py-files", DEFAULT_PY_FILES]
        args += [MAIN_PY, PIPELINE_ARG]
        return args

    @task()
    def build_spark_steps(args: list) -> list:
        step = {
            "Name": "Run User Activity ETL",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": args,
            },
        }
        return [step]

    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
    )

    args = prepare_spark_step_args()
    steps = build_spark_steps(args)

    add_step = EmrAddStepsOperator(
        task_id="add_user_activity_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster') }}",
        steps=steps,
    )

    watch_step = EmrStepSensor(
        task_id="watch_user_activity_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster') }}",
        step_id="{{ task_instance.xcom_pull('add_user_activity_step')[0] }}",
        poke_interval=30,
        timeout=60 * 60 * 2,  # 2 hours
    )

    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster') }}",
    )

    create_cluster >> add_step >> watch_step >> terminate_cluster


user_activity_emr_pipeline()
