# sales_emr_taskflow_dag.py
import pendulum
from airflow.sdk import dag, task
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

# ====== Static / configuration (edit these values as needed) ======
JOB_FLOW_OVERRIDES = {
    "Name": "sales-emr-cluster",
    "ReleaseLabel": "emr-6.6.0",
    "LogUri": "s3://emr-aditya-salabh/emr-logs/",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {"Name": "Master", "Market": "ON_DEMAND", "InstanceRole": "MASTER", "InstanceType": "m5.xlarge", "InstanceCount": 1},
            {"Name": "Core",   "Market": "ON_DEMAND", "InstanceRole": "CORE",   "InstanceType": "m5.xlarge", "InstanceCount": 2},
        ],
        # Set True temporarily for debugging; set False to auto-terminate.
        "KeepJobFlowAliveWhenNoSteps": True,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

# If you packaged your code into a zip uploaded to S3, set it here (optional).
# If not using a zip, remove the --py-files entry in prepare_args below.
DEFAULT_PY_FILES = "s3://emr-aditya-salabh/maindir/dataplatforms.zip"
MAIN_PY = "s3://emr-aditya-salabh/dataplatforms/main.py"
PIPELINE_ARG = "sales"


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["emr", "spark", "sales"],
)
def sales_emr_pipeline():
    """
    TaskFlow-style DAG to create an EMR cluster, submit a spark-submit step, wait, and terminate.
    """

    @task()
    def prepare_spark_step_args() -> list:
        """
        Return the args list for the HadoopJarStep -> command-runner.jar spark-submit invocation.
        Modify to include --py-files if you packaged your code.
        """
        args = [
            "spark-submit",
            "--deploy-mode",
            "cluster",
        ]
        # Optional: include packaged python zip for your project
        if DEFAULT_PY_FILES:
            args += ["--py-files", DEFAULT_PY_FILES]

        args += [
            MAIN_PY,
            PIPELINE_ARG,
        ]
        return args

    # Prepare step structure in the boto3/EMR accepted format
    @task()
    def build_spark_steps(args: list) -> list:
        step = {
            "Name": "Run Sales ETL",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": args,
            },
        }
        return [step]

    # Prepare cluster (operator)
    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
    )

    # Build step args and steps via TaskFlow tasks
    args = prepare_spark_step_args()
    steps = build_spark_steps(args)

    # Add steps (operator). The job_flow_id will be XCom from create_cluster (Airflow handles that).
    add_step = EmrAddStepsOperator(
        task_id="add_sales_step",
        job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=steps,
    )

    # Wait for the step to finish
    watch_step = EmrStepSensor(
        task_id="watch_sales_step",
        job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ ti.xcom_pull(task_ids='add_sales_step', key='return_value')[0] }}",
        poke_interval=30,
        timeout=60 * 60 * 4,  # 4 hours max (was 3)
    )

    # Terminate the cluster
    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    )

    # Wiring using TaskFlow-style "call ordering" mixed with operators
    create_cluster >> add_step >> watch_step >> terminate_cluster


sales_emr_pipeline()
