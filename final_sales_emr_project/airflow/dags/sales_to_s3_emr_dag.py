from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.models import Variable
import pendulum

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,  # No retries to see errors faster
}

@dag(
    dag_id='sales_to_s3_emr_FINAL_WORKING',
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz='UTC'),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=['emr', 'production', 'FINAL']
)
def sales_to_s3_emr():
    """
    FINAL WORKING VERSION
    
    Key fixes:
    1. No upload step (file already in S3)
    2. Fixed bootstrap action (no && operator, simpler command)
    3. Client deploy mode for better error visibility
    4. Proper Python path configuration
    """

    JOB_FLOW_OVERRIDES = {
        'Name': 'sales-books-scraper',
        'ReleaseLabel': 'emr-6.15.0',  # Latest stable EMR version
        'Applications': [{'Name': 'Spark'}, {'Name': 'Hadoop'}],
        
        'Instances': {
            'InstanceGroups': [
                {
                    'Name': 'Master',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1
                },
                {
                    'Name': 'Core',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,  # Keep alive for debugging
            'TerminationProtected': False,
        },
        
        # CRITICAL FIX: Simplified bootstrap - just use pip3 directly
        'BootstrapActions': [
            {
                'Name': 'Install_Python_Packages',
                'ScriptBootstrapAction': {
                    'Path': 's3://elasticmapreduce/bootstrap-actions/run-if',
                    'Args': [
                        'instance.isMaster=true',
                        'sudo',
                        'pip3',
                        'install',
                        'urllib3<2',
                        'requests',
                        'beautifulsoup4',
                        'lxml',
                        '--no-cache-dir'
                    ]
                }
            }
        ],
        
        # Spark configuration for better compatibility
        'Configurations': [
            {
                'Classification': 'spark-env',
                'Configurations': [
                    {
                        'Classification': 'export',
                        'Properties': {
                            'PYSPARK_PYTHON': '/usr/bin/python3',
                            'PYSPARK_DRIVER_PYTHON': '/usr/bin/python3'
                        }
                    }
                ]
            },
            {
                'Classification': 'spark-defaults',
                'Properties': {
                    'spark.pyspark.python': '/usr/bin/python3',
                    'spark.pyspark.driver.python': '/usr/bin/python3'
                }
            }
        ],
        
        'VisibleToAllUsers': True,
        'JobFlowRole': Variable.get('job_flow_role', default_var='EMR_EC2_DefaultRole'),
        'ServiceRole': Variable.get('service_role', default_var='EMR_DefaultRole'),
        'LogUri': 's3://personalbucket20645/logs/',
    }

    create_cluster = EmrCreateJobFlowOperator(
        task_id='create_emr_cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
    )

    # Spark job configuration
    bucket = 'personalbucket20645'
    s3_code_prefix = "s3://personalbucket20645/sales_pipeline/code"
    output_path = f's3://{bucket}/output/books/'

    SPARK_STEP = [
        {
            'Name': 'Books_Scraper_Job',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'client',  # Client mode for better logs
                    '--master', 'yarn',
                    '--conf', 'spark.yarn.submit.waitAppCompletion=true',
                    '--conf', 'spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3',
                    '--py-files', f'{s3_code_prefix}/spark_jobs.zip',
                    f'{s3_code_prefix}/fetch_sales_spark.py',
                    '--start_url', 'https://books.toscrape.com/',
                    '--output_s3', output_path,
                    '--max_pages', '3'  # Start with just 3 pages for testing
                ]
            }
        }
    ]

    add_steps = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=SPARK_STEP,
        aws_conn_id='aws_default',
        do_xcom_push=True
    )

    step_sensor = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
)


    terminate = EmrTerminateJobFlowOperator(
        task_id='terminate_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        trigger_rule='all_done',
    )

    create_cluster >> add_steps >> step_sensor >> terminate

dag = sales_to_s3_emr()