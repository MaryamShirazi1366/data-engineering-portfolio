import os
from airflow import DAG
from datetime import datetime
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from pipelines.reddit_pipelines import reddit_pipeline
from pipelines.Aws_s3_pipeline import upload_s3_pipeline
from airflow.providers.amazon.aws.hooks.s3 import S3Hook






# Default arguments for the DAG
default_args = {
    'owner': 'Marysm Shirazi',
    'start_date': datetime(2025, 2, 1),
}

# Define the DAG
dag = DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['reddit', 'etl', 'pipeline'],
)

# Task to create the output directory
create_output_directory = BashOperator(
    task_id='create_output_directory',
    bash_command='mkdir -p /opt/airflow/data/output',
    dag=dag,
)

# Task to extract data from Reddit
extract = PythonOperator(
    task_id='reddit_extraction',
    python_callable=reddit_pipeline,
    op_kwargs={
        'file_name': 'reddit_pipeline',
        'subreddit_name': 'dataengineering',
        'time_filter': 'day',
        'limit': 100,
    },
    dag=dag,
)

# Task to upload data to S3
upload_s3 = PythonOperator(
    task_id='upload_s3',
    python_callable=upload_s3_pipeline,
    dag=dag,
)

# Set task dependencies
create_output_directory >> extract >> upload_s3