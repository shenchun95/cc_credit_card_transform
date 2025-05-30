from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1), # Adjust start date as needed
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pyspark_credit_card_pipeline_dag',
    default_args=default_args,
    description='DAG to run the PySpark credit card processing pipeline',
    schedule_interval='@daily', # Adjust schedule as needed
    catchup=False,
)

submit_pyspark_job = SparkSubmitOperator(
    task_id='submit_credit_card_pyspark_job',
    application='pyspark_scripts/pyspark_credit_card_pipeline.py', # Path to the PySpark script
    conn_id='spark_default',  # Assumes a Spark connection named 'spark_default' is configured in Airflow
    dag=dag,
    # application_args are not strictly needed here as paths are relative in the script,
    # but could be used to pass parameters if the script was designed to accept them.
    # Example: application_args=['--input_path', 'raw_cc_credit_card/raw_cc_credit_card', '--output_path', '...']
)

# Define task dependencies if there were more tasks
# For a single task, this is not strictly necessary
# submit_pyspark_job
