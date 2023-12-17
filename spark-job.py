from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_analysis_dag',
    default_args=default_args,
    description='DAG to submit Spark job and perform analysis',
    schedule_interval='@daily'
)

# Define a Python function for Spark Job
def run_spark_job():
    # Add your Spark job submission code here
    # Example: spark-submit your_spark_job.py

# Define a Python function for Data Analysis
def perform_analysis():
    # Add your data analysis code here
    # Example: simple aggregation, churn-retention analysis, etc.

# Define a Python function for Data Output
def output_data():
    # Add your data output code here
    # Example: console print, CSV, postgres table, etc.

# Task to run Spark Job
submit_spark_job = PythonOperator(
    task_id='submit_spark_job',
    python_callable=run_spark_job,
    dag=dag,
)

# Task to perform Data Analysis
perform_data_analysis = PythonOperator(
    task_id='perform_data_analysis',
    python_callable=perform_analysis,
    dag=dag,
)

# Task to output Data
output_data_task = PythonOperator(
    task_id='output_data',
    python_callable=output_data,
    dag=dag,
)

# Define the execution order of tasks in the DAG
submit_spark_job >> perform_data_analysis >> output_data_task
