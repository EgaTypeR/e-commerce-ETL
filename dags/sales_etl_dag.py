from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'sales_etl_dag',
    default_args=default_args,
    description='ETL process for Amazon Sales data with PostgreSQL',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 10, 25),
    catchup=False,
) as dag:

    # Task to run the PySpark ETL job
    run_etl = BashOperator(
        task_id='run_amazon_sales_etl',
        bash_command='spark-submit ../ETL/sales_etl_process.py'  # Adjust to your script location
    )

run_etl
