from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ETL.sales_etl_process import main as run_etl_process  # Adjust the import based on your function

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

    # Task to run the ETL job using PythonOperator
    run_etl = PythonOperator(
        task_id='run_amazon_sales_etl',
        python_callable=run_etl_process,  # Your ETL main function
        dag=dag
    )

run_etl
