from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, UTC
from dotenv import load_dotenv
import os

from transform_data import transform_data
from fetch_data import fetch_data
from store_data import store_data

load_dotenv()

API_KEY = os.getenv("API_KEY")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    'air_quality_final',
    default_args=default_args,
    description='A simple DAG to fetch air quality data every hour',
    schedule_interval="@hourly",
    catchup=False   
)


fetch_data_task = PythonOperator(
    task_id='fetch_data_task',
    python_callable=fetch_data,
    op_args=[18.5204, 73.8567, API_KEY],
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id="transform_data_task",
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

store_data_task = PythonOperator(
    task_id="store_data_task",
    python_callable=store_data,
    provide_context=True,   
    dag=dag,
)

fetch_data_task >> transform_data_task >> store_data_task 
