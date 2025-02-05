from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# Import the functions from the different .py files
from fetch_air_quality_data import fetch_air_quality_data
from calculate_aqi import calculate_air_quality_aqi
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
    'air_quality_forecast',
    default_args=default_args,
    description='A simple DAG to fetch air quality data every hour',
    schedule_interval="@hourly",
)

fetch_air_quality_task = PythonOperator(
    task_id='fetch_air_quality_data',
    python_callable=fetch_air_quality_data,
    op_args=[18.5204, 73.8567, API_KEY],
    dag=dag,
)

calculate_aqi_task = PythonOperator(
    task_id='calculate_aqi',
    python_callable=calculate_air_quality_aqi,
    provide_context=True,
    dag=dag,
)

store_data_task = PythonOperator(
    task_id="store_data_task",
    python_callable=store_data,
    provide_context=True,
    dag=dag,
)

fetch_air_quality_task >> calculate_aqi_task >> store_data_task

