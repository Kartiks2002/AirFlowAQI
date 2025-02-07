from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from aqi_tasks.fetch_aqi_task import fetch_aqi_data
from aqi_tasks.transform_aqi_task import transform_aqi_data
from aqi_tasks.store_aqi_task import store_aqi_data
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Define the DAG
with DAG(
    dag_id='my_aqi_dag',
    schedule_interval='0 * * * *',  # Run every hour
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Fetch the API key from environment variables
    api_key = os.getenv('API_KEY')

    # Define the longitude and latitude for Pune
    lon, lat = 73.8567, 18.5204

    # Task 1: Fetch AQI Data
    fetch_aqi_task = PythonOperator(
        task_id='fetch_aqi_task',
        python_callable=fetch_aqi_data,
        op_kwargs={'api_key': api_key, 'lon': lon, 'lat': lat},
        provide_context=True,
    )

    # Task 2: Transform AQI Data
    transform_aqi_task = PythonOperator(
        task_id='transform_aqi_task',
        python_callable=transform_aqi_data,
        provide_context=True,
    )

    # Task 3: Store AQI Data into PostgreSQL
    store_aqi_task = PythonOperator(
        task_id='store_aqi_task',
        python_callable=store_aqi_data,
        provide_context=True,
    )

    # Task dependencies
    fetch_aqi_task >> transform_aqi_task >> store_aqi_task 