import os
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from weather_tasks.fetch_weather_task import fetch_weather_data
from weather_tasks.transform_weather_task import transform_weather_data
from weather_tasks.store_weather_task import store_weather_data 

# Load environment variables from the .env file
load_dotenv()

# Define the DAG
with DAG(
    dag_id='weather_dag',
    schedule_interval='*/10 * * * *', 
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Fetch the API key from environment variables
    api_key = os.getenv('API_KEY')

    # Task 1: Fetch Weather Data
    fetch_weather_task = PythonOperator(
        task_id='fetch_weather_task',
        python_callable=fetch_weather_data,
        op_kwargs={'api_key': api_key},  # Pass the API key here
        provide_context=True,
    )

    # Task 2: Transform Weather Data
    transform_weather_task = PythonOperator(
        task_id='transform_weather_task',
        python_callable=transform_weather_data,
        provide_context=True,  # To access the XCom from the previous task
    )

    # Task 3: Store Weather Data into PostgreSQL
    store_weather_task = PythonOperator(
        task_id='store_weather_task',
        python_callable=store_weather_data,
        provide_context=True,  # To access the XCom from the previous task
    )

    # Task dependencies
    fetch_weather_task >> transform_weather_task >> store_weather_task