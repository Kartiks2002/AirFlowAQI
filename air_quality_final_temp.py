from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# Import helper functions from the correct paths
from helpers2.transform_aqi_data_task import transform_data
from helpers2.fetch_aqi_data_task import fetch_data
from helpers2.store_data_task import store_data
from helpers2.fetch_last_two_days_task import fetch_last_two_days_data
from helpers2.apply_arima_forecasting_task import apply_arima_forecasting
from helpers2.store_arima_forecasting_task import store_arima_forecasting

# Load environment variables
load_dotenv()

API_KEY = os.getenv("API_KEY")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'air_quality_final_temp',
    default_args=default_args,
    description='A simple DAG to fetch air quality data every hour',
    schedule_interval="@hourly",
    catchup=False
)

# Task 1: Fetch Air Quality Data
fetch_data_task = PythonOperator(
    task_id='fetch_aqi_data_task',
    python_callable=fetch_data,
    op_args=[API_KEY, 18.5204, 73.8567],
    dag=dag,
)

# Task 2: Transform the Data
transform_data_task = PythonOperator(
    task_id="transform_aqi_data_task",
    python_callable=transform_data,
    op_kwargs={},  # Add necessary arguments if needed
    dag=dag,
)

# Task 3: Store the Data in PostgreSQL
store_data_task = PythonOperator(
    task_id="store_data_task",
    python_callable=store_data,
    op_kwargs={},  # Add necessary arguments if needed
    dag=dag,
)

# Task 4: Fetch the Last Two Days of Data
fetch_last_two_days_data_task = PythonOperator(
    task_id="fetch_last_two_days_data_task",
    python_callable=fetch_last_two_days_data,
    op_kwargs={},  # Add necessary arguments if needed
    dag=dag,
)

# Task 5: Apply Time Series Forecasting Using ARIMA
apply_forecasting_task = PythonOperator(
    task_id="apply_arima_forecasting_task",
    python_callable=apply_arima_forecasting,
    op_kwargs={},  # Add necessary arguments if needed
    dag=dag,
)

# Task 6: Store the Forecasted Data
store_arima_forecast_task = PythonOperator(
    task_id="store_arima_forecast_task",
    python_callable=store_arima_forecasting,
    op_kwargs={},  # Add necessary arguments if needed
    dag=dag,
)

# Set up task dependencies
fetch_data_task >> transform_data_task >> store_data_task >> fetch_last_two_days_data_task >> apply_forecasting_task >> store_arima_forecast_task
