from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests


# Function to calculate AQI based on pollutant value
def calculate_aqi(pollutant_value, breakpoints):
    for i, breakpoint in enumerate(breakpoints):
        if pollutant_value <= breakpoint[1]:
            aqi = (breakpoint[3] - breakpoint[2]) / (breakpoint[1] - breakpoint[0]) * (
                        pollutant_value - breakpoint[0]) + breakpoint[2]
            return round(aqi)
    return 0


# Function to get air quality data and calculate AQI
def get_air_quality_data(lat, lon, api_key):
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={api_key}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        components = data['list'][0]['components']

        pm25 = components['pm2_5']
        pm10 = components['pm10']
        o3 = components['o3']
        so2 = components['so2']
        no2 = components['no2']
        co = components['co']

        # Define the breakpoints for AQI calculation (for simplicity, add more if needed)
        pm25_breakpoints = [(0, 12, 0, 50), (12, 35.4, 51, 100), (35.4, 55.4, 101, 150)]
        pm10_breakpoints = [(0, 54, 0, 50), (54, 154, 51, 100), (154, 254, 101, 150)]
        o3_breakpoints = [(0, 54, 0, 50), (54, 104, 51, 100), (104, 254, 101, 150)]
        so2_breakpoints = [(0, 35, 0, 50), (35, 75, 51, 100), (75, 185, 101, 150)]
        no2_breakpoints = [(0, 53, 0, 50), (53, 100, 51, 100), (100, 360, 101, 150)]
        co_breakpoints = [(0, 4.4, 0, 50), (4.4, 9.4, 51, 100), (9.4, 12.4, 101, 150)]

        # Calculate AQI for each pollutant
        pm25_aqi = calculate_aqi(pm25, pm25_breakpoints)
        pm10_aqi = calculate_aqi(pm10, pm10_breakpoints)
        o3_aqi = calculate_aqi(o3, o3_breakpoints)
        so2_aqi = calculate_aqi(so2, so2_breakpoints)
        no2_aqi = calculate_aqi(no2, no2_breakpoints)
        co_aqi = calculate_aqi(co, co_breakpoints)

        # Find the maximum AQI
        overall_aqi = max(pm25_aqi, pm10_aqi, o3_aqi, so2_aqi, no2_aqi, co_aqi)

        return {
            "pm25_aqi": pm25_aqi,
            "pm10_aqi": pm10_aqi,
            "o3_aqi": o3_aqi,
            "so2_aqi": so2_aqi,
            "no2_aqi": no2_aqi,
            "co_aqi": co_aqi,
            "overall_aqi": overall_aqi
        }
    else:
        return {"error": "Error fetching data"}


# Airflow default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),  # Set the start date to when you want the job to start
}

# Define the DAG
dag = DAG(
    'air_quality_forecast',  # DAG name
    default_args=default_args,
    description='A simple DAG to fetch air quality data every hour',
    schedule_interval="@hourly",  # Schedule the DAG to run every hour
)

# Task to get air quality data and process it
fetch_air_quality_task = PythonOperator(
    task_id='fetch_air_quality_data',
    python_callable=get_air_quality_data,
    op_args=[18.5204, 73.8567, '01ab2a8e148f687fc8a8f2444ba42748'],  # Example: Pune coordinates and API key
    dag=dag,
)

# Task dependencies (if you have other tasks, you can set them here)
fetch_air_quality_task
