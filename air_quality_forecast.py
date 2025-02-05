from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta, timezone, UTC
import requests
from dotenv import load_dotenv
import os


load_dotenv()

API_KEY = os.getenv("API_KEY")

def calculate_aqi(pollutant_value, breakpoints):
    for i, breakpoint in enumerate(breakpoints):
        if pollutant_value <= breakpoint[1]:
            aqi = ((breakpoint[3] - breakpoint[2]) / (breakpoint[1] - breakpoint[0])) * (pollutant_value - breakpoint[0]) + breakpoint[2]
            return round(aqi)
    return 0 

def get_air_quality_data(lat, lon, api_key):    
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={api_key}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        components = data['list'][0]['components']

        dt = data['list'][0]['dt']
        IST = timezone(timedelta(hours=5, minutes=30))
        timestamp = datetime.fromtimestamp(dt, IST)

        pm25 = components['pm2_5']
        pm10 = components['pm10']
        so2 = components['so2']
        no2 = components['no2']
        nh3 = components['nh3']

        no2_breakpoints = [(0, 40, 0, 50), (41, 80, 51, 100), (81, 180, 101, 200), (181, 280, 201, 300), (281, 400, 301, 400)]
        so2_breakpoints = [(0, 40, 0, 50), (41, 80, 51, 100), (81, 380, 101, 200), (381, 800, 201, 300), (801, 1600, 301, 400)]
        nh3_breakpoints = [(0, 200, 0, 50), (201, 400, 51, 100), (401, 800, 101, 200), (801, 1200, 201, 300), (1201, 1800, 301, 400)]
        pm25_breakpoints = [(0, 30, 0, 50), (31, 60, 51, 100), (61, 90, 101, 200), (91, 120, 201, 300), (121, 250, 301, 400)]
        pm10_breakpoints = [(0, 50, 0, 50), (51, 100, 51, 100), (101, 250, 101, 200), (251, 350, 201, 300), (351, 430, 301, 400)]

        pm25_aqi = calculate_aqi(pm25, pm25_breakpoints)
        pm10_aqi = calculate_aqi(pm10, pm10_breakpoints)
        so2_aqi = calculate_aqi(so2, so2_breakpoints)
        no2_aqi = calculate_aqi(no2, no2_breakpoints)
        nh3_aqi = calculate_aqi(nh3, nh3_breakpoints)

        overall_aqi = max(pm25_aqi, pm10_aqi, no2_aqi, so2_aqi, nh3_aqi)

        return [timestamp.isoformat(), overall_aqi]
            
    else:
        return {"error": "Error fetching data"}
    

def store_data(**kwargs):
    ti = kwargs["ti"]
    
    data = ti.xcom_pull(task_ids="fetch_air_quality_data")
    
    if not data or len(data) != 2:
        raise ValueError("Invalid data format returned from fetch_data_task")

    record_time_str, aqi = data
    timestamp = datetime.fromisoformat(record_time_str)
    
    postgres_hook = PostgresHook(postgres_conn_id="postgres_airflowaqi")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    insert_query = "INSERT INTO aqi_data (timestamp, aqi) VALUES (%s, %s)"
    cursor.execute(insert_query, (timestamp, aqi))
    

    conn.commit()
    cursor.close()
    conn.close()
    
    print("Data inserted successfully:", timestamp, aqi)



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
    python_callable=get_air_quality_data,
    op_args=[18.5204, 73.8567, API_KEY],
    dag=dag,
)

store_data_task = PythonOperator(
    task_id="store_data_task",
    python_callable=store_data,
    provide_context=True,
    dag=dag,
)

fetch_air_quality_task >> store_data_task
