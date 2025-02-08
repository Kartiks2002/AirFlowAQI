from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

def store_data(**kwargs):
    ti = kwargs["ti"]
    
    # Pull the transformed data from XCom
    data = ti.xcom_pull(task_ids="transform_aqi_data_task", key="transformed_aqi_data")
    
    # Parse timestamp from ISO format
    timestamp = datetime.fromisoformat(data["timestamp_ist"])
    
    # Extract other fields
    longitude = data["longitude"]
    latitude = data["latitude"]
    aqi = data["aqi"]
    pm2_5 = data["pm2_5"]
    pm10 = data["pm10"]
    no2 = data["no2"]
    o3 = data["o3"]
    co = data["co"]
    so2 = data["so2"]
    nh3 = data["nh3"]
    no = data["no"]

    # Set up PostgreSQL connection using PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id="postgres_airflowaqi")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Insert query for all columns
    insert_query = """
        INSERT INTO aqi_data2 
        (timestamp, longitude, latitude, aqi, pm2_5, pm10, no2, o3, co, so2, nh3, no) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Execute the query with the data
    cursor.execute(insert_query, (
        timestamp, longitude, latitude, aqi, pm2_5, pm10, no2, o3, co, so2, nh3, no
    ))
    
    # Commit the transaction
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Data inserted successfully for timestamp: {timestamp}, AQI: {aqi}")


import logging
from datetime import datetime

def store_data(**context):
    # Retrieve the data passed from the previous task
    data = context.get('ti').xcom_pull(task_ids='transform_data_task')
    
    # Log the data for debugging
    logging.info(f"Data received for storing: {data}")
    
    # Check if data is None or if 'timestamp_ist' key is missing
    if data is None or 'timestamp_ist' not in data:
        raise ValueError("Data is None or 'timestamp_ist' key is missing from the data")

    # Convert the timestamp to the correct format
    timestamp = datetime.fromisoformat(data["timestamp_ist"])
    
    # Proceed with storing data in PostgreSQL (omitting actual logic for brevity)
    # ...
