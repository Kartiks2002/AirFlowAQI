from datetime import datetime, timezone, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook

def store_data(**kwargs):
    ti = kwargs["ti"]
    
    data = ti.xcom_pull(task_ids="transform_data_task")
    
    # if not data or len(data) != 2:
    #     raise ValueError("Invalid data format returned from fetch_data_task")

    timestamp = data["timestamp"]
    timestamp = datetime.fromisoformat(timestamp)
    aqi = data["aqi"]
    
    
    postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    insert_query = "INSERT INTO aqi_data2 (timestamp, aqi) VALUES (%s, %s)"
    cursor.execute(insert_query, (timestamp, aqi))
    

    conn.commit()
    cursor.close()
    conn.close()
    
    print("Data inserted successfully:", timestamp, aqi)