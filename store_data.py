from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

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

