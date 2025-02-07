from airflow.providers.postgres.hooks.postgres import PostgresHook

def store_aqi_data(**kwargs):
    # Pull the transformed AQI data from XCom
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_aqi_task', key='transformed_aqi_data')

    if not transformed_data:
        raise ValueError("No transformed AQI data found.")

    # Set up the Postgres connection using PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')  # Use the connection ID set up in Airflow

    # Define the INSERT query
    insert_query = """
    INSERT INTO aqi_data (
        longitude, latitude, aqi, aqi_category, co, no, no2, o3, so2, pm2_5, 
        pm10, nh3, timestamp, timestamp_ist
    ) VALUES (
        %(longitude)s, %(latitude)s, %(aqi)s, %(aqi_category)s, %(co)s, %(no)s, 
        %(no2)s, %(o3)s, %(so2)s, %(pm2_5)s, %(pm10)s, %(nh3)s, %(timestamp)s, 
        %(timestamp_ist)s
    )
    """
    
    # Execute the query
    postgres_hook.run(insert_query, parameters=transformed_data)

    # Optionally, log success message
    print("AQI data successfully inserted into the PostgreSQL database.")
