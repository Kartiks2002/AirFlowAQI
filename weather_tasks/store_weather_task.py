from airflow.providers.postgres.hooks.postgres import PostgresHook

def store_weather_data(**kwargs):
    # Pull the transformed weather data from XCom
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_weather_data', key='transformed_weather_data')

    if not transformed_data:
        raise ValueError("No transformed weather data found.")

    # Set up the Postgres connection using PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')  # Use the connection ID set up in Airflow
    
    # Define the INSERT query
    insert_query = """
    INSERT INTO weather_data (
        longitude, latitude, weather_main, weather_description, temperature_kelvin, 
        feels_like_kelvin, temp_min_kelvin, temp_max_kelvin, pressure, humidity, 
        visibility, wind_speed, wind_deg, wind_gust, clouds, timestamp, city, country, 
        temperature_celsius, feels_like_celsius, temp_min_celsius, temp_max_celsius, timestamp_ist
    ) VALUES (
        %(longitude)s, %(latitude)s, %(weather_main)s, %(weather_description)s, %(temperature_kelvin)s, 
        %(feels_like_kelvin)s, %(temp_min_kelvin)s, %(temp_max_kelvin)s, %(pressure)s, %(humidity)s, 
        %(visibility)s, %(wind_speed)s, %(wind_deg)s, %(wind_gust)s, %(clouds)s, %(timestamp)s, 
        %(city)s, %(country)s, %(temperature_celsius)s, %(feels_like_celsius)s, %(temp_min_celsius)s, 
        %(temp_max_celsius)s, %(timestamp_ist)s
    )
    """
    
    # Execute the query
    postgres_hook.run(insert_query, parameters=transformed_data)

    # Optionally, log success message
    print("Weather data successfully inserted into the PostgreSQL database.")
