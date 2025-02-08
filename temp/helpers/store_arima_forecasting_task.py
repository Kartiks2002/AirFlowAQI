from datetime import datetime, timezone, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook

def store_arima_forecasting(**kwargs):
    ti = kwargs['ti']
    
    # Fetch forecast data from the 'apply_forecasting_task' using XCom
    forecast_data = ti.xcom_pull(task_ids="apply_forecasting_task", key='arima_forecast')
    
    print(forecast_data)
    
    if forecast_data:
        # Get the current timestamp for storing the forecasted data
        current_timestamp = datetime.now(timezone.utc)
        
        # Define the interval for each forecast step (e.g., 1 hour)
        forecast_interval = timedelta(hours=1)
        
        # PostgreSQL connection setup
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        # Insert forecasted data into the PostgreSQL table
        insert_query = "INSERT INTO aqi_forecast_data (timestamp, forecast_aqi) VALUES (%s, %s)"
        
        # Loop over the forecasted AQI values and insert each one
        for i, forecast_value in enumerate(forecast_data):
            forecast_timestamp = current_timestamp + (i * forecast_interval)
            cursor.execute(insert_query, (forecast_timestamp, forecast_value))
        
        # Commit the transaction and close the connection
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"Forecasted data stored successfully for {len(forecast_data)} hours.")
    else:
        print("No forecast data available to store.")
