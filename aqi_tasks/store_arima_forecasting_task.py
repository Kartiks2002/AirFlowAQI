from datetime import datetime, timezone, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

def store_arima_forecasting(**kwargs):
    ti = kwargs['ti']
    
    # Fetch forecast data from the 'apply_forecasting_task' using XCom
    forecast_data = ti.xcom_pull(task_ids="apply_arima_forecasting_task", key='arima_forecast')
    print(forecast_data)
    
    if forecast_data:
        
        # PostgreSQL connection setup
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        # Truncate the table before inserting new data
        truncate_query = "TRUNCATE TABLE aqi_forecast_data"
        cursor.execute(truncate_query)
        print("Table truncated successfully.")

        # Insert forecasted data into the PostgreSQL table
        insert_query = "INSERT INTO aqi_forecast_data (timestamp, forecast_aqi) VALUES (%s, %s)"
        
        data = [(pd.to_datetime(f["timestamp"]), f["aqi"]) for f in forecast_data]

        # Loop over the forecasted AQI values and insert each one
        cursor.executemany(insert_query, data)

        # Commit the transaction and close the connection
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"Forecasted data stored successfully for {len(forecast_data)} hours.")
    else:
        print("No forecast data available to store.")
