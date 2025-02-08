from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

def fetch_last_two_days_data(**kwargs):
    try:
        # Establish Postgres connection using Airflow's PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id="postgres_airflowaqi")
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        # Calculate the timestamp for two days ago (considering IST)
        two_days_ago = datetime.now() - timedelta(days=2)

        # Query to fetch AQI data for the last two days
        fetch_query = """
        SELECT timestamp, aqi
        FROM aqi_data
        WHERE timestamp >= %s
        ORDER BY timestamp ASC;
        """
        
        cursor.execute(fetch_query, (two_days_ago,))
        result = cursor.fetchall()

        # Ensure the cursor is closed after fetching the data
        cursor.close()
        conn.close()

        # Check if the result is empty, and handle it
        if not result:
            raise ValueError("No data found for the last two days.")

        # Prepare data for XCom
        aqi_data_ist = []
        for row in result:
            timestamp_ist = row[0]  # Timestamp already in IST
            aqi_value = row[1]
            
            # Append the data as it is (since it's already in IST)
            aqi_data_ist.append((timestamp_ist.isoformat(), aqi_value))

        # Push the fetched data to XCom for downstream tasks
        kwargs['ti'].xcom_push(key='aqi_data', value=aqi_data_ist)

        print(f"Successfully fetched {len(aqi_data_ist)} records.")

    except Exception as e:
        # Handle exceptions (like database connection errors, etc.)
        print(f"Error occurred while fetching AQI data: {e}")
        raise
