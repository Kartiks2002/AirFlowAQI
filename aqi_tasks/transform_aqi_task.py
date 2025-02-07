from datetime import datetime, timezone, timedelta

# Define the function that transforms the AQI data
def transform_aqi_data(**kwargs):
    # Pull the AQI data from XCom
    ti = kwargs['ti']
    aqi_data = ti.xcom_pull(task_ids='fetch_aqi_task', key='aqi_data')

    if not aqi_data:
        raise ValueError("No AQI data found from previous task.")

    # Convert the timestamp to Indian date time (IST: UTC+5:30)
    ist_offset = timedelta(hours=5, minutes=30)
    aqi_data['timestamp_ist'] = (datetime.fromtimestamp(aqi_data['timestamp'], tz=timezone.utc) + ist_offset).strftime('%Y-%m-%d %H:%M:%S')

    # Normalize AQI value to textual category
    aqi_scale = {
        1: "Good",
        2: "Fair",
        3: "Moderate",
        4: "Poor",
        5: "Very Poor"
    }
    aqi_data['aqi_category'] = aqi_scale.get(aqi_data['aqi'], "Unknown")

    # Push the transformed AQI data to XCom for the next task
    ti.xcom_push(key='transformed_aqi_data', value=aqi_data)
    # return aqi_data
