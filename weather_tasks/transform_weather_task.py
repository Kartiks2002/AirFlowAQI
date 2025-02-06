from datetime import datetime, timezone, timedelta

def transform_weather_data(**kwargs):
    # Pull the weather data from XCom
    ti = kwargs['ti']
    weather_data = ti.xcom_pull(task_ids='fetch_weather_task', key='weather_data')

    if not weather_data:
        raise ValueError("No weather data found from previous task.")

    # Convert temperature fields from Kelvin to Celsius and round to 2 decimal places
    weather_data['temperature_celsius'] = round(weather_data['temperature_kelvin'] - 273.15, 2)
    weather_data['feels_like_celsius'] = round(weather_data['feels_like_kelvin'] - 273.15, 2)
    weather_data['temp_min_celsius'] = round(weather_data['temp_min_kelvin'] - 273.15, 2)
    weather_data['temp_max_celsius'] = round(weather_data['temp_max_kelvin'] - 273.15, 2)

    # Convert the timestamp to Indian date time (IST: UTC+5:30)
    ist_offset = timedelta(hours=5, minutes=30)
    weather_data['timestamp_ist'] = (datetime.fromtimestamp(weather_data['timestamp'], tz=timezone.utc) + ist_offset).strftime('%Y-%m-%d %H:%M:%S')

    # Pass the complete weather data, with transformations, forward
    ti.xcom_push(key='transformed_weather_data', value=weather_data)
    return weather_data