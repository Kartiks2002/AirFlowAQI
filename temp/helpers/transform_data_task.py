from datetime import datetime, timezone, timedelta

# Function to convert timestamp to IST
def convert_to_ist(timestamp):
    IST = timezone(timedelta(hours=5, minutes=30))
    return datetime.fromtimestamp(timestamp, IST).isoformat()

# Dummy AQI calculator function
def calculate_aqi(pm2_5, pm10, no2, o3, co, so2, nh3, no):
    #break points
    # Simple dummy formula for AQI calculation (for demonstration purposes)
    aqi = (0.5 * pm2_5 + 0.3 * pm10 + 0.1 * no2 + 0.05 * o3 +
           0.03 * co + 0.02 * so2 + 0.02 * nh3 + 0.01 * no)
    return round(aqi)

# Task to transform AQI data
def transform_data(**kwargs):
    ti = kwargs['ti']
    
    # Fetch data from XCom
    aqi_data = ti.xcom_pull(task_ids='fetch_aqi_data_task', key='aqi_data')
    
    # Convert timestamp to IST
    timestamp_ist = convert_to_ist(aqi_data['timestamp'])
    
    # Calculate AQI using the dummy function
    calculated_aqi = calculate_aqi(
        pm2_5=aqi_data['pm2_5'],
        pm10=aqi_data['pm10'],
        no2=aqi_data['no2'],
        o3=aqi_data['o3'],
        co=aqi_data['co'],
        so2=aqi_data['so2'],
        nh3=aqi_data['nh3'],
        no=aqi_data['no']
    )
    
    # Update the AQI data with the transformed fields
    transformed_data = {
        "longitude": aqi_data['longitude'],
        "latitude": aqi_data['latitude'],
        "aqi": calculated_aqi,  # Use the calculated AQI
        "pm2_5": aqi_data['pm2_5'],
        "pm10": aqi_data['pm10'],
        "no2": aqi_data['no2'],
        "o3": aqi_data['o3'],
        "co": aqi_data['co'],
        "so2": aqi_data['so2'],
        "nh3": aqi_data['nh3'],
        "no": aqi_data['no'],
        "timestamp": aqi_data['timestamp'],  # Keep original timestamp for reference
        "timestamp_ist": timestamp_ist  # Add converted IST timestamp
    }
    
    # Push the transformed data to XCom for the next task
    ti.xcom_push(key='transformed_aqi_data', value=transformed_data)
    
    return transformed_data
