from datetime import datetime, timezone, timedelta

# Function to convert timestamp to IST
def convert_to_ist(timestamp):
    IST = timezone(timedelta(hours=5, minutes=30))
    return datetime.fromtimestamp(timestamp, IST).isoformat()

def individual_aqi(pollutant_value, breakpoints):
    for i, breakpoint in enumerate(breakpoints):
        if pollutant_value <= breakpoint[1]:
            aqi = ((breakpoint[3] - breakpoint[2]) / (breakpoint[1] - breakpoint[0])) * (pollutant_value - breakpoint[0]) + breakpoint[2]
            return round(aqi)
    return 0 

# Dummy AQI calculator function
def calculate_aqi(pm2_5, pm10, no2, o3, co, so2, nh3, no):
    pm25_breakpoints = [(0, 30, 0, 50), (31, 60, 51, 100), (61, 90, 101, 200), (91, 120, 201, 300), (121, 250, 301, 400)]
    pm10_breakpoints = [(0, 50, 0, 50), (51, 100, 51, 100), (101, 250, 101, 200), (251, 350, 201, 300), (351, 430, 301, 400)]
    so2_breakpoints = [(0, 40, 0, 50), (41, 80, 51, 100), (81, 380, 101, 200), (381, 800, 201, 300), (801, 1600, 301, 400)]
    no2_breakpoints = [(0, 40, 0, 50), (41, 80, 51, 100), (81, 180, 101, 200), (181, 280, 201, 300), (281, 400, 301, 400)]
    nh3_breakpoints = [(0, 200, 0, 50), (201, 400, 51, 100), (401, 800, 101, 200), (801, 1200, 201, 300), (1201, 1800, 301, 400)]
    # Simple dummy formula for AQI calculation (for demonstration purposes)
    pm25_aqi = individual_aqi(pm2_5, pm25_breakpoints)
    pm10_aqi = individual_aqi(pm10, pm10_breakpoints)
    so2_aqi = individual_aqi(so2, so2_breakpoints)
    no2_aqi = individual_aqi(no2, no2_breakpoints)
    nh3_aqi = individual_aqi(nh3, nh3_breakpoints)

    overall_aqi = max(pm25_aqi, pm10_aqi, no2_aqi, so2_aqi, nh3_aqi)
    overall_aqi = 100 + (overall_aqi - 100)/300 * 50

    return round(overall_aqi)

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
