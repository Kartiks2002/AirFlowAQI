import requests

# Define the function that fetches AQI data
def fetch_data(api_key, lat=18.5204, lon=73.8567, **kwargs):
    url = f"https://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={api_key}"
    
    # Fetch the data
    response = requests.get(url)
    
    # Raise an exception if the request fails
    if response.status_code != 200:
        raise Exception(f"Failed to fetch AQI data: {response.status_code}")

    # Parse the JSON response
    data = response.json()

    # Extract AQI data into a dictionary
    aqi_data = {
        "longitude": lon,
        "latitude": lat,
        "aqi": data["list"][0]["main"]["aqi"],
        "pm2_5": data["list"][0]["components"]["pm2_5"],
        "pm10": data["list"][0]["components"]["pm10"],
        "no2": data["list"][0]["components"]["no2"],
        "o3": data["list"][0]["components"]["o3"],
        "co": data["list"][0]["components"]["co"],
        "so2": data["list"][0]["components"]["so2"],
        "nh3": data["list"][0]["components"]["nh3"],
        "no": data["list"][0]["components"]["no"],
        "timestamp": data["list"][0]["dt"]
    }
    
    # Push the AQI data to XCom for the next task
    ti = kwargs['ti']  # Get task instance from kwargs
    ti.xcom_push(key='aqi_data', value=aqi_data)
    
    # Optionally, return the data for logging or other use
    return aqi_data
