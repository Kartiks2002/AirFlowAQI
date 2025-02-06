import requests

# Define the function that fetches the weather data
def fetch_weather_data(api_key, city='Pune', **kwargs):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
    
    # Fetch the data
    response = requests.get(url)
    data = response.json()

    if response.status_code != 200:
        raise Exception(f"Failed to fetch weather data: {response.status_code}")

    # Extract all relevant data into a dictionary
    weather_data = {
        "longitude": data["coord"]["lon"],
        "latitude": data["coord"]["lat"],
        "weather_main": data["weather"][0]["main"],
        "weather_description": data["weather"][0]["description"],
        "temperature_kelvin": data["main"]["temp"],
        "feels_like_kelvin": data["main"]["feels_like"],
        "temp_min_kelvin": data["main"]["temp_min"],
        "temp_max_kelvin": data["main"]["temp_max"],
        "pressure": data["main"]["pressure"],
        "humidity": data["main"]["humidity"],
        "visibility": data["visibility"],
        "wind_speed": data["wind"]["speed"],
        "wind_deg": data["wind"]["deg"],
        "wind_gust": data.get("wind", {}).get("gust", None),
        "clouds": data["clouds"]["all"],
        "timestamp": data["dt"],
        "city": data["name"],
        "country": data["sys"]["country"],
        "sunrise": data["sys"]["sunrise"],
        "sunset": data["sys"]["sunset"],
    }
    
    # Push the weather data to XCom for the next task
    kwargs['ti'].xcom_push(key='weather_data', value=weather_data)
    return weather_data
