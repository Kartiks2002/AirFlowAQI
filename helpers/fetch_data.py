import requests
from datetime import datetime, timezone, timedelta

def fetch_data(lat, lon, api_key):
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={api_key}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        components = data['list'][0]['components']

        dt = data['list'][0]['dt']
        IST = timezone(timedelta(hours=5, minutes=30))
        timestamp = datetime.fromtimestamp(dt, IST)

        pm25 = components['pm2_5']
        pm10 = components['pm10']
        so2 = components['so2']
        no2 = components['no2']
        nh3 = components['nh3']

        return {
            "timestamp": timestamp.isoformat(),
            "pm25": pm25,
            "pm10": pm10,
            "so2": so2,
            "no2": no2,
            "nh3": nh3
        }
    else:
        return {"error": "Error fetching data"}
