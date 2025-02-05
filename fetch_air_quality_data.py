import requests
from datetime import datetime, timedelta, timezone

def fetch_air_quality_data(lat, lon, api_key):
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={api_key}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        components = data['list'][0]['components']

        dt = data['list'][0]['dt']
        IST = timezone(timedelta(hours=5, minutes=30))
        timestamp = datetime.fromtimestamp(dt, IST)

        return {
            "timestamp": timestamp.isoformat(),
            "components": components
        }
    else:
        return {"error": "Error fetching data"}

