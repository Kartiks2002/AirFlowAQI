def calculate_aqi(pollutant_value, breakpoints):
    for i, breakpoint in enumerate(breakpoints):
        if pollutant_value <= breakpoint[1]:
            aqi = ((breakpoint[3] - breakpoint[2]) / (breakpoint[1] - breakpoint[0])) * (pollutant_value - breakpoint[0]) + breakpoint[2]
            return round(aqi)
    return 0 

def transform_data(**kwargs):
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="fetch_data_task")

    # if "error" in data:
    #     raise ValueError("Error or invalid data returned from fetch_air_quality_data")
    
    pm25 = data['pm25']
    pm10 = data['pm10']
    so2 = data['so2']
    no2 = data['no2']
    nh3 = data['nh3']

    pm25_breakpoints = [(0, 30, 0, 50), (31, 60, 51, 100), (61, 90, 101, 200), (91, 120, 201, 300), (121, 250, 301, 400)]
    pm10_breakpoints = [(0, 50, 0, 50), (51, 100, 51, 100), (101, 250, 101, 200), (251, 350, 201, 300), (351, 430, 301, 400)]
    so2_breakpoints = [(0, 40, 0, 50), (41, 80, 51, 100), (81, 380, 101, 200), (381, 800, 201, 300), (801, 1600, 301, 400)]
    no2_breakpoints = [(0, 40, 0, 50), (41, 80, 51, 100), (81, 180, 101, 200), (181, 280, 201, 300), (281, 400, 301, 400)]
    nh3_breakpoints = [(0, 200, 0, 50), (201, 400, 51, 100), (401, 800, 101, 200), (801, 1200, 201, 300), (1201, 1800, 301, 400)]

    pm25_aqi = calculate_aqi(pm25, pm25_breakpoints)
    pm10_aqi = calculate_aqi(pm10, pm10_breakpoints)
    so2_aqi = calculate_aqi(so2, so2_breakpoints)
    no2_aqi = calculate_aqi(no2, no2_breakpoints)
    nh3_aqi = calculate_aqi(nh3, nh3_breakpoints)

    overall_aqi = max(pm25_aqi, pm10_aqi, no2_aqi, so2_aqi, nh3_aqi)

    return {
        "timestamp": data["timestamp"],
        "aqi": overall_aqi
    }
