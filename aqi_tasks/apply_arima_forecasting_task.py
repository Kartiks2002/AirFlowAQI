from statsmodels.tsa.arima.model import ARIMA
import pandas as pd
from datetime import timedelta

def apply_arima_forecasting(**kwargs):
    ti = kwargs['ti']
    
    # Pull the AQI data fetched by the previous task
    data = ti.xcom_pull(key='aqi_data_list', task_ids='fetch_last_two_days_data_task')

    if data and len(data) >= 10:  # Check if there's sufficient data for forecasting
        try:
            # Convert the data to a DataFrame
            df = pd.DataFrame(data, columns=['timestamp', 'aqi'])
            # df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp')
            
            y = df['aqi'].tolist()  # The AQI values to forecast

            # ARIMA model configuration (adjust (p, d, q) as per requirement)
            model = ARIMA(y, order=(5, 1, 0))
            
            # Fit the model
            model_fit = model.fit()

            # Forecast for the next 4 hours (or desired steps)
            forecast = model_fit.forecast(steps=4)

            # Convert forecast to a list to ensure it is JSON serializable
            last_timestamp = pd.to_datetime(df["timestamp"].max())
            future_timestamps = [last_timestamp + timedelta(hours=i) for i in range(1, 5)]
            forecast_list = [
                {
                    "timestamp": ts.strftime('%Y-%m-%d %H:%M:%S%z'),  # Store as string in timezone-aware format
                    "aqi": int(f)
                }
                for ts, f in zip(future_timestamps, forecast)
            ]

            # Push the forecasted data to XCom for further use
            ti.xcom_push(key='arima_forecast', value=forecast_list)

            print(f"ARIMA Forecast for the next 4 hours: {forecast_list}")
            return forecast_list
        
        except Exception as e:
            # Handle any errors in model fitting or forecasting
            print(f"Error while applying ARIMA model: {e}")
            ti.xcom_push(key='arima_forecast', value=None)
            return None
    else:
        print("Not enough data available for forecasting.")
        ti.xcom_push(key='arima_forecast', value=None)
        return None
