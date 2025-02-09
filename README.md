# AirFlowAQI
A pipeline for forecasting air quality (AQI) using Airflow, OpenWeather API, PostgreSQL, and Grafana for real-time data collection, storage, and visualization.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Technologies Used](#technologies-used)
- [Features](#features)
- [Airflow DAG](#airflow-dag)
- [Setup Instructions](#setup-instructions)
  - [1. Clone the Repository](#1-clone-the-repository)
  - [2. Setup PostgreSQL Database](#2-setup-postgresql-database)
  - [3. Setup Airflow](#3-setup-airflow)
  - [4. Configure OpenWeather API](#4-configure-openweather-api)
  - [5. Configure Grafana](#5-configure-grafana)
  - [6. Initialize and Run Airflow](#6-initialize-and-run-airflow)
- [Project Structure](#project-structure)
- [Screenshots](#screenshots)
- [Future Enhancements](#future-enhancements)

## Overview

This project provides a complete pipeline for fetching real-time air quality data from the **OpenWeather API**, processing and storing it in a **PostgreSQL** database, applying **ARIMA** forecasting to predict future AQI, and visualizing the data in **Grafana**.

The pipeline is managed using **Apache Airflow**, which orchestrates the various stages: data fetching, transformation, storage, and forecasting.

## Architecture

1. **Airflow** orchestrates the data pipeline.
2. **OpenWeather API** provides real-time air quality data.
3. **PostgreSQL** stores the transformed AQI data and the forecasting results.
4. **Grafana** visualizes the collected and forecasted AQI data.
5. **ARIMA** (AutoRegressive Integrated Moving Average) is used for forecasting AQI values.

## Technologies Used

- **Airflow**: Task scheduling and orchestration.
- **OpenWeather API**: Real-time air quality data source.
- **PostgreSQL**: Relational database for data storage.
- **Grafana**: Visualization tool for monitoring AQI trends.
- **ARIMA**: Forecasting method for time series data.

## Features

- **Real-Time Data Collection**: Fetches air quality data every hour using the OpenWeather API.
- **Data Transformation**: Converts timestamps to Indian Standard Time (IST) and performs temperature conversions.
- **Forecasting**: Uses ARIMA to predict AQI for the next 4 hours.
- **Storage**: Data is stored in PostgreSQL, including historical and forecasted AQI values.
- **Visualization**: Grafana provides real-time charts and dashboards for AQI trends and predictions.

## Airflow DAG

The DAG (`air_quality_final`) consists of the following tasks:

1. **`fetch_data_task`**: Fetches AQI data from OpenWeather API.
2. **`transform_data_task`**: Transforms the data (converts temperature and timestamps).
3. **`store_data_task`**: Stores the transformed data in PostgreSQL.
4. **`fetch_last_two_days_data_task`**: Fetches the last two days of AQI data for forecasting.
5. **`apply_arima_forecasting_task`**: Applies ARIMA forecasting on the past data.
6. **`store_arima_forecasting_task`**: Stores the forecasted AQI values into PostgreSQL.

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/airflow-aqi-forecast.git
cd airflow-aqi-forecast
```

### 2. Setup PostgreSQL Database
Install PostgreSQL.
Create a new database:
```bash
sudo -u postgres psql
```
```sql
CREATE DATABASE aqi_db;
\c aqi_db
CREATE TABLE aqi_data (
    id SERIAL PRIMARY KEY,
    longitude FLOAT,
    latitude FLOAT,
    aqi INT,
    co FLOAT,
    no FLOAT,
    no2 FLOAT,
    o3 FLOAT,
    so2 FLOAT,
    pm2_5 FLOAT,
    pm10 FLOAT,
    nh3 FLOAT,
    timestamp TIMESTAMP,
    timestamp_ist TIMESTAMP,
    aqi_category VARCHAR(50)
);

CREATE TABLE aqi_forecast_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    forecast_aqi INT
);
```

### 3. Setup Airflow
Install Airflow and required dependencies:
```bash
pip install apache-airflow
pip install apache-airflow-providers-postgres
```
Configure your Airflow airflow.cfg file to include PostgreSQL and OpenWeather connections.
Set up a PostgreSQL connection in Airflow with postgres_conn_id = "my_postgres_connection".
### 4. Configure OpenWeather API
Sign up for an OpenWeather API key here.
Update your Airflow DAG configuration with your API key.
```python
API_KEY = 'your_openweather_api_key'
```
### 5. Configure Grafana
Install Grafana and connect it to your PostgreSQL database.
Set up a dashboard to visualize AQI data and predictions.
### 6. Initialize and Run Airflow
Initialize Airflow’s database:
```bash
airflow db init
```
Start the Airflow web server and scheduler:
```bash
airflow webserver --port 8080
airflow scheduler
```
Trigger the air_quality_final DAG from the Airflow UI.
Project Structure
```bash
.
├── dags/
│   └── air_quality_final.py  # Airflow DAG for AQI pipeline
├── helper/
│   └── transformations.py    # Data transformation helper functions
├── config/
│   └── settings.py           # Configurations for API and database
└── README.md                 # Project documentation
```
### Screenshots
#### AQI Dag
![image](https://github.com/user-attachments/assets/61d1ff1b-47e4-417a-8583-76cf53736c0e)

#### Weather Dag
![image](https://github.com/user-attachments/assets/faaf6358-3f87-4da5-ad45-7efa456a94ef)

#### Grafana Dashboard
![Screenshot (1)](https://github.com/user-attachments/assets/51e0aa60-b243-404c-b0a8-b12d332b219c)



### Future Enhancements
Integrate additional data sources for more accurate AQI forecasting.<br>
Improve the forecasting model (e.g., testing LSTM for time series predictions).<br>
Add a notification system (email/SMS) for alerts when AQI crosses critical levels.<br>


