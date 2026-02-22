from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import logging
import os

def fetch_weather_data():
    api_endpoint = "https://api.openweathermap.org/data/2.5/weather"
    api_key = "bafeb0d911dab09870265df0124550ab"
    lat = 45.0760
    lon = 72.8777

    logging.info(f"Fetching weather data for coordinates: lat={lat}, lon={lon}")
    response = requests.get(f"{api_endpoint}?lat={lat}&lon={lon}&appid={api_key}")

    if response.status_code == 200:
        data = response.json()
        logging.info("Weather data fetched successfully")
        return data
    else:
        logging.error(f"Failed to fetch weather data: {response.status_code} - {response.text}")
        return None

def process_and_save_weather_data(**context):
    weather_data = context["task_instance"].xcom_pull(task_ids="fetch_weather_data_task")

    if weather_data:
        location_name = weather_data["name"]
        temperature_k = weather_data["main"]["temp"]
        temperature_c = temperature_k - 273.15
        weather_condition = weather_data["weather"][0]["description"]
        timestamp = datetime.utcfromtimestamp(weather_data["dt"]).strftime("%Y-%m-%d %H:%M:%S")

        df = pd.DataFrame([{
            "location": location_name,
            "temperature_c": temperature_c,
            "weather_condition": weather_condition,
            "timestamp": timestamp,
        }])

        output_file_path = "/usr/local/airflow/dags/weather_data56.csv"
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
        df.to_csv(output_file_path, index=False)
        logging.info("Weather data saved successfully")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 29),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "weather_data_trigger_dag",
    default_args=default_args,
    description="A DAG to fetch weather data and store it",
    schedule_interval=timedelta(days=1),
    catchup=False,
)

fetch_weather_data_task = PythonOperator(
    task_id="fetch_weather_data_task",
    python_callable=fetch_weather_data,
    dag=dag,
)

process_and_save_weather_data_task = PythonOperator(
    task_id="process_and_save_weather_data_task",
    python_callable=process_and_save_weather_data,
    provide_context=True,
    dag=dag,
)

trigger_dag_b = TriggerDagRunOperator(
    task_id="trigger_weather_processing",
    trigger_dag_id="weather_processing_dag",
    dag=dag,
)

fetch_weather_data_task >> process_and_save_weather_data_task >> trigger_dag_b
