import logging
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def fetch_weather():
    api_endpoint = 'https://api.openweathermap.org/data/2.5/weather'
    api_key = 'bafeb0d911dab09870265df0124550ab'
    latitude = 45.0760
    longitude = 72.8777

    logging.info(
        f"Fetching weather data for coordinates: lat={latitude}, lon={longitude}"
    )
    response = requests.get(
        f"{api_endpoint}?lat={latitude}&lon={longitude}&appid={api_key}"
    )

    if response.status_code == 200:
        weather_response = response.json()
        logging.info("Weather data fetched successfully")
        return weather_response
    else:
        logging.error(
            f"Failed to fetch weather data: {response.status_code} - {response.text}"
        )
        return None


def process_and_store_weather(**context):
    weather_data = context['task_instance'].xcom_pull(
        task_ids='fetch_weather'
    )

    if weather_data:
        city_name = weather_data['name']
        temperature_kelvin = weather_data['main']['temp']
        temperature_celsius = temperature_kelvin - 273.15
        weather_description = weather_data['weather'][0]['description']
        recorded_at = datetime.utcfromtimestamp(
            weather_data['dt']
        ).strftime('%Y-%m-%d %H:%M:%S')

        weather_df = pd.DataFrame([{
            'location': city_name,
            'temperature_celsius': temperature_celsius,
            'weather_condition': weather_description,
            'timestamp': recorded_at
        }])

        output_csv_path = '/usr/local/airflow/dags/weather_data56.csv'

        os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
        weather_df.to_csv(output_csv_path, index=False)

        logging.info("Weather data saved successfully")


with DAG(
    dag_id='weather_pipeline',
    start_date=datetime(2026, 2, 9),
    schedule_interval='@daily',
    catchup=False
) as dag:

    fetch_weather_task = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_weather
    )

    process_weather_task = PythonOperator(
        task_id='process_weather',
        python_callable=process_and_store_weather
    )

    fetch_weather_task >> process_weather_task
