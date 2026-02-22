from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import logging
import os

def load_and_print_weather_data():
    file_path = "/usr/local/airflow/dags/weather_data56.csv"

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found at {file_path}")

    df = pd.read_csv(file_path)

    logging.info("Weather Data Loaded Successfully")
    logging.info(f"\n{df}")

    print(df)  # This will also show in task logs

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
}

dag = DAG(
    dag_id="weather_processing_dag",
    default_args=default_args,
    schedule_interval=None,  # Trigger-only DAG
    catchup=False,
)

load_weather_task = PythonOperator(
    task_id="load_and_print_weather_data",
    python_callable=load_and_print_weather_data,
    dag=dag,
)
