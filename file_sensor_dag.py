from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
 
default_args = {
    "owner": "data_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}
 
def process_file():
    print("File found. Processing started...")
    # Add processing logic here
 
with DAG(
    dag_id="file_sensor_dag",
    start_date=datetime(2024, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["sensor", "production"],
) as dag:
 
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/opt/airflow/dags/weather_data56.csv",
        poke_interval=60,      # check every 60 seconds
        timeout=600,           # fail after 10 minutes
        mode="reschedule",     # VERY IMPORTANT (frees worker slot)
    )
 
    process = PythonOperator(
        task_id="process_file",
        python_callable=process_file,
    )
 
    wait_for_file >> process