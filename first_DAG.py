from datetime import datetime
 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator as po
 
 
def greet():
    print("Hello, this is my first DAG.")
 
 
with DAG(
    dag_id="my_first_dag",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task1 = po(task_id="greet_task", python_callable=greet)