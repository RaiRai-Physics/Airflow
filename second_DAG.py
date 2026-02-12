from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
 
def task_a():
    print("task a is done")
 
def task_b():
    print("task b is done")
 
with DAG(
    dag_id ='task_pipeline',
    start_date=datetime(2026,2,2),
    schedule_interval='@daily',
    catchup = False
) as dag :
    
    A = PythonOperator(task_id='greet_1',python_callable=task_a)
    B = PythonOperator(task_id='greet_2',python_callable=task_b)
 
    A >> B