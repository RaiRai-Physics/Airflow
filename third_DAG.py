# Your task is to create a airflow dag that reads a text file and
# Counts the number of words in it and print the word count in airflow logs
from datetime import datetime
 
from airflow import DAG
from airflow.operators.python_operator import PythonOperator as po
 
 
def read_print():
    with open("/usr/local/airflow/dags/demo.txt", "r") as f:
        content = f.read()
        split_content = content.split()
        print(len(split_content))
 
 
with DAG(
    dag_id="read_print_dag",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task1 = po(task_id="printcount_task", python_callable=read_print)