from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd


def read_csv_1():
    df = pd.read_csv("/opt/airflow/dags/sales.csv")
    print(df)


def read_csv_2():
    df = pd.read_csv("/opt/airflow/dags/products.csv")
    print(df)


def read_csv_3():
    df = pd.read_csv("/opt/airflow/dags/customers.csv")
    print(df)


with DAG(
    dag_id="parallel_tasks_csv_read_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    task_1 = PythonOperator(
        task_id="task_1",
        python_callable=read_csv_1,
    )

    task_2 = PythonOperator(
        task_id="task_2",
        python_callable=read_csv_2,
    )

    task_3 = PythonOperator(
        task_id="task_3",
        python_callable=read_csv_3,
    )

    end = EmptyOperator(task_id="end")

    start >> [task_1, task_2, task_3] >> end

#'/usr/local/airflow/dags/weather_data56.csv'