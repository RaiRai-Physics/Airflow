from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "rae",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def extract_data():
    data = {
        "order_id": [1, 2, 3, 4],
        "customer_name": ["Asha", "Ben", "Mina", "Leo"],
        "product": ["Laptop", "Mouse", "Keyboard", "Monitor"],
        "quantity": [1, 2, 1, 2],
        "price": [800, 25, 60, 150],
    }

    df = pd.DataFrame(data)
    df.to_csv("/tmp/raw_sales_data.csv", index=False)

    print("Raw sales data created successfully.")


def transform_data():
    df = pd.read_csv("/tmp/raw_sales_data.csv")

    df["total_amount"] = df["quantity"] * df["price"]

    df.to_csv("/tmp/clean_sales_data.csv", index=False)

    print("Sales data transformed successfully.")
    print(df)


def load_data():
    df = pd.read_csv("/tmp/clean_sales_data.csv")

    print("Final data loaded successfully.")
    print(df)

    # In real projects, this could load into Snowflake, Redshift, Postgres, etc.


with DAG(
    dag_id="simple_sales_etl_dag",
    description="A simple ETL pipeline using Airflow and Python",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["beginner", "etl", "python"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    extract_task >> transform_task >> load_task
