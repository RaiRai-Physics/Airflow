from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def generate_value(**context):
    value = 27
    context['ti'].xcom_push(key='my_value', value=value)

def read_value(**context):
    value = context['ti'].xcom_pull(
        key='my_value',
        task_ids='generate_task'
    )
    print("Value from XCom:", value)

with DAG(
    dag_id='xcom_example',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    generate_task = PythonOperator(
        task_id='generate_task',
        python_callable=generate_value,
        provide_context=True
    )

    read_task = PythonOperator(
        task_id='read_task',
        python_callable=read_value,
        provide_context=True
    )

    generate_task >> read_task
