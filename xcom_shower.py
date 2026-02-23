from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def generate_values(**context):
    ti = context['ti']

    ti.xcom_push(key='age', value=27)
    ti.xcom_push(key='city', value='Mumbai')
    ti.xcom_push(key='temperature', value=32.5)
    ti.xcom_push(key='is_sunny', value=True)

def read_values(**context):
    ti = context['ti']

    age = ti.xcom_pull(key='age', task_ids='generate_task')
    city = ti.xcom_pull(key='city', task_ids='generate_task')
    temperature = ti.xcom_pull(key='temperature', task_ids='generate_task')
    is_sunny = ti.xcom_pull(key='is_sunny', task_ids='generate_task')

    print("Age:", age)
    print("City:", city)
    print("Temperature:", temperature)
    print("Is Sunny:", is_sunny)

with DAG(
    dag_id='xcom_multiple_example',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    generate_task = PythonOperator(
        task_id='generate_task',
        python_callable=generate_values,
        provide_context=True
    )

    read_task = PythonOperator(
        task_id='read_task',
        python_callable=read_values,
        provide_context=True
    )

    generate_task >> read_task
