from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
 
with DAG(
    dag_id="parallel_tasks_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example"],
) as dag:
 
    start = EmptyOperator(task_id="start")
 
    # Parallel tasks
    task_1 = EmptyOperator(task_id="task_1")
    task_2 = EmptyOperator(task_id="task_2")
    task_3 = EmptyOperator(task_id="task_3")
 
    end = EmptyOperator(task_id="end")
 
    # Flow
    start >> [task_1, task_2, task_3] >> end