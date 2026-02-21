from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id='dag_a',
    start_date=datetime(2026,1,1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    task1 = DummyOperator(task_id='task1')
      
    trigger_dag_b = TriggerDagRunOperator(
        task_id='trigger_dag_b',
        trigger_dag_id='dag_b'
    )

    task1 >> trigger_dag_b