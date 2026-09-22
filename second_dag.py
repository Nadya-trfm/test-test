import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor


default_args = {
    'owner' : 'airflow',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=10)
}


def rename_file():
    old_file = "/tmp/data.txt"
    current_date = datetime.now().strftime("%Y-%m-%d")
    new_file = f"/tmp/data_{current_date}.txt"
    os.rename(old_file, new_file)


with DAG(
    dag_id="second_dag",
    default_args=default_args,
    start_date=datetime(2026, 9, 20),
    catchup=False,
    schedule_interval="@daily",
) as dag: 
      
    waiting_first_task = ExternalTaskSensor(
                task_id="waiting_first_task",
                timeout=360,
                poke_interval=10,
                external_dag_id="first_dag",
                external_task_id="finish_pipeline"
            )

    rename_file = PythonOperator(
                task_id="rename_file",
                python_callable=rename_file 
            )

    waiting_first_task >> rename_file
