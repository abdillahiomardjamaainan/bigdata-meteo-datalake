from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def say_hello():
    print("Hello Airflow! ✅ Pipeline is running.")


with DAG(
    dag_id="00_hello_airflow",
    description="First minimal DAG to validate Airflow setup",
    start_date=datetime(2026, 2, 1),
    schedule=None,  # manual run only
    catchup=False,
    tags=["setup", "hello"],
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )
