import logging
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

logging.basicConfig(level=logging.INFO)

def helloWorld():
        logging.info("Deploy test")

with DAG(
        dag_id = "hello_word_dag",
        start_date = datetime(2026,1,1),
        schedule = "@hourly",
        catchup = False
) as dag:

        task1 = PythonOperator(
                task_id = "hello_word",
                python_callable = helloWorld
        )

