from airflow import DAG
from datetime import datetime, timedelta

# PYTHON
# Los imports pesados (google-genai, requests) van DENTRO de cada task, no aquí:
# el dag-processor re-ejecuta este archivo completo en cada ciclo de parseo.

# TASK


# DAG

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['jafetdc16@gmail.com','cporvenir3102@gmail.com'],
    'retries': 1,
    'retry_delay':timedelta(minutes=5),
}

with DAG(
    dag_id="procesar_comprobante",
    default_args=default_args,
    description="Dag de procesamiento de comprobantes de pago con ia para registro en notion",
    schedule=None,
    start_date= datetime(2026,8,8), #ISO YYYY-MM-DD
    catchup=False,
    tags=['Comprobante','Finanzas','Automatizacion'],
)as dag:
    pass
