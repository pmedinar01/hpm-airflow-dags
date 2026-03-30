from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'pmedina',
    'start_date': datetime(2026, 3, 28),
    'retries': 1,
}

with DAG(
    'test_github_sync_v1',
    default_args=default_args,
    schedule=None,  # <--- Corregido para Airflow 3.x
    catchup=False,
    tags=['cern', 'test']
) as dag:

    t1 = BashOperator(
        task_id='confirmacion_cern',
        bash_command='echo "Sincronización y sintaxis correctas en Airflow 3.x"',
    )
