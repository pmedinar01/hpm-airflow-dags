from airflow import DAG
from airflow.operators.bash import BashOperator  # <--- Corregido 'operators'
from datetime import datetime, timedelta

default_args = {
    'owner': 'pmedina',
    'start_date': datetime(2026, 3, 28),
    'retries': 1,
}

with DAG(
    'test_github_sync_v1',
    default_args=default_args,
    schedule_interval=None, # Lo activaremos manualmente para probar
    catchup=False,
) as dag:

    t1 = BashOperator(
        task_id='confirmacion_cern',
        bash_command='echo "Sincronización correcta en el nodo del CERN"',
    )
