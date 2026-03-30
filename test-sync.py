from airflow import DAG
from airflow.ops.bash import BashOperator
from datetime import datetime, timedelta

# Configuración básica
default_args = {
    'owner': 'pmedina',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 28), # Una fecha cercana al "hoy" de tu clúster
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'test_github_sync_v1',
    default_args=default_args,
    description='DAG de prueba para validar git-sync desde GitHub',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['test', 'github'],
) as dag:

    # Tarea 1: Saludo
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    # Tarea 2: Confirmación
    t2 = BashOperator(
        task_id='say_hello_github',
        bash_command='echo "¡Hola desde GitHub! Sincronización exitosa en el clúster del CERN."',
    )

    t1 >> t2
