from airflow.decorators import dag, task
from datetime import datetime, timedelta
import os
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['pablo.medina.ramos@cern.ch'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

EOS_MGM = 'root://eoshomedev.cern.ch'
EOS_PATH = '/eos/user/p/pmedinar/test1.txt'
EOS_ENV = {
    'XrdSecPROTOCOL': 'sss',
    'XrdSecSSSKT': '/etc/eos.keytab',
}


@dag(
    dag_id='eos_touch_rm_test',
    default_args=default_args,
    description='Crea y borra un fichero en EOS usando eos -r 0 0',
    schedule='*/5 * * * *',  # probe cada 5 minutos
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['eos', 'test'],
)
def eos_touch_rm_test():

    @task()
    def eos_touch():
        """Crea el fichero en EOS con eos -r 0 0 touch"""
        env = {**os.environ, **EOS_ENV}
        result = subprocess.run(
            ['eos', '-r', '0', '0', EOS_MGM, 'touch', EOS_PATH],
            capture_output=True,
            text=True,
            env=env,
        )
        print(result.stdout)
        if result.returncode != 0:
            raise RuntimeError(f'eos touch falló: {result.stderr}')
        return f'Fichero creado: {EOS_PATH}'

    @task()
    def eos_rm():
        """Borra el fichero en EOS con eos -r 0 0 rm"""
        env = {**os.environ, **EOS_ENV}
        result = subprocess.run(
            ['eos', '-r', '0', '0', EOS_MGM, 'rm', EOS_PATH],
            capture_output=True,
            text=True,
            env=env,
        )
        print(result.stdout)
        if result.returncode != 0:
            raise RuntimeError(f'eos rm falló: {result.stderr}')
        return f'Fichero borrado: {EOS_PATH}'

    eos_touch() >> eos_rm()


eos_touch_rm_test()
