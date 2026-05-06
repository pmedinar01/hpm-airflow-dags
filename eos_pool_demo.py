from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging
import os
import subprocess

log = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'email': ['pablo.medina.ramos@cern.ch'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

EOS_MGM = 'root://eoshomedev.cern.ch'
EOS_BASE = '/eos/user/p/pmedinar/pool-demo'
EOS_ENV = {
    'XrdSecPROTOCOL': 'sss',
    'XrdSecSSSKT': '/etc/eos.keytab',
}

POOL = 'eoshpm_pool'


def eos_run(*args):
    """Run an EOS command and raise on failure."""
    cmd = ['eos', '-r', '0', '0', EOS_MGM] + list(args)
    log.info('Running: %s', ' '.join(cmd))
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env={**os.environ, **EOS_ENV},
    )
    if result.stdout:
        log.info('stdout:\n%s', result.stdout)
    if result.stderr:
        log.warning('stderr:\n%s', result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f'EOS command failed (rc={result.returncode}): {result.stderr}')
    return result.stdout.strip()


@dag(
    dag_id='eos_pool_demo_sequential',
    default_args=default_args,
    description='mkdir → touch → ls → rm → rmdir. One task at a time via pool.',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,
    catchup=False,
    tags=['eos', 'pool', 'demo'],
)
def eos_pool_demo_sequential():
    """
    Sequential pipeline: each step depends on the previous one.
    All tasks share the same pool — demonstrates ordered, throttled execution.
    """

    @task(pool=POOL)
    def make_dir():
        eos_run('mkdir', '-p', EOS_BASE)
        return EOS_BASE

    @task(pool=POOL)
    def touch_files():
        for i in range(3):
            eos_run('touch', f'{EOS_BASE}/file_{i}.txt')
        return f'Created 3 files in {EOS_BASE}'

    @task(pool=POOL)
    def list_dir():
        output = eos_run('ls', '-l', EOS_BASE)
        log.info('Directory listing:\n%s', output)
        return output

    @task(pool=POOL)
    def remove_files():
        for i in range(3):
            eos_run('rm', f'{EOS_BASE}/file_{i}.txt')
        return 'Files removed'

    @task(pool=POOL)
    def remove_dir():
        eos_run('rmdir', EOS_BASE)
        return f'Removed {EOS_BASE}'

    make_dir() >> touch_files() >> list_dir() >> remove_files() >> remove_dir()


@dag(
    dag_id='eos_pool_demo_parallel',
    default_args=default_args,
    description='Touch 5 files in parallel, then remove them. Pool limits concurrency.',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,
    catchup=False,
    tags=['eos', 'pool', 'demo'],
)
def eos_pool_demo_parallel():
    """
    Parallel fan-out: 5 touch tasks fire at once but the pool allows only N at a time.
    Shows how the pool queues the excess tasks instead of flooding EOS.
    """

    @task(pool=POOL)
    def setup():
        eos_run('mkdir', '-p', EOS_BASE)

    @task(pool=POOL)
    def touch_file(index: int):
        path = f'{EOS_BASE}/parallel_{index}.txt'
        eos_run('touch', path)
        return path

    @task(pool=POOL)
    def list_result():
        output = eos_run('ls', '-l', EOS_BASE)
        log.info('Result:\n%s', output)

    @task(pool=POOL)
    def cleanup():
        for i in range(5):
            eos_run('rm', f'{EOS_BASE}/parallel_{i}.txt')
        eos_run('rmdir', EOS_BASE)

    init = setup()
    touches = [touch_file.override(task_id=f'touch_{i}')(i) for i in range(5)]
    listing = list_result()
    done = cleanup()

    init >> touches >> listing >> done


eos_pool_demo_sequential()
eos_pool_demo_parallel()
