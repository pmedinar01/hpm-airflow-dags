from airflow.decorators import dag, task
from airflow.sdk import Asset
from datetime import datetime, timedelta
import logging
import os
import subprocess
import time

log = logging.getLogger(__name__)

EOS_MGM = 'root://eoshomedev.cern.ch'
EOS_FILE = '/eos/user/p/pmedinar/asset-demo/probe.txt'
EOS_ENV = {
    'XrdSecPROTOCOL': 'sss',
    'XrdSecSSSKT': '/etc/eos.keytab',
}

file_created  = Asset('eos://eoshomedev.cern.ch/eos/user/p/pmedinar/asset-demo/probe.txt')
file_verified = Asset('eos://eoshomedev.cern.ch/eos/user/p/pmedinar/asset-demo/verified')


def eos_run(*args):
    cmd = ['eos', '-r', '0', '0', EOS_MGM] + list(args)
    log.info('Running: %s', ' '.join(cmd))
    result = subprocess.run(
        cmd, capture_output=True, text=True,
        env={**os.environ, **EOS_ENV},
    )
    if result.stdout:
        log.info('stdout:\n%s', result.stdout)
    if result.stderr:
        log.warning('stderr:\n%s', result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f'EOS failed (rc={result.returncode}): {result.stderr}')
    return result.stdout.strip()


@dag(
    dag_id='eos_asset_touch',
    description='Creates a file in EOS and publishes file_created asset.',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['eos', 'assets', 'demo'],
)
def eos_asset_touch():

    @task(outlets=[file_created])
    def touch():
        eos_run('mkdir', '-p', '/eos/user/p/pmedinar/asset-demo')
        eos_run('touch', EOS_FILE)
        log.info('File created: %s', EOS_FILE)
        time.sleep(10)

    touch()


@dag(
    dag_id='eos_asset_ls',
    description='Lists the EOS directory. Triggered when file_created is updated.',
    schedule=[file_created],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['eos', 'assets', 'demo'],
)
def eos_asset_ls():

    @task(outlets=[file_verified])
    def ls():
        output = eos_run('ls', '-l', '/eos/user/p/pmedinar/asset-demo')
        log.info('Directory listing:\n%s', output)
        time.sleep(10)

    ls()


@dag(
    dag_id='eos_asset_cleanup',
    description='Removes the file and directory. Triggered when file_verified is updated.',
    schedule=[file_verified],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['eos', 'assets', 'demo'],
)
def eos_asset_cleanup():

    @task()
    def cleanup():
        eos_run('rm', EOS_FILE)
        eos_run('rmdir', '/eos/user/p/pmedinar/asset-demo')
        log.info('Cleaned up.')

    cleanup()


eos_asset_touch()
eos_asset_ls()
eos_asset_cleanup()
