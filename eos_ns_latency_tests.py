from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging
import os
import subprocess
import time
import requests

log = logging.getLogger(__name__)

EOS_MGM     = 'root://eoshomedev.cern.ch'
EOS_TESTDIR = '/eos/homedev.cern.ch/opstest/graphite'
EOS_ENV = {
    'XrdSecPROTOCOL': 'sss',
    'XrdSecSSSKT': '/etc/eos.keytab',
}

PROM_HOST     = 'eos-prometheus.cern.ch:9091'
PROM_TIMEOUT  = 30
PROM_JOB      = 'eos_ns_latency'
PROM_INSTANCE = 'eoshomedev.cern.ch'
PROM_HELP = (
    f'# TYPE {PROM_JOB} gauge\n'
    f'# HELP {PROM_JOB} EOS namespace latency metrics\n'
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['pablo.medina.ramos@cern.ch'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def eos_run(*args):
    """Run an EOS command against EOS_MGM and return the CompletedProcess."""
    cmd = ['eos', '-r', '0', '0', EOS_MGM] + list(args)
    log.info('Running: %s', ' '.join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, **EOS_ENV})
    if result.returncode != 0:
        log.error('stderr: %s', result.stderr.strip())
    return result


@dag(
    dag_id='eos_ns_latency_tests',
    default_args=default_args,
    description='Measures EOS namespace operation latency and publishes metrics to Prometheus',
    schedule='*/10 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['eos', 'monitoring', 'prometheus'],
)
def eos_ns_latency_tests():

    @task()
    def ensure_testdir():
        """Create the EOS test directory if it does not exist."""
        if eos_run('ls', EOS_TESTDIR).returncode != 0:
            log.info('Directory %s not found, creating...', EOS_TESTDIR)
            eos_run('mkdir', '-p', EOS_TESTDIR)
        else:
            log.info('Directory %s already exists.', EOS_TESTDIR)

    @task()
    def collect_ns_metrics() -> dict:
        """Run EOS namespace commands and measure latency for each."""
        commands = {
            'dir':   ['mkdir', 'ls', 'rmdir'],
            'file':  ['touch', 'rm'],
            'other': ['whoami'],
        }
        metrics = {}
        for cmd_type, cmds in commands.items():
            for cmd in cmds:
                args = [cmd, f'{EOS_TESTDIR}/test_{cmd_type}'] if cmd_type != 'other' else [cmd]
                before = time.time()
                result = eos_run(*args)
                duration = time.time() - before
                if result.returncode != 0:
                    duration = -duration
                key = f'{cmd_type}.{cmd}'
                metrics[key] = duration
                log.info('%s: %.4f s', key, duration)
        return metrics

    @task()
    def push_to_prometheus(metrics: dict):
        """Push latency metrics to the Prometheus pushgateway."""
        url = f'http://{PROM_HOST}/metrics/job/{PROM_JOB}/instance/{PROM_INSTANCE}'
        body = PROM_HELP + ''.join(
            f'{PROM_JOB}{{cluster="{PROM_INSTANCE}", instance="{PROM_INSTANCE}", tag="{k}"}} {v}\n'
            for k, v in metrics.items()
        )
        log.info('Pushing to %s:\n%s', url, body)
        r = requests.post(
            url,
            headers={'X-Requested-With': 'Python requests', 'Content-type': 'text/xml'},
            data=body,
            timeout=PROM_TIMEOUT,
        )
        r.raise_for_status()
        log.info('Done (HTTP %d)', r.status_code)

    # Pipeline
    testdir = ensure_testdir()
    metrics = collect_ns_metrics()
    push_to_prometheus(metrics)

    testdir >> metrics


eos_ns_latency_tests()
