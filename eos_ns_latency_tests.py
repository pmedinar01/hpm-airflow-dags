from airflow.decorators import dag, task
from airflow.models.param import Param
from datetime import datetime, timedelta
import logging
import os
import subprocess
import time
import requests

log = logging.getLogger(__name__)

EOS_MGM = 'root://eoshomedev.cern.ch'
EOS_ENV = {
    'XrdSecPROTOCOL': 'sss',
    'XrdSecSSSKT': '/etc/eos.keytab',
}

PROMHOST = "eos-prometheus.cern.ch:9091"
PROMTIMEOUT = 30
PROMMETRIC_LATENCY = "eos_ns_latency"
PROMMETRIC_LATENCY_HELP = (
    f"# TYPE {PROMMETRIC_LATENCY} gauge\n"
    f"# HELP {PROMMETRIC_LATENCY} EOS latency and state metrics\n"
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


@dag(
    dag_id='eos_ns_latency_tests',
    default_args=default_args,
    description='Measures EOS namespace operation latency and publishes metrics to Prometheus',
    schedule='*/10 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['eos', 'monitoring', 'prometheus'],
    params={
        'instance': Param('eoshomedev.cern.ch', type='string', description='EOS instance to monitor'),
    },
)
def eos_ns_latency_tests():

    @task()
    def ensure_testdir(**context) -> str:
        """Create the EOS test directory if it does not exist and return its path."""
        instance = context['params']['instance']
        eos_dirname = instance[3:] if instance.startswith('eos') else instance
        testdir = f'/eos/{eos_dirname}/opstest/graphite/'
        env = {**os.environ, **EOS_ENV}
        result = subprocess.run(
            ['eos', EOS_MGM, 'ls', testdir],
            capture_output=True, text=True, env=env,
        )
        if result.returncode != 0:
            log.info('Directory %s does not exist, creating...', testdir)
            mkdir_result = subprocess.run(
                ['eos', EOS_MGM, 'mkdir', '-p', testdir],
                capture_output=True, text=True, env=env,
            )
            if mkdir_result.returncode != 0:
                log.error('Could not create directory %s: %s', testdir, mkdir_result.stderr)
        else:
            log.info('Directory %s already exists.', testdir)
        return testdir

    @task()
    def collect_ns_metrics(testdir: str, **context) -> dict:
        """Run EOS commands (mkdir/ls/rmdir, touch/rm, whoami) and measure their latency."""
        instance = context['params']['instance']
        ROLE = 'dteam001 cg'
        env = {**os.environ, **EOS_ENV}
        commands = {
            'dir':   ['mkdir', 'ls', 'rmdir'],
            'file':  ['touch', 'rm'],
            'other': ['whoami'],
        }
        metrics = {}
        for cmd_type, cmds in commands.items():
            for cmd in cmds:
                if cmd_type != 'other':
                    args = ['eos', '--batch', '--role', ROLE, EOS_MGM, cmd, testdir + '/test_' + cmd_type]
                else:
                    args = ['eos', '--batch', '--role', ROLE, EOS_MGM, cmd]
                log.info('Running: %s', ' '.join(args))
                before = time.time()
                proc = subprocess.run(args, capture_output=True, text=True, env=env)
                duration = time.time() - before
                if proc.returncode != 0:
                    duration = -duration
                    log.error('ERROR running %s: %s', ' '.join(args), proc.stderr)
                else:
                    if proc.stdout:
                        log.info('stdout: %s', proc.stdout)
                key = f'{cmd_type}.{cmd}'
                metrics[key] = duration
                log.info('%s: %.4f s', key, duration)
        return metrics

    @task()
    def push_to_prometheus(metrics: dict, **context):
        """Push metrics to the Prometheus pushgateway."""
        instance = context['params']['instance']
        job = PROMMETRIC_LATENCY
        headers = {'X-Requested-With': 'Python requests', 'Content-type': 'text/xml'}
        url = f'http://{PROMHOST}/metrics/job/{job}/instance/{instance}'
        data = PROMMETRIC_LATENCY_HELP
        for k, v in metrics.items():
            data += f'{job}{{cluster="{instance}", instance="{instance}", tag="{k}"}} {v}\n'
        log.info('Pushing metrics to %s:\n%s', url, data)
        r = requests.post(url, headers=headers, data=data, timeout=PROMTIMEOUT)
        r.raise_for_status()
        log.info('Metrics pushed successfully (HTTP %d)', r.status_code)

    # Pipeline
    testdir = ensure_testdir()
    metrics = collect_ns_metrics(testdir)
    push_to_prometheus(metrics)


eos_ns_latency_tests()
