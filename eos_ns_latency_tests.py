from airflow.decorators import dag, task
from airflow.models.param import Param
from datetime import datetime, timedelta
import logging
import re
import subprocess
import time
import requests

log = logging.getLogger(__name__)

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
    def check_is_master():
        """Verify that this node is the MGM master (mode=master-rw or is_master=true)."""
        p = subprocess.run(['eos', '-b', 'ns'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out = p.stdout.decode('utf-8')
        log.info('eos -b ns output:\n%s', out)
        for line in out.splitlines():
            if re.search(r'Replication\s+(mode=master-rw|is_master=true)', line):
                log.info('This node is the MGM master.')
                return True
        raise RuntimeError('This node is not the MGM master. Aborting.')

    @task()
    def ensure_testdir(**context) -> str:
        """Create the EOS test directory if it does not exist and return its path."""
        instance = context['params']['instance']
        eos_dirname = instance[3:] if instance.startswith('eos') else instance
        testdir = f'/eos/{eos_dirname}/opstest/graphite/'
        result = subprocess.run(['eos', 'ls', testdir], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode != 0:
            log.info('Directory %s does not exist, creating...', testdir)
            mkdir_result = subprocess.run(
                ['eos', 'mkdir', '-p', testdir],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            if mkdir_result.returncode != 0:
                log.error('Could not create directory %s: %s', testdir, mkdir_result.stderr.decode())
        else:
            log.info('Directory %s already exists.', testdir)
        return testdir

    @task()
    def collect_ns_metrics(testdir: str, **context) -> dict:
        """Run EOS commands (mkdir/ls/rmdir, touch/rm, whoami) and measure their latency."""
        instance = context['params']['instance']
        ROLE = 'dteam001 cg'
        URL = 'root://' + instance + '/'
        commands = {
            'dir':   ['mkdir', 'ls', 'rmdir'],
            'file':  ['touch', 'rm'],
            'other': ['whoami'],
        }
        metrics = {}
        for cmd_type, cmds in commands.items():
            for cmd in cmds:
                if cmd_type != 'other':
                    args = ['eos', '--batch', '--role', ROLE, URL, cmd, testdir + '/test_' + cmd_type]
                else:
                    args = ['eos', '--batch', '--role', ROLE, URL, cmd]
                log.info('Running: %s', ' '.join(args))
                before = time.time()
                proc = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                duration = time.time() - before
                if proc.returncode != 0:
                    duration = -duration
                    log.error('ERROR running %s: %s', ' '.join(args), proc.stderr.decode())
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
    master_ok = check_is_master()
    testdir = ensure_testdir()
    metrics = collect_ns_metrics(testdir)
    push = push_to_prometheus(metrics)

    master_ok >> testdir >> metrics >> push


eos_ns_latency_tests()
