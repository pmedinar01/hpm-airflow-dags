"""
Assets demo for Airflow 3.x

Shows data-driven scheduling: the consumer DAG is triggered automatically
whenever the producer DAG updates its Asset — no cron needed on the consumer.

Pipeline:
  [eos_asset_producer]  runs on schedule / manually
        |
        | updates Asset: scan_report
        v
  [eos_asset_consumer]  triggered automatically
        |
        | reads report, logs summary
        v
  [eos_asset_alert]     triggered automatically when consumer finishes
                        (chained assets)
"""

from airflow.decorators import dag, task
from airflow.sdk import Asset
from datetime import datetime, timedelta
import logging
import os
import subprocess
import json

log = logging.getLogger(__name__)

EOS_MGM = 'root://eoshomedev.cern.ch'
EOS_REPORT_PATH = '/eos/user/p/pmedinar/asset-demo/scan_report.json'
EOS_ENV = {
    'XrdSecPROTOCOL': 'sss',
    'XrdSecSSSKT': '/etc/eos.keytab',
}

# --- Asset definitions ---
# An Asset is just a URI that represents a piece of data.
# Any DAG that declares it as an outlet "publishes" it.
# Any DAG that uses it as a schedule "subscribes" to it.

scan_report = Asset(f'eos://{EOS_REPORT_PATH}')
alert_asset  = Asset('eos://eoshomedev.cern.ch/eos/user/p/pmedinar/asset-demo/alert_sent')


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


# ─────────────────────────────────────────────────────────────
# PRODUCER: runs on a schedule, writes a report to EOS.
# Declares scan_report as an outlet → triggers the consumer.
# ─────────────────────────────────────────────────────────────

@dag(
    dag_id='eos_asset_producer',
    description='Scans EOS namespace and writes a JSON report. Triggers consumer via Asset.',
    schedule='0 * * * *',   # every hour
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,
    catchup=False,
    tags=['eos', 'assets', 'demo'],
)
def eos_asset_producer():

    @task()
    def setup():
        eos_run('mkdir', '-p', '/eos/user/p/pmedinar/asset-demo')

    @task(outlets=[scan_report])   # publishing the asset triggers downstream DAGs
    def scan_and_report():
        ns_output = eos_run('ns', 'stat')

        report = {
            'timestamp': datetime.utcnow().isoformat(),
            'mgm': EOS_MGM,
            'ns_stat': ns_output,
        }

        # Write report as JSON to EOS
        report_json = json.dumps(report, indent=2)
        eos_run('put', '-', EOS_REPORT_PATH)   # pipe not shown for simplicity
        log.info('Report written to %s', EOS_REPORT_PATH)
        return report

    setup() >> scan_and_report()


# ─────────────────────────────────────────────────────────────
# CONSUMER: no cron — triggered when scan_report is updated.
# Reads the report and publishes alert_asset.
# ─────────────────────────────────────────────────────────────

@dag(
    dag_id='eos_asset_consumer',
    description='Reads the scan report. Triggered automatically when producer updates it.',
    schedule=[scan_report],   # data-driven: runs when scan_report is updated
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['eos', 'assets', 'demo'],
)
def eos_asset_consumer():

    @task(outlets=[alert_asset])
    def read_report():
        output = eos_run('cat', EOS_REPORT_PATH)
        report = json.loads(output)
        log.info('=== Scan report summary ===')
        log.info('Timestamp : %s', report.get('timestamp'))
        log.info('MGM       : %s', report.get('mgm'))
        log.info('NS stat   :\n%s', report.get('ns_stat'))
        return report

    read_report()


# ─────────────────────────────────────────────────────────────
# ALERT: triggered when the consumer finishes (chained assets).
# In a real setup this would send a SNOW ticket or email.
# ─────────────────────────────────────────────────────────────

@dag(
    dag_id='eos_asset_alert',
    description='Triggered when consumer publishes alert_asset. Sends a notification.',
    schedule=[alert_asset],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['eos', 'assets', 'demo'],
)
def eos_asset_alert():

    @task()
    def notify():
        log.info('Alert triggered — in production this would open a SNOW ticket or send an email.')

    notify()


eos_asset_producer()
eos_asset_consumer()
eos_asset_alert()
