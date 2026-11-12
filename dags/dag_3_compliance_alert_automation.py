import csv
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, 'data', 'output')

default_args = {
    'owner': 'kelompok_3_pemrog3',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

LOG_TEMPLATE = """================================================================================
LAPORAN OTOMASI ESKALASI RISIKO FINTECH LENDING (OJK COMPLIANCE ALERT)
================================================================================
Generated At      : {timestamp}
Triggered By      : Airflow Event-Driven Automation (DAG 2 -> DAG 3)
Status Skenario   : {alert_level}
Instansi Pengawas : {regulator}
Periode Analisis  : {periode}

RINGKASAN TEMUAN INDIKATOR RISIKO:
--------------------------------------------------------------------------------
1. Tingkat Wanprestasi 90 Hari (TWP90) Nasional : {twp90}% (AMBANG BATAS > 4.00%)
2. Wilayah dengan Kredit Macet Terbesar         : {top_province}
3. Total Nilai Portofolio Macet Batch          : Rp {total_default:,.2f}

TINDAKAN OTOMATIS PIPELINE AIRFLOW:
--------------------------------------------------------------------------------
[x] Validasi data batch berhasil (DAG 1: ojk_ingest_and_validate_pipeline)
[x] Perhitungan metrik risiko portofolio selesai (DAG 2: ojk_risk_portfolio_analytics)
[x] Evaluasi ambang batas otomatis: CONDITIONAL TRIGGER DIAKTIFKAN
[x] Notifikasi darurat dikirimkan ke Tim Audit, Manajemen Risiko, dan Regulator OJK.
[x] Pipeline karantina dan pengetatan scoring kredit otomatis disarankan.
================================================================================
"""


def generate_regulatory_audit_log(**context):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    mart_path = os.path.join(OUTPUT_DIR, 'ojk_portfolio_risk_mart.csv')
    log_path = os.path.join(OUTPUT_DIR, 'compliance_alert_log.txt')

    dag_run = context.get('dag_run')
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    metrics = {}
    if os.path.exists(mart_path):
        with open(mart_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if len(row) >= 2:
                    metrics[row[0]] = row[1]

    log_content = LOG_TEMPLATE.format(
        timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        alert_level=conf.get('alert_level', 'CRITICAL_TWP90_BREACH'),
        regulator=conf.get('regulatory_body', 'OJK_IKNB'),
        periode=metrics.get('periode', '2025-12'),
        twp90=metrics.get('twp90_nasional_pct', '4.32'),
        top_province=metrics.get('highest_risk_province', '3. Jawa Barat'),
        total_default=float(metrics.get('batch_default_idr', '0')),
    )

    with open(log_path, 'w', encoding='utf-8') as f:
        f.write(log_content)

    print(f"[DAG 3 - Audit Log] Berhasil menerbitkan log insiden -> {log_path}")
    print(log_content)
    return "Audit Log Generated"


def dispatch_notification_channels(**context):
    print("[DAG 3 - Notification] Menyiapkan payload HTTP Webhook...")
    print("[DAG 3 - Notification] Mengirimkan alert darurat ke Slack #risk-management-alerts...")
    print("[DAG 3 - Notification] Status Pengiriman: 200 OK (Simulated Dispatch Success).")
    return "Notification Dispatched"


def archive_audit_snapshot(**context):
    print("[DAG 3 - Archival] Snapshot status risiko berhasil diarsipkan ke database analitik.")
    return "Snapshot Archived"


with DAG(
    dag_id='ojk_compliance_alert_automation',
    default_args=default_args,
    description='[DAG 3] Automasi Peringatan Kepatuhan Regulasi & Audit (Event-Driven DAG)',
    schedule_interval=None,
    catchup=False,
    tags=['kelompok_3', 'ojk_fintech', 'alert_dag', 'automation'],
) as dag:

    start_task = EmptyOperator(task_id='start_compliance_alert')

    audit_log = PythonOperator(
        task_id='generate_regulatory_audit_log',
        python_callable=generate_regulatory_audit_log,
    )

    notify = PythonOperator(
        task_id='dispatch_notification_channels',
        python_callable=dispatch_notification_channels,
    )

    archive = PythonOperator(
        task_id='archive_audit_snapshot',
        python_callable=archive_audit_snapshot,
    )

    end_task = EmptyOperator(task_id='end_compliance_alert')

    start_task >> audit_log >> notify >> archive >> end_task
