import csv
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'processed')

default_args = {
    'owner': 'kelompok_3_pemrog3',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


def extract_and_load_summary(**context):
    summary_path = os.path.join(DATA_DIR, 'ojk_monthly_summary.csv')
    if not os.path.exists(summary_path):
        raise FileNotFoundError(f"File {summary_path} tidak ditemukan!")

    with open(summary_path, 'r', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))

    print(f"[DAG 1 - Ingestion] Berhasil memuat {len(rows)} periode ringkasan OJK.")
    context['task_instance'].xcom_push(key='total_summary_records', value=len(rows))
    return f"Loaded {len(rows)} summary rows"


def validate_loan_batch_quality(**context):
    batch_path = os.path.join(DATA_DIR, 'fintech_loan_daily_batch.csv')
    if not os.path.exists(batch_path):
        raise FileNotFoundError(f"File {batch_path} tidak ditemukan!")

    total_valid = 0
    total_invalid = 0

    with open(batch_path, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            try:
                amount = float(row['loan_amount'])
            except ValueError:
                total_invalid += 1
                continue

            if amount <= 0 or not row['loan_status']:
                total_invalid += 1
            else:
                total_valid += 1

    print(f"[DAG 1 - Quality Gate] Validasi Selesai: {total_valid} Valid, {total_invalid} Anomali.")
    if total_invalid > 0:
        raise ValueError(f"Ditemukan {total_invalid} data tidak valid pada batch pinjaman!")

    context['task_instance'].xcom_push(key='batch_valid_count', value=total_valid)
    return "Batch Data 100% Valid"


with DAG(
    dag_id='ojk_ingest_and_validate_pipeline',
    default_args=default_args,
    description='[DAG 1] Pipeline Ingest dan Validasi Data OJK (Controller DAG)',
    schedule_interval='@monthly',
    catchup=False,
    tags=['kelompok_3', 'ojk_fintech', 'controller_dag'],
) as dag:

    start_task = EmptyOperator(task_id='start_ingestion')

    ingest_summary = PythonOperator(
        task_id='ingest_ojk_monthly_summary',
        python_callable=extract_and_load_summary,
    )

    validate_batch = PythonOperator(
        task_id='validate_loan_batch_quality',
        python_callable=validate_loan_batch_quality,
    )

    trigger_analytics = TriggerDagRunOperator(
        task_id='trigger_risk_portfolio_analytics',
        trigger_dag_id='ojk_risk_portfolio_analytics',
        execution_date='{{ execution_date }}',
        conf={
            'triggered_by': 'ojk_ingest_and_validate_pipeline',
            'execution_status': 'SOURCE_VALIDATED',
            'timestamp': '{{ ts }}',
        },
        wait_for_completion=False,
        reset_dag_run=True,
    )

    end_task = EmptyOperator(task_id='end_ingestion')

    start_task >> ingest_summary >> validate_batch >> trigger_analytics >> end_task
