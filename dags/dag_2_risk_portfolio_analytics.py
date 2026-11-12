import csv
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'processed')
OUTPUT_DIR = os.path.join(BASE_DIR, 'data', 'output')

TWP90_THRESHOLD = 4.0

default_args = {
    'owner': 'kelompok_3_pemrog3',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


def aggregate_portfolio_risk(**context):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    batch_path = os.path.join(DATA_DIR, 'fintech_loan_daily_batch.csv')
    summary_path = os.path.join(DATA_DIR, 'ojk_monthly_summary.csv')

    with open(summary_path, 'r', encoding='utf-8') as f:
        latest_summary = list(csv.DictReader(f))[-1]
    twp90_nasional = float(latest_summary['twp90_persen'])

    total_loans = 0
    total_disbursed_idr = 0.0
    total_default_idr = 0.0
    macet_count = 0
    province_risk = {}

    with open(batch_path, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            total_loans += 1
            amount = float(row['loan_amount'])
            total_disbursed_idr += amount

            if row['loan_status'] == 'MACET' or int(row['is_twp90_breach']) == 1:
                macet_count += 1
                total_default_idr += amount
                province = row['province']
                province_risk[province] = province_risk.get(province, 0) + amount

    batch_twp90_pct = round(macet_count / total_loans * 100, 2) if total_loans else 0.0

    top_risk_province = max(province_risk, key=province_risk.get) if province_risk else 'N/A'
    top_risk_amount = province_risk.get(top_risk_province, 0.0)

    mart_path = os.path.join(OUTPUT_DIR, 'ojk_portfolio_risk_mart.csv')
    with open(mart_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['metric', 'value', 'keterangan'])
        writer.writerow(['periode', latest_summary['periode'], 'Periode Data OJK'])
        writer.writerow(['twp90_nasional_pct', twp90_nasional, 'Rasio Kredit Macet Nasional OJK'])
        writer.writerow(['batch_total_loans', total_loans, 'Jumlah Akun Pinjaman Terdaftar'])
        writer.writerow(['batch_disbursed_idr', total_disbursed_idr, 'Total Penyaluran Pinjaman (IDR)'])
        writer.writerow(['batch_default_idr', total_default_idr, 'Total Portofolio Macet (IDR)'])
        writer.writerow(['batch_twp90_pct', batch_twp90_pct, 'Tingkat Wanprestasi 90 Hari Batch'])
        writer.writerow([
            'highest_risk_province',
            top_risk_province,
            f'Provinsi Kredit Macet Tertinggi: Rp {top_risk_amount:,.0f}',
        ])

    print(f"[DAG 2 - Analytics] Sukses menulis Data Mart -> {mart_path}")
    print(f"[DAG 2 - Metrik] TWP90 Nasional: {twp90_nasional}%, Batch TWP90: {batch_twp90_pct}%")

    ti = context['task_instance']
    ti.xcom_push(key='twp90_nasional', value=twp90_nasional)
    ti.xcom_push(key='top_risk_province', value=top_risk_province)
    ti.xcom_push(key='total_default_idr', value=total_default_idr)
    return "Analytics Completed"


def evaluate_risk_threshold(**context):
    twp90 = context['task_instance'].xcom_pull(task_ids='aggregate_portfolio_risk', key='twp90_nasional')
    if twp90 is None:
        twp90 = 4.32

    print(f"[DAG 2 - Decision Gate] Memeriksa Ambang Batas TWP90 OJK: {twp90}% vs Batas Regulasi ({TWP90_THRESHOLD:.2f}%)")

    if float(twp90) > TWP90_THRESHOLD:
        print("[ALERT TRIGGERED] TWP90 melampaui batas aman OJK! Mengalihkan ke trigger_compliance_alert...")
        return 'trigger_compliance_alert'

    print("[NORMAL] TWP90 berada dalam batas wajar. Melewati alert.")
    return 'skip_compliance_alert'


with DAG(
    dag_id='ojk_risk_portfolio_analytics',
    default_args=default_args,
    description='[DAG 2] Analisis Risiko Portofolio & Decision Gate (Target Worker DAG)',
    schedule_interval=None,
    catchup=False,
    tags=['kelompok_3', 'ojk_fintech', 'worker_dag', 'branching'],
) as dag:

    wait_for_dag1 = ExternalTaskSensor(
        task_id='wait_for_dag1_validation',
        external_dag_id='ojk_ingest_and_validate_pipeline',
        external_task_id='validate_loan_batch_quality',
        allowed_states=['success'],
        check_existence=True,
        mode='reschedule',
        poke_interval=15,
        timeout=180,
    )

    process_risk = PythonOperator(
        task_id='aggregate_portfolio_risk',
        python_callable=aggregate_portfolio_risk,
    )

    branch_decision = BranchPythonOperator(
        task_id='evaluate_risk_threshold',
        python_callable=evaluate_risk_threshold,
    )

    trigger_alert = TriggerDagRunOperator(
        task_id='trigger_compliance_alert',
        trigger_dag_id='ojk_compliance_alert_automation',
        execution_date='{{ execution_date }}',
        conf={
            'alert_level': 'CRITICAL_TWP90_BREACH',
            'regulatory_body': 'OJK_IKNB',
            'action_required': 'RECOVERY_AND_RISK_AUDIT',
        },
        wait_for_completion=False,
        reset_dag_run=True,
    )

    skip_alert = EmptyOperator(task_id='skip_compliance_alert')

    end_task = EmptyOperator(
        task_id='end_risk_analytics',
        trigger_rule='none_failed_min_one_success',
    )

    wait_for_dag1 >> process_risk >> branch_decision
    branch_decision >> trigger_alert >> end_task
    branch_decision >> skip_alert >> end_task
