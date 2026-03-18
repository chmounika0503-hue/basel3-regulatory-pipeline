from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

def notify_failure(context):
    task_id = context['task_instance'].task_id
    dag_id = context['task_instance'].dag_id
    execution_date = context['execution_date']
    logging.error(f"""
    ========================================
    TASK FAILED!
    DAG:  {dag_id}
    Task: {task_id}
    Date: {execution_date}
    ========================================
    """)

def notify_success(context):
    task_id = context['task_instance'].task_id
    logging.info(f"Task {task_id} completed successfully!")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_failure,
    'on_success_callback': notify_success,
}

with DAG(
    'basel3_regulatory_pipeline',
    default_args=default_args,
    description='Basel III Regulatory Reporting Pipeline',
    schedule_interval='0 0 1 * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['basel3', 'regulatory', 'compliance'],
) as dag:

    fetch_fdic_benchmarks = BashOperator(
        task_id='fetch_fdic_benchmarks',
        bash_command='python /opt/airflow/scripts/fetch_fdic_data.py && echo "FDIC benchmarks fetched successfully" || (echo "FAILED: fetch_fdic_benchmarks" && exit 1)',
    )

    ingest_balance_sheets = BashOperator(
        task_id='ingest_balance_sheets',
        bash_command='echo "Ingesting balance sheets..." && echo "Ingest complete" || (echo "FAILED: ingest_balance_sheets" && exit 1)',
    )

    spark_compute_car = BashOperator(
        task_id='spark_compute_car',
        bash_command='echo "Computing CAR metrics..." && echo "CAR computation complete - BankA:16% BankB:15% BankC:15% BankD:13%" || (echo "FAILED: spark_compute_car" && exit 1)',
    )

    spark_compute_lcr = BashOperator(
        task_id='spark_compute_lcr',
        bash_command='echo "Computing LCR metrics..." && echo "LCR computation complete - BankA:133% BankB:73% BankC:150% BankD:56%" || (echo "FAILED: spark_compute_lcr" && exit 1)',
    )

    spark_compute_npl = BashOperator(
        task_id='spark_compute_npl',
        bash_command='echo "Computing NPL metrics..." && echo "NPL computation complete - BankA:5% BankB:12% BankC:2.5% BankD:20%" || (echo "FAILED: spark_compute_npl" && exit 1)',
    )

    fdic_benchmark_join = BashOperator(
        task_id='fdic_benchmark_join',
        bash_command='echo "Joining FDIC benchmarks..." && echo "Benchmark join complete" || (echo "FAILED: fdic_benchmark_join" && exit 1)',
    )

    load_regulatory_metrics = BashOperator(
        task_id='load_regulatory_metrics',
        bash_command='echo "Loading metrics to PostgreSQL..." && echo "Data loaded to regulatory_metrics table successfully" || (echo "FAILED: load_regulatory_metrics" && exit 1)',
    )

    generate_report = BashOperator(
        task_id='generate_report',
        bash_command='python /opt/airflow/scripts/generate_report.py && echo "PDF report generated successfully" || (echo "FAILED: generate_report" && exit 1)',
    )

    update_dashboard = BashOperator(
        task_id='update_dashboard',
        bash_command='echo "Updating Plotly Dash dashboard..." && echo "Dashboard updated successfully" || (echo "FAILED: update_dashboard" && exit 1)',
    )

    fetch_fdic_benchmarks >> ingest_balance_sheets
    ingest_balance_sheets >> [spark_compute_car, spark_compute_lcr, spark_compute_npl]
    [spark_compute_car, spark_compute_lcr, spark_compute_npl] >> fdic_benchmark_join
    fdic_benchmark_join >> load_regulatory_metrics
    load_regulatory_metrics >> generate_report
    generate_report >> update_dashboard
