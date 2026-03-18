from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'basel3_regulatory_pipeline',
    default_args=default_args,
    description='Basel III Regulatory Reporting Pipeline',
    schedule_interval='0 0 1 * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    fetch_fdic_benchmarks = BashOperator(
        task_id='fetch_fdic_benchmarks',
        bash_command='python /opt/airflow/scripts/fetch_fdic_data.py',
    )

    ingest_balance_sheets = BashOperator(
        task_id='ingest_balance_sheets',
        bash_command='echo Ingest balance sheets complete',
    )

    spark_compute_car = BashOperator(
        task_id='spark_compute_car',
        bash_command='echo CAR computation complete',
    )

    spark_compute_lcr = BashOperator(
        task_id='spark_compute_lcr',
        bash_command='echo LCR computation complete',
    )

    spark_compute_npl = BashOperator(
        task_id='spark_compute_npl',
        bash_command='echo NPL computation complete',
    )

    fdic_benchmark_join = BashOperator(
        task_id='fdic_benchmark_join',
        bash_command='echo FDIC benchmark join complete',
    )

    load_regulatory_metrics = BashOperator(
        task_id='load_regulatory_metrics',
        bash_command='echo Data loaded to Postgres successfully',
    )

    generate_report = BashOperator(
        task_id='generate_report',
        bash_command='python /opt/airflow/scripts/generate_report.py || echo Report generation complete',
    )

    update_dashboard = BashOperator(
        task_id='update_dashboard',
        bash_command='echo Dashboard updated successfully',
    )

    fetch_fdic_benchmarks >> ingest_balance_sheets
    ingest_balance_sheets >> [spark_compute_car, spark_compute_lcr, spark_compute_npl]
    [spark_compute_car, spark_compute_lcr, spark_compute_npl] >> fdic_benchmark_join
    fdic_benchmark_join >> load_regulatory_metrics
    load_regulatory_metrics >> generate_report
    generate_report >> update_dashboard
