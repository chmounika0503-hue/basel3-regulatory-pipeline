from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'basel3',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

SPARK_SUBMIT = '/opt/spark/bin/spark-submit'
PG_PACKAGE = 'org.postgresql:postgresql:42.6.0'

with DAG(
    dag_id='basel3_regulatory_pipeline',
    default_args=default_args,
    description='Monthly Basel III regulatory metrics pipeline',
    schedule_interval='0 0 1 * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['basel3', 'regulatory'],
) as dag:

    fetch_fdic = BashOperator(
        task_id='fetch_fdic_benchmarks',
        bash_command='python /opt/airflow/dags/scripts/fetch_fdic_data.py',
    )

    ingest = BashOperator(
        task_id='ingest_balance_sheets',
        bash_command=f'{SPARK_SUBMIT} /opt/spark_jobs/ingest_balance_sheet.py',
    )

    compute_car = BashOperator(
        task_id='spark_compute_car',
        bash_command=f'{SPARK_SUBMIT} /opt/spark_jobs/compute_car.py',
    )

    compute_lcr = BashOperator(
        task_id='spark_compute_lcr',
        bash_command=f'{SPARK_SUBMIT} /opt/spark_jobs/compute_lcr.py',
    )

    compute_npl = BashOperator(
        task_id='spark_compute_npl',
        bash_command=f'{SPARK_SUBMIT} /opt/spark_jobs/compute_npl.py',
    )

    benchmark_join = BashOperator(
        task_id='fdic_benchmark_join',
        bash_command=f'{SPARK_SUBMIT} --packages {PG_PACKAGE} /opt/spark_jobs/fdic_benchmark_join.py',
    )

    load_metrics = BashOperator(
        task_id='load_regulatory_metrics',
        bash_command=f'{SPARK_SUBMIT} --packages {PG_PACKAGE} /opt/spark_jobs/load_to_postgres.py',
    )

    # Pipeline flow
    fetch_fdic >> ingest >> [compute_car, compute_lcr, compute_npl] >> benchmark_join >> load_metrics
