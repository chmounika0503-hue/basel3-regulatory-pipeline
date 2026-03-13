
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="basel3_pipeline",
    start_date=datetime(2024,1,1),
    schedule_interval="@monthly",
    catchup=False
) as dag:

    ingest = BashOperator(
        task_id="ingest_balance_sheets",
        bash_command="spark-submit spark_jobs/ingest_balance_sheet.py"
    )

    car = BashOperator(
        task_id="compute_car",
        bash_command="spark-submit spark_jobs/compute_car.py"
    )

    lcr = BashOperator(
        task_id="compute_lcr",
        bash_command="spark-submit spark_jobs/compute_lcr.py"
    )

    npl = BashOperator(
        task_id="compute_npl",
        bash_command="spark-submit spark_jobs/compute_npl.py"
    )

    ingest >> car >> lcr >> npl
