from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


with DAG(
    "datalake_etl",
    default_args={"owner": "airflow", "retries": 1},
    description="ETL Pipeline for Batch Data Lake",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    landing_to_bronze = BashOperator(
        task_id="landing_to_bronze",
        bash_command="C:/Repos/Python/goit-de-fp/landing_to_bronze.py",
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="C:/Repos/Python/goit-de-fp/bronze_to_silver.py",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="C:/Repos/Python/goit-de-fp/silver_to_gold.py",
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
