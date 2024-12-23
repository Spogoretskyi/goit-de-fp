from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import subprocess


def run_landing_to_bronze():
    try:
        print("Starting landing_to_bronze...")

        result = subprocess.run(
        ["python", "C:\Repos\Python\goit-de-fp\landing_to_bronze.py"],
        shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        print("landing_to_bronze completed successfully!")
        print(result.stdout.decode())
    except subprocess.CalledProcessError as e:
        if e.stderr:
            print(f"Error occurred: {e.stderr.decode()}")
        else:
            print(f"Error occurred, but no stderr output: {e}")
        raise


def run_bronze_to_silver():
    try:
        result = subprocess.run(
            ["python", "C:/Repos/Python/goit-de-fp/bronze_to_silver.py"],
            shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        print(result.stdout.decode())
    except subprocess.CalledProcessError as e:
        if e.stderr:
            print(f"Error occurred: {e.stderr.decode()}")
        else:
            print(f"Error occurred, but no stderr output: {e}")
        raise


def run_silver_to_gold():
    try:
        result = subprocess.run(
            ["python", "C:/Repos/Python/goit-de-fp/silver_to_gold.py"],
            shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        print(result.stdout.decode())
    except subprocess.CalledProcessError as e:
        if e.stderr:
            print(f"Error occurred: {e.stderr.decode()}")
        else:
            print(f"Error occurred, but no stderr output: {e}")
        raise


with DAG(
    "datalake_etl",
    default_args={"owner": "airflow", "retries": 1},
    description="ETL Pipeline for Batch Data Lake",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    landing_to_bronze = PythonOperator(
        task_id="landing_to_bronze",
        python_callable=run_landing_to_bronze,
    )

    bronze_to_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=run_bronze_to_silver,
    )

    silver_to_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=run_silver_to_gold,
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold