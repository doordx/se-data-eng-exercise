from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

with DAG(
        dag_id='load_errors',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
) as dag:
    load_errors = BashOperator(
        task_id='load_errors',
        bash_command="cd /dbt_curate && dbt run --models load_errors",
    )

    load_errors