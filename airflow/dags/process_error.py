from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

QUERY = """ SELECT load_id FROM `ee-india-se-data.movies_data_manish.load_errors`; """


def delete_valid_records_from_error(**context):
    client = bigquery.Client(location="asia-south1")
    query_job = client.query(QUERY)
    result = query_job.result()

    for error_index, error_row in enumerate(result):
        for index, row in enumerate(get_bad_data_movies(client, error_row)):
            if row.id.isnumeric():
                print("valid id:", row.id)
                delete_record(client, row)


def delete_record(client, row):
    delete_query = f""" DELETE FROM `ee-india-se-data.movies_data_manish.load_errors`
                    where load_id = "{row.load_id}"; """
    delete_query_job = client.query(delete_query)
    delete_query_job.result()
    print("Deleted!", row.load_id)


def get_bad_data_movies(client, error_row):
    movie_query = f""" SELECT * FROM `ee-india-se-data.movies_data_manish.movies_raw`
            where load_id = "{error_row.load_id}"; """
    movie_query_job = client.query(movie_query)
    movies = movie_query_job.result()
    return movies


with DAG(
        dag_id='process_errors',
        default_args=default_args,
        schedule_interval=None,  #timedelta(minutes=15)
        catchup=False,
) as dag:
    process_errors = BashOperator(
        task_id='process_errors',
        bash_command="cd /dbt_curate && dbt run --models processed_error",
    )

    delete_valid_records_from_error = PythonOperator(
        task_id='delete_valid_records_from_error',
        python_callable=delete_valid_records_from_error,
        provide_context=True
    )

    process_errors >> delete_valid_records_from_error
