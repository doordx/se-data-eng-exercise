from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery, storage
from datetime import timezone

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'load_movie',
    default_args=default_args,
    description='Load new movie data from GCS to BigQuery',
    schedule_interval=None,
    catchup=False,
)

max_load_date_query = f"""
    SELECT MAX(load_date) AS latest_load_date
    FROM `ee-india-se-data.movies_data_manish.movies_raw`
    """


def check_for_new_files(**context):
    new_files = []
    for file in get_files():
        if latest_file_present(file):
            new_files.append(file.name)

    if not new_files:
        raise ValueError("No new files found.")

    context['ti'].xcom_push(key='new_files', value=new_files[0])

    print(f"New files to load: {new_files}")


def get_files():
    bucket_name = 'se-data-landing-manish'
    gcs_client = storage.Client()
    bucket = gcs_client.get_bucket(bucket_name)
    files = bucket.list_blobs()
    return files


def latest_file_present(file):
    latest_load_date = get_latest_load_date()

    print(f"Latest load date: {latest_load_date}")

    return file.name.endswith('.csv') and file.name.startswith('airflow_') and (
            latest_load_date is None or file.time_created > latest_load_date)


def get_latest_load_date():
    bq_client = bigquery.Client(location="asia-south1")
    query_job = bq_client.query(max_load_date_query)
    result = query_job.result()
    latest_load_date = None

    for row in result:
        record = dict(row)
        latest_load_date = record.get('latest_load_date')
        if latest_load_date and latest_load_date.tzinfo is None:
            latest_load_date = latest_load_date.replace(tzinfo=timezone.utc)

    return latest_load_date


def get_table_schema(table_ref):
    bq_client = bigquery.Client(location="asia-south1")
    table = bq_client.get_table(table_ref)
    schema = []
    for field in table.schema:
        if field.name not in ['load_date', 'load_id']:
            schema.append({
                'name': field.name,
                'type': field.field_type,
                'mode': field.mode
            })
    return schema


check_files = PythonOperator(
    task_id='check_for_new_files',
    python_callable=check_for_new_files,
    provide_context=True,
    dag=dag,
)

load_data = GCSToBigQueryOperator(
    task_id='load_new_data',
    bucket='se-data-landing-manish',
    source_objects="{{ task_instance.xcom_pull(task_ids='check_for_new_files', key='new_files') }}",
    destination_project_dataset_table='ee-india-se-data.movies_data_manish.movies_raw',
    schema_fields=get_table_schema('ee-india-se-data.movies_data_manish.movies_raw'),
    source_format='CSV',
    write_disposition='WRITE_APPEND',
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,  # Skip the header row if it's present
    dag=dag,
)

check_files >> load_data
