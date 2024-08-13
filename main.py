import logging
import os
import pandas as pd
from io import StringIO
from google.cloud import bigquery, storage

def process_csv(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    bucket_name = event['bucket']
    file_name = event['name']

    # Ensure the event is for a CSV file matching the pattern
    if not file_name.endswith('.csv') or not file_name.startswith('movies_'):
        logging.info(f"Skipping non-matching file: {file_name}")
        return

    # Load the CSV from GCS
    gcs_client = storage.Client()
    bucket = gcs_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Download the CSV file content
    csv_data = blob.download_as_string()
    logging.info(f"Downloaded file: {file_name} from bucket: {bucket_name}")

    # Load CSV data into a Pandas DataFrame
    df = pd.read_csv(StringIO(csv_data.decode('utf-8')))

    # Set up BigQuery client
    bq_client = bigquery.Client()

    # Define the BigQuery table
    dataset_id = os.getenv('BIGQUERY_DATASET')  # e.g., "your_dataset"
    table_id = os.getenv('BIGQUERY_TABLE')      # e.g., "raw_movies"

    table_ref = bq_client.dataset(dataset_id).table(table_id)

    # Load the DataFrame to BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()  # Waits for the job to complete

    logging.info(f"Loaded {len(df)} rows into {dataset_id}:{table_id}")