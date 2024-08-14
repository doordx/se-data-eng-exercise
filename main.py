import logging
import os
import csv
import pandas as pd
from io import StringIO
from google.cloud import bigquery, storage

def process_movies_csv(event, context):
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

    # Convert all columns to strings
    df = df.astype(str)

    # Set up BigQuery client
    bq_client = bigquery.Client()

    # Define the BigQuery table
    dataset_id = os.getenv('BIGQUERY_DATASET')
    table_id = os.getenv('BIGQUERY_MOVIES_TABLE')

    table_ref = bq_client.dataset(dataset_id).table(table_id)

    # Load the DataFrame to BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()  # Waits for the job to complete

    logging.info(f"Loaded {len(df)} rows into {dataset_id}:{table_id}")

def process_ratings_csv(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    bucket_name = event['bucket']
    file_name = event['name']

    # Ensure the event is for a CSV file matching the pattern
    if not file_name.endswith('.csv') or not file_name.startswith('ratings_'):
        logging.info(f"Skipping non-matching file: {file_name}")
        return

    gcs_client = storage.Client()
    bucket = gcs_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Set up BigQuery client
    bq_client = bigquery.Client()
    dataset_id = os.getenv('BIGQUERY_DATASET')
    table_id = os.getenv('BIGQUERY_RATINGS_TABLE', 'ratings_raw')
    logging.info(f"table env name: {os.getenv('BIGQUERY_RATINGS_TABLE')}")
    logging.info(f"table default name: {table_id}")
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    logging.info(f"table ref: {table_ref}")
    # Initialize job configuration
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    rows_to_insert = []

    # Download the CSV file content as a stream and process line by line
    csv_stream = blob.download_as_text().splitlines()

    # Read the CSV line by line using the CSV reader
    csv_reader = csv.reader(csv_stream)
    header = next(csv_reader)  # Get the header row

    for row in csv_reader:
        # Convert the row to a dictionary with all values as strings
        row_dict = {header[i]: str(row[i]) for i in range(len(header))}
        rows_to_insert.append(row_dict)

        # Insert rows in batches to manage memory usage
        if len(rows_to_insert) >= 1000:  # Adjust the batch size as needed
            bq_client.insert_rows_json(table_ref, rows_to_insert, row_ids=[None]*len(rows_to_insert))
            logging.info(f"Inserted {len(rows_to_insert)} rows into {dataset_id}:{table_id}")
            rows_to_insert.clear()  # Clear the list to free up memory

    # Insert any remaining rows
    if rows_to_insert:
        bq_client.insert_rows_json(table_ref, rows_to_insert, row_ids=[None]*len(rows_to_insert))
        logging.info(f"Inserted {len(rows_to_insert)} rows into {dataset_id}:{table_id}")

    logging.info(f"Completed loading data into {dataset_id}:{table_id}")
