import logging
import os
import csv
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

    process_csv(bucket_name, file_name, os.getenv('BIGQUERY_MOVIES_TABLE'))

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

    process_csv(bucket_name, file_name, os.getenv('BIGQUERY_RATINGS_TABLE'))

def process_csv(bucket_name, file_name, table_id):
    gcs_client = storage.Client()
    bucket = gcs_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Set up BigQuery client
    bq_client = bigquery.Client()
    dataset_id = os.getenv('BIGQUERY_DATASET')
    table_ref = bq_client.dataset(dataset_id).table(table_id)

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