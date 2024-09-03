import unittest
from unittest.mock import patch, MagicMock
import os

from main import process_movies_csv

class MainTest(unittest.TestCase):

    @patch('main.storage.Client')
    @patch('main.bigquery.Client')
    @patch('main.logging')
    def test_should_process_valid_movies_csv(self, mock_logging, mock_bigquery_client, mock_storage_client):
        event = {
            'bucket': 'test-bucket',
            'name': 'movies_valid.csv'
        }
        context = {}

        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.download_as_string.return_value = b'id,title\ntt0111161,The Shawshank Redemption\n'

        mock_bucket.blob.return_value = mock_blob
        mock_storage_client.return_value.get_bucket.return_value = mock_bucket

        mock_bq_client = MagicMock()
        mock_load_job = MagicMock()
        mock_load_job.result.return_value = None
        mock_bq_client.load_table_from_dataframe.return_value = mock_load_job
        mock_bigquery_client.return_value = mock_bq_client

        os.environ['BIGQUERY_DATASET'] = 'test_dataset'
        os.environ['BIGQUERY_MOVIES_TABLE'] = 'test_table'

        process_movies_csv(event, context)

        mock_blob.download_as_string.assert_called_once()
        mock_bq_client.load_table_from_dataframe.assert_called_once()
        mock_logging.info.assert_called_with(f"Loaded 1 rows into test_dataset:test_table")

    @patch('main.logging')
    def test_should_not_process_non_csv(self, mock_logging):
        event = {
            'bucket': 'test-bucket',
            'name': 'invalid_movies.txt'
        }
        context = {}

        process_movies_csv(event, context)

        mock_logging.info.assert_called_with("Skipping non-matching file: invalid_movies.txt")

    @patch('main.logging')
    def test_should_not_process_file_not_start_movies(self, mock_logging):
        event = {
            'bucket': 'test-bucket',
            'name': 'not_valid_movies.csv'
        }
        context = {}

        process_movies_csv(event, context)

        mock_logging.info.assert_called_with("Skipping non-matching file: not_valid_movies.csv")

if __name__ == '__main__':
    unittest.main()