resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = "movies_data_manish"
  friendly_name               = "movies"
  description                 = "This dataset is for storing movies data"
  location                    = "asia-south1"

  labels = {
    env = "default"
  }
}