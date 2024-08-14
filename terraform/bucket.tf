provider "google" {
  project = "ee-india-se-data"
  region  = "asia-south1"
}

resource "google_storage_bucket" "bucket" {
  name     = "se-data-landing-manish"
  location = "asia-south1"

  force_destroy = false

  uniform_bucket_level_access = false
}