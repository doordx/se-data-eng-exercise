resource "google_storage_bucket_object" "function_zip" {
  name   = "movies-function-source.zip"
  bucket = google_storage_bucket.bucket.name
  source = "/Users/manishkumarkhatri/personal/se-data-eng-exercise/src/main/movies-function-source.zip" # Path to function zip file
}

resource "google_cloudfunctions_function" "movie_upload_function" {
  name        = "movie-upload-function"
  runtime     = "python310"
  entry_point = "process_csv"

  source_archive_bucket = google_storage_bucket.bucket.name
  source_archive_object = google_storage_bucket_object.function_zip.name

  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = google_storage_bucket.bucket.name
  }

  environment_variables = {
    "PATTERN" = "movies_*.csv"  # Pattern to match the files
  }
}

resource "google_storage_bucket_iam_binding" "function_binding" {
  bucket = google_storage_bucket.bucket.name

  role    = "roles/storage.objectViewer"
  members = ["serviceAccount:${google_cloudfunctions_function.movie_upload_function.service_account_email}"]
}

resource "google_storage_bucket_object" "uploaded_file" {
  name   = "movies_${timestamp()}.csv"
  bucket = google_storage_bucket.bucket.name
  source = "/Users/manishkumarkhatri/personal/se-data-eng-exercise/src/main/resources/movies_all.csv"
}

locals {
  timestamp = formatdate("YYYYMMDDHH_mm_ss", timestamp())
}