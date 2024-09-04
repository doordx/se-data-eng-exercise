resource "google_bigquery_table" "table_ratings_raw" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "ratings_raw"

  labels = {
    env = "default"
  }

  schema = <<EOF
[
  {
    "name": "userId",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "movieId",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "rating",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "timestamp",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "load_date",
    "type": "DATETIME",
    "mode": "NULLABLE"
  },
  {
    "name": "load_id",
    "type": "STRING",
    "mode": "NULLABLE",
    "defaultValueExpression": "GENERATE_UUID()"
  }
]
EOF

}