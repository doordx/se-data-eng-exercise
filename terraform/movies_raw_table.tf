resource "google_bigquery_table" "table_movies_raw" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "movies_raw"

  labels = {
    env = "default"
  }

  schema = <<EOF
[
  {
    "name": "adult",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "belongs_to_collection",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "budget",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "genres",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "homepage",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "id",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "imdb_id",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "original_language",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "original_title",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "overview",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "popularity",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "poster_path",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "production_companies",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "production_countries",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "release_date",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "revenue",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "runtime",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "spoken_languages",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "status",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "tagline",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "title",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "video",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "vote_average",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "vote_count",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "load_date",
    "type": "DATETIME",
    "mode": "NULLABLE",
    "defaultValueExpression": "CURRENT_DATETIME"
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