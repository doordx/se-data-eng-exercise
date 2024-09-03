{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (
    SELECT
        CAST(adult as BOOLEAN) AS adult,
        belongs_to_collection,
        CAST(budget as NUMERIC) AS budget,
        genres,
        homepage,
        CAST(id as NUMERIC) AS id,
        CAST(imdb_id as NUMERIC) AS imdb_id,
        original_language,
        original_title,
        overview,
        CAST(popularity as FLOAT42) AS popularity,
        poster_path,
        production_companies,
        production_countries,
        release_date,
        CAST(revenue as NUMERIC) AS revenue,
        CAST(runtime as NUMERIC) AS runtime,
        spoken_languages,
        status,
        tagline,
        title,
        CAST(video AS BOOLEAN) AS video,
        vote_average,
        vote_count,
        CURRENT_TIMESTAMP() AS load_time
    FROM
        {{ source('movies_data_manish', 'movies_raw') }}
)

SELECT * FROM source_data