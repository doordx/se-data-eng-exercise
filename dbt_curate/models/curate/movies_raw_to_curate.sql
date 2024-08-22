{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (
    SELECT
        CAST(adult as BOOLEAN) AS adult,
        belongs_to_collection,
        budget,
        genres,
        homepage,
        id,
        imdb_id,
        original_language,
        original_title,
        overview,
        popularity,
        poster_path,
        production_companies,
        production_countries,
        release_date,
        revenue,
        runtime,
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