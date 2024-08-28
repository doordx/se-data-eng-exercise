{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

WITH base AS (
    SELECT
        adult,
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
        video,
        vote_average,
        vote_count,
        load_date,
        is_error,
        SAFE_CAST(id AS FLOAT64) AS numeric_id,
    FROM {{ source('movies_data_manish', 'movies_raw') }}
    WHERE {{ '1=1' if is_incremental() else '1=1' }}
)

SELECT
 adult,
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
 video,
 vote_average,
 vote_count,
 load_date,
 CASE
     WHEN numeric_id IS NULL THEN is_error is true
     ELSE is_error
     END AS is_error
FROM base