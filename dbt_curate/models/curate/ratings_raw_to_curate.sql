{{
    config(
        materialized='incremental',
        unique_key='load_time'
    )
}}

WITH source_data AS (
    SELECT
        CAST(userId as NUMERIC) AS userId,
        CAST(movieId as NUMERIC) AS movieId,
        CAST(rating as FLOAT64) AS rating,
        CAST(timestamp as NUMERIC) AS timestamp,
        CURRENT_TIMESTAMP() AS load_time
    FROM
        {{ source('movies_data_manish', 'ratings_raw') }}
)

SELECT * FROM source_data

{% if is_incremental() %}

  where load_time >= (select coalesce(max(load_time), '1900-01-01') from {{ this }})

{% endif %}