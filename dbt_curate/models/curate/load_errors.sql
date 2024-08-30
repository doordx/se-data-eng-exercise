{{
    config(
        materialized='incremental'
    )
}}

with source_data AS (
    SELECT
        load_id,
        'movies_raw' as table_name,
        CAST(load_date as timestamp) AS load_time
    FROM
        {{ source('movies_data_manish', 'movies_raw') }}

    UNION ALL

    SELECT
        load_id,
        'ratings_raw' as table_name,
        CAST(load_date as timestamp) AS load_time
    FROM
        {{ source('movies_data_manish', 'ratings_raw') }}
)

SELECT * FROM source_data

{% if is_incremental() %}

where load_time >= (select coalesce(max(load_time), '1900-01-01') from {{ this }})

{% endif %}

