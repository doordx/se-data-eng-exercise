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
    WHERE
        REGEXP_CONTAINS(id, '[^0-9]+')
)

SELECT * FROM source_data

{% if is_incremental() %}

where load_time >= (select coalesce(max(load_time), '1900-01-01') from {{ this }})

{% endif %}

