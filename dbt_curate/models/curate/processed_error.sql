{{
    config(
        materialized='table'
    )
}}

with source_data AS (
    SELECT
        *
    FROM
        {{ source('movies_data_manish', 'movies_raw') }}
    WHERE
        REGEXP_CONTAINS(id, '[0-9]+')
        AND load_id in (
            SELECT
             load_id
            FROM
                {{ source('movies_data_manish', 'load_errors') }}
        )
)

SELECT * FROM source_data