SELECT
    COUNT(1)
FROM
    {{ source('movies_data_manish', 'movies_raw_to_curate') }}
WHERE
    load_time IS NULL

SELECT
    COUNT(1)
FROM
    {{ source('movies_data_manish', 'ratings_raw_to_curate') }}
WHERE
    load_time IS NULL