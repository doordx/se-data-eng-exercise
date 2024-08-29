{{
    config(
        materialized='table'
    )
}}

WITH latest_ratings AS (
    SELECT
        t1.*
    FROM
        {{ source('movies_data_manish', 'ratings_raw_to_curate') }} t1
    WHERE
        t1.load_time = (
            SELECT
                max(t2.load_time)
            FROM
                {{ source('movies_data_manish', 'ratings_raw_to_curate') }} t2
            WHERE
                t1.movieId = t2.movieId and t1.userId = t2.userId
            )
)

SELECT
    movieId,
    userId,
    rating,
    timestamp,
    load_time
FROM
    latest_ratings