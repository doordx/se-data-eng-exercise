{{
    config(
        materialized='table'
    )
}}

WITH latest_ratings AS (
    SELECT
        t1.movieId, t1.userId, t1.rating, t1.load_time, t1.timestamp
    FROM
        {{ source('movies_data_manish', 'ratings_raw_to_curate') }} t1
    INNER JOIN
        {{ source('movies_data_manish', 'ratings_raw_to_curate') }} t2
    ON
        (t1.movieId = t2.movieId and t1.userId = t2.userId)
    GROUP BY
        t1.movieId, t1.userId, t1.rating, t1.load_time, t1.timestamp
    ORDER BY
        t1.load_time
)

SELECT
    movieId,
    userId,
    rating,
    timestamp,
    load_time
FROM
    latest_ratings