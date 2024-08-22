{{
    config(
        materialized='view'
    )
}}

WITH median_ratings AS (
    SELECT
        movieId,
        PERCENTILE_CONT(rating, 0.5) OVER (PARTITION BY movieId) AS median_rating
    FROM
        {{ source('movies_data_manish', 'ratings_raw_to_curate') }}
)

SELECT
    m.id AS movie_id,
    m.title AS title,
    COUNT(r.rating) AS number_of_ratings,
    mr.median_rating AS median_rating,
    DENSE_RANK() OVER (ORDER BY mr.median_rating DESC) AS rank_movie_by_median_rating
FROM
    {{ source('movies_data_manish', 'movies_raw_to_curate') }} AS m
        LEFT join
    {{ source('movies_data_manish', 'ratings_raw_to_curate') }} AS r
    ON m.id = CAST(r.movieId as STRING)
        LEFT JOIN
    median_ratings AS mr
    ON m.id = CAST(mr.movieId as STRING)
GROUP BY
    m.id,
    m.title,
    mr.median_rating