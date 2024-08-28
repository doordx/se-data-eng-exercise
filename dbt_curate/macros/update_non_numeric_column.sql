-- macros/update_non_numeric_column.sql

{% macro update_non_numeric_column() %}

UPDATE `{{ source('movies_data_manish', 'movies_raw') }}`
SET is_error = TRUE
WHERE SAFE_CAST(id AS FLOAT64) IS NULL

{% endmacro %}
