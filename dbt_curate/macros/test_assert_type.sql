{% macro test_assert_type(model, column_name, expected_type) %}
SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND NOT SAFE_CAST({{ column_name }} AS {{ expected_type }}) IS NOT NULL
    {% endmacro %}
