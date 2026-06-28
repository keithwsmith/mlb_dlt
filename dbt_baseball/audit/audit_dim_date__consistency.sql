-- =============================================================
-- Audit test: dim_date — compare_queries
-- Validates that calculated date attributes (day_of_week,
-- month_number, year) are internally consistent with
-- date_actual and date_key.
-- =============================================================

{%- set expected_query -%}
    SELECT
        date_key,
        DATEPART(WEEKDAY, date_actual)  AS day_of_week,
        DATEPART(MONTH, date_actual)    AS month_number,
        DATEPART(YEAR, date_actual)     AS year
    FROM {{ ref('dim_date') }}
{%- endset -%}

{%- set model_query -%}
    SELECT
        date_key,
        day_of_week,
        month_number,
        year
    FROM {{ ref('dim_date') }}
{%- endset -%}

{{ audit_helper.compare_queries(
    a_query = model_query,
    b_query = expected_query,
    primary_key = 'date_key'
) }}
