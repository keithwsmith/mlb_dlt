-- =============================================================
-- Audit test: dim_date — compare_column_values
-- Validates that date_key values are unique and that
-- baseball_season correctly maps from the seasons source.
-- Since dim_date is generated from a date spine, we compare
-- a sample of season-tagged dates against the source.
-- =============================================================

{%- set source_query -%}
    SELECT
        CAST(FORMAT(CAST(s.regular_season_start_date AS DATE), 'yyyyMMdd') AS INT) AS date_key,
        s.season AS baseball_season
    FROM {{ source('dw', 'seasons') }} s
    WHERE s.regular_season_start_date IS NOT NULL
{%- endset -%}

{%- set model_query -%}
    SELECT
        date_key,
        baseball_season
    FROM {{ ref('dim_date') }}
    WHERE is_regular_season = 1
      AND day_of_month = 1
      AND baseball_season IS NOT NULL
{%- endset -%}

{{ audit_helper.compare_column_values(
    a_query           = source_query,
    b_query           = model_query,
    primary_key       = 'date_key',
    column_to_compare = 'baseball_season'
) }}
