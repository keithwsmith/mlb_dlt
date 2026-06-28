-- =============================================================
-- Audit test: dim_season — compare_column_values (derived)
-- Validates that regular_season_days is correctly derived from
-- datediff(day, regular_season_start_date, regular_season_end_date).
-- =============================================================

{%- set source_query -%}
    SELECT
        season,
        DATEDIFF(
            day,
            regular_season_start_date,
            regular_season_end_date
        ) AS regular_season_days
    FROM {{ source('dw', 'seasons') }}
{%- endset -%}

{%- set model_query -%}
    SELECT
        season,
        regular_season_days
    FROM {{ ref('dim_season') }}
{%- endset -%}

{{ audit_helper.compare_column_values(
    a_query           = model_query,
    b_query           = source_query,
    primary_key       = 'season',
    column_to_compare = 'regular_season_days'
) }}
