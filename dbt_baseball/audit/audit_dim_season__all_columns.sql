-- =============================================================
-- Audit test: dim_season — compare_all_columns
-- Near pass-through from dw.seasons; validates that key
-- season milestone dates are preserved correctly.
-- =============================================================

{%- set source_query -%}
    SELECT
        season,
        season_id,
        has_wildcard,
        regular_season_start_date,
        regular_season_end_date,
        spring_start_date,
        spring_end_date,
        post_season_start_date,
        post_season_end_date,
        all_star_date,
        qualifier_plate_appearances,
        qualifier_outs_pitched
    FROM {{ source('dw', 'seasons') }}
{%- endset -%}

{%- set model_query -%}
    SELECT
        season,
        season_id,
        has_wildcard,
        regular_season_start_date,
        regular_season_end_date,
        spring_start_date,
        spring_end_date,
        post_season_start_date,
        post_season_end_date,
        all_star_date,
        qualifier_plate_appearances,
        qualifier_outs_pitched
    FROM {{ ref('dim_season') }}
{%- endset -%}

{{ audit_helper.compare_all_columns(
    a_query     = model_query,
    b_query     = source_query,
    primary_key = 'season'
) }}
