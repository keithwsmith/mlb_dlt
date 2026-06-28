-- =============================================================
-- Audit test: dim_venue — compare_all_columns
-- Simple pass-through from dw.venues; all columns should match.
-- =============================================================

{%- set source_query -%}
    SELECT
        venue_id,
        venue_name,
        venue_link,
        location_name,
        first_year_of_play,
        spring_venue_id,
        spring_venue_link
    FROM {{ source('dw', 'venues') }}
{%- endset -%}

{%- set model_query -%}
    SELECT
        venue_id,
        venue_name,
        venue_link,
        location_name,
        first_year_of_play,
        spring_venue_id,
        spring_venue_link
    FROM {{ ref('dim_venue') }}
{%- endset -%}

{{ audit_helper.compare_all_columns(
    a_query     = model_query,
    b_query     = source_query,
    primary_key = 'venue_id'
) }}
