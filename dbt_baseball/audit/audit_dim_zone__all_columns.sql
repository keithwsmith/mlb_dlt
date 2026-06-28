-- =============================================================
-- Audit test: dim_zone — compare_all_columns
-- Simple pass-through from dw.Zones; validates column renames.
-- =============================================================

{%- set source_query -%}
    SELECT
        id              AS zone_id,
        zone            AS zone_number,
        [description]   AS zone_description
    FROM {{ source('dw', 'Zones') }}
{%- endset -%}

{%- set model_query -%}
    SELECT
        zone_id,
        zone_number,
        zone_description
    FROM {{ ref('dim_zone') }}
{%- endset -%}

{{ audit_helper.compare_all_columns(
    a_query     = model_query,
    b_query     = source_query,
    primary_key = 'zone_id'
) }}
