-- =============================================================
-- Audit test: dim_pitch_type — compare_all_columns
-- Simple pass-through; validates that pitch_type_code and
-- pitch_description match the source exactly.
-- =============================================================

{%- set source_query -%}
    SELECT
        pitch_type      AS pitch_type_code,
        pitch_description
    FROM {{ source('dw', 'pitch_type') }}
    WHERE pitch_type IS NOT NULL
{%- endset -%}

{%- set model_query -%}
    SELECT
        pitch_type_code,
        pitch_description
    FROM {{ ref('dim_pitch_type') }}
{%- endset -%}

{{ audit_helper.compare_all_columns(
    a_query     = model_query,
    b_query     = source_query,
    primary_key = 'pitch_type_code'
) }}
