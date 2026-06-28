-- =============================================================
-- Audit test: dim_award — compare_column_values
-- Validates that every distinct award_id in the source
-- appears in dim_award with the correct award_name.
-- =============================================================

{%- set source_query -%}
    SELECT DISTINCT
        award_id,
        award_name
    FROM {{ source('dw', 'award_recipients') }}
    WHERE award_id IS NOT NULL
{%- endset -%}

{%- set model_query -%}
    SELECT
        award_id,
        award_name
    FROM {{ ref('dim_award') }}
{%- endset -%}

{{ audit_helper.compare_column_values(
    a_query   = model_query,
    b_query   = source_query,
    primary_key = 'award_id',
    column_to_compare = 'award_name'
) }}
