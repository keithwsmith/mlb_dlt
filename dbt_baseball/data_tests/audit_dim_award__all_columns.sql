-- =============================================================
-- Audit test: dim_award — compare_queries
-- Full column-level comparison between the model and a
-- re-derived version of the expected output from source.
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

{{ audit_helper.compare_queries(
    a_query     = model_query,
    b_query     = source_query,
    primary_key = 'award_id'
) }}