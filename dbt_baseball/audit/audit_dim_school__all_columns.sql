-- =============================================================
-- Audit test: dim_school — compare_all_columns
-- Validates that school attributes aggregated from dw.draft
-- match the model output.
-- =============================================================

{%- set source_query -%}
    WITH draft_schools AS (
        SELECT
            school__name,
            MAX(school__city)         AS school__city,
            MAX(school__state)        AS school__state,
            MAX(school__country)      AS school__country,
            MAX(school__school_class) AS school__school_class
        FROM {{ source('dw', 'draft') }}
        WHERE school__name IS NOT NULL
        GROUP BY school__name
    ),
    school_types AS (
        SELECT
            school_name,
            school_type,
            classified_by
        FROM {{ source('dw', 'school_type_lookup') }}
    )
    SELECT
        d.school__name,
        d.school__city,
        d.school__state,
        d.school__country,
        d.school__school_class,
        s.school_type,
        s.classified_by
    FROM draft_schools d
    LEFT JOIN school_types s ON d.school__name = s.school_name
{%- endset -%}

{%- set model_query -%}
    SELECT
        school__name,
        school__city,
        school__state,
        school__country,
        school__school_class,
        school_type,
        classified_by
    FROM {{ ref('dim_school') }}
{%- endset -%}

{{ audit_helper.compare_all_columns(
    a_query     = model_query,
    b_query     = source_query,
    primary_key = 'school__name'
) }}
