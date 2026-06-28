-- =============================================================
-- Audit test: dim_draft — compare_all_columns
-- Validates that key player/team columns in dim_draft match
-- the renamed source columns from dw.draft.
-- =============================================================

{%- set source_query -%}
    SELECT
        person__id                          AS person_id,
        person__full_name                   AS person_full_name,
        person__first_name                  AS person_first_name,
        person__last_name                   AS person_last_name,
        team__id                            AS team_id,
        team__name                          AS team_name,
        school__name                        AS school_name,
        draft_type__code                    AS draft_type_code,
        draft_type__description             AS draft_type_description,
        _dlt_id                             AS source_dlt_id
    FROM {{ source('dw', 'draft') }}
{%- endset -%}

{%- set model_query -%}
    SELECT
        person_id,
        person_full_name,
        person_first_name,
        person_last_name,
        team_id,
        team_name,
        school_name,
        draft_type_code,
        draft_type_description,
        source_dlt_id
    FROM {{ ref('dim_draft') }}
{%- endset -%}

{{ audit_helper.compare_all_columns(
    a_query     = model_query,
    b_query     = source_query,
    primary_key = 'source_dlt_id'
) }}
