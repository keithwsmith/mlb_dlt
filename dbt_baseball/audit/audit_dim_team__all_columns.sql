-- =============================================================
-- Audit test: dim_team — compare_all_columns
-- Validates that renamed team columns from dw.teams match
-- the dim_team model output.
-- =============================================================

{%- set source_query -%}
    SELECT
        id                              AS team_id,
        name                            AS team_name,
        team_code,
        abbreviation,
        location_name,
        franchise_name,
        club_name,
        short_name,
        league__id                      AS league_id,
        league__name                    AS league_name,
        division__id                    AS division_id,
        division__name                  AS division_name,
        venue__id                       AS venue_id,
        active                          AS is_active,
        TRY_CAST(first_year_of_play AS INT) AS first_year_of_play,
        season,
        _dlt_id
    FROM {{ source('dw', 'teams') }}
    WHERE id IS NOT NULL
{%- endset -%}

{%- set model_query -%}
    SELECT
        team_id,
        team_name,
        team_code,
        abbreviation,
        location_name,
        franchise_name,
        club_name,
        short_name,
        league_id,
        league_name,
        division_id,
        division_name,
        venue_id,
        is_active,
        first_year_of_play,
        season,
        _dlt_id
    FROM {{ ref('dim_team') }}
{%- endset -%}

{{ audit_helper.compare_all_columns(
    a_query     = model_query,
    b_query     = source_query,
    primary_key = '_dlt_id'
) }}
