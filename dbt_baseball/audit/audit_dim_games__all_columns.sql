-- =============================================================
-- Audit test: dim_games — compare_all_columns
-- Validates that core game attributes (teams, scores, venue)
-- are correctly mapped from the source.
-- =============================================================

{%- set source_query -%}
    SELECT
        CAST(game_pk AS BIGINT)                AS game_pk,
        TRY_CAST(season AS INT)                AS season,
        CAST(game_type AS VARCHAR(10))         AS game_type,
        CAST(game_date AS DATE)                AS game_date,
        teams__home__team__id                  AS home_team_id,
        teams__home__team__name                AS home_team_name,
        teams__home__score                     AS home_score,
        teams__away__team__id                  AS away_team_id,
        teams__away__team__name                AS away_team_name,
        teams__away__score                     AS away_score,
        venue__id                              AS venue_id,
        venue__name                            AS venue_name
    FROM {{ source('dw', 'games') }}
{%- endset -%}

{%- set model_query -%}
    SELECT
        game_pk,
        season,
        game_type,
        game_date,
        home_team_id,
        home_team_name,
        home_score,
        away_team_id,
        away_team_name,
        away_score,
        venue_id,
        venue_name
    FROM {{ ref('dim_games') }}
{%- endset -%}

{{ audit_helper.compare_all_columns(
    a_query     = model_query,
    b_query     = source_query,
    primary_key = 'game_pk'
) }}
