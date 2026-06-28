{{
    config(
        materialized='table',
        unique_key='team_id'
    )
}}

WITH source_teams AS (
    SELECT
        id AS team_id,
        name AS team_name,
        team_code,
        file_code,
        abbreviation,
        location_name,
        franchise_name,
        club_name,
        short_name,
        league__id AS league_id,
        league__name AS league_name,
        division__id AS division_id,
        division__name AS division_name,
        sport__id AS sport_id,
        sport__name AS sport_name,
        venue__id AS venue_id,
        spring_venue__id AS spring_venue_id,
        spring_league__id AS spring_league_id,
        spring_league__name AS spring_league_name,
        spring_league__abbreviation AS spring_league_abbreviation,
        all_star_status,
        active AS is_active,
        TRY_CAST(first_year_of_play AS INT) AS first_year_of_play,
        season,
        _dlt_load_id,
        _dlt_id
    FROM {{ source('dw', 'teams') }}
    WHERE id IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['team_id', 'season']) }}
                        AS team_key,
    team_id,
    team_name,
    team_code,
    file_code,
    abbreviation,
    location_name,
    franchise_name,
    club_name,
    short_name,
    league_id,
    league_name,
    division_id,
    division_name,
    sport_id,
    sport_name,
    venue_id,
    spring_venue_id,
    spring_league_id,
    spring_league_name,
    spring_league_abbreviation,
    all_star_status,
    is_active,
    first_year_of_play,
    season,
    GETDATE() AS created_at,
    GETDATE() AS updated_at
FROM source_teams