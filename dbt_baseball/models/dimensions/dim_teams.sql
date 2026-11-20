{{
    config(
        materialized='table',
        unique_key='team_id'
    )
}}

{#
    dim_teams -- one row per team per season.

    MLB teams come from dw.teams; minor league teams (AAA, AA, High-A,
    Single-A, Rookie) from dw.milb_teams (mlb_load.py milb_teams).

    New columns:
      level            'MLB', 'AAA', 'AA', 'High-A', 'Single-A', 'Rookie'
                       (from sport_id), same values as dim_player.level.
      parent_org_id    MLB organization the team belongs to. For MLB teams
      parent_org_name  it is the team itself, so every row rolls up to an
                       MLB organization the same way.

    dw.milb_teams has no spring training columns, so those are NULL for
    minor league teams. A team/season that is already in dw.teams is not
    added again from dw.milb_teams, so team_key stays unique.

    Models that join on team_id + season (fact_games, fact_player_stats)
    are unaffected. Anything that should list MLB teams only must now
    filter on level = 'MLB' (or sport_id = 1).

    Unknown member: one extra row with team_id = 0 ("Unknown"), for source
    rows that carry team_id 0 meaning "no team" (e.g. Hall of Fame and
    executive awards in dim_award_recipient). It has season 0 and
    level 'Unknown', so it never matches a real team + season join and
    is easy to filter out (WHERE team_id <> 0).
#}

WITH mlb_teams AS (
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
        id AS parent_org_id,                -- an MLB team is its own organization
        name AS parent_org_name,
        season,
        _dlt_load_id,
        _dlt_id
    FROM {{ source('dw', 'teams') }}
    WHERE id IS NOT NULL
),

milb_teams AS (
    SELECT
        m.id AS team_id,
        m.name AS team_name,
        m.team_code,
        m.file_code,
        m.abbreviation,
        m.location_name,
        -- Some minor league clubs have no franchise/club/short name in the
        -- API; fall back so the existing not_null tests still hold.
        COALESCE(m.franchise_name, m.location_name, m.name) AS franchise_name,
        COALESCE(m.club_name, m.team_name, m.name)          AS club_name,
        COALESCE(m.short_name, m.name)                      AS short_name,
        m.league__id AS league_id,
        m.league__name AS league_name,
        m.division__id AS division_id,
        m.division__name AS division_name,
        m.sport__id AS sport_id,
        m.sport__name AS sport_name,
        m.venue__id AS venue_id,
        CAST(NULL AS BIGINT)        AS spring_venue_id,
        CAST(NULL AS BIGINT)        AS spring_league_id,
        CAST(NULL AS NVARCHAR(255)) AS spring_league_name,
        CAST(NULL AS NVARCHAR(50))  AS spring_league_abbreviation,
        m.all_star_status,
        m.active AS is_active,
        TRY_CAST(m.first_year_of_play AS INT) AS first_year_of_play,
        m.parent_org_id,
        m.parent_org_name,
        m.season,
        m._dlt_load_id,
        m._dlt_id
    FROM {{ source('dw', 'milb_teams') }} m
    WHERE m.id IS NOT NULL
      -- don't duplicate a team/season dw.teams already has
      AND NOT EXISTS (
          SELECT 1
          FROM {{ source('dw', 'teams') }} t
          WHERE t.id = m.id
            AND t.season = m.season
      )
),

-- Unknown member for team_id = 0. Column order must match the CTEs above.
unknown_team AS (
    SELECT
        CAST(0 AS BIGINT)               AS team_id,
        CAST('Unknown' AS NVARCHAR(255)) AS team_name,
        CAST('UNK' AS NVARCHAR(50))     AS team_code,
        CAST('UNK' AS NVARCHAR(50))     AS file_code,
        CAST('UNK' AS NVARCHAR(50))     AS abbreviation,
        CAST('Unknown' AS NVARCHAR(255)) AS location_name,
        CAST('Unknown' AS NVARCHAR(255)) AS franchise_name,
        CAST('Unknown' AS NVARCHAR(255)) AS club_name,
        CAST('Unknown' AS NVARCHAR(255)) AS short_name,
        CAST(NULL AS BIGINT)            AS league_id,
        CAST('Unknown' AS NVARCHAR(255)) AS league_name,
        CAST(NULL AS BIGINT)            AS division_id,
        CAST('Unknown' AS NVARCHAR(255)) AS division_name,
        CAST(0 AS BIGINT)               AS sport_id,
        CAST('Unknown' AS NVARCHAR(255)) AS sport_name,
        CAST(NULL AS BIGINT)            AS venue_id,
        CAST(NULL AS BIGINT)            AS spring_venue_id,
        CAST(NULL AS BIGINT)            AS spring_league_id,
        CAST(NULL AS NVARCHAR(255))     AS spring_league_name,
        CAST(NULL AS NVARCHAR(50))      AS spring_league_abbreviation,
        CAST('N' AS NVARCHAR(10))       AS all_star_status,
        CAST(0 AS BIT)                  AS is_active,
        CAST(NULL AS INT)               AS first_year_of_play,
        CAST(NULL AS BIGINT)            AS parent_org_id,
        CAST('Unknown' AS NVARCHAR(255)) AS parent_org_name,
        CAST(0 AS BIGINT)               AS season,
        CAST('unknown_member' AS NVARCHAR(64))  AS _dlt_load_id,
        CAST('unknown_member_0' AS NVARCHAR(64)) AS _dlt_id
    -- skip it if a real source ever sends team id 0
    WHERE NOT EXISTS (SELECT 1 FROM {{ source('dw', 'teams') }} WHERE id = 0)
      AND NOT EXISTS (SELECT 1 FROM {{ source('dw', 'milb_teams') }} WHERE id = 0)
),

source_teams AS (
    SELECT * FROM mlb_teams
    UNION ALL
    SELECT * FROM milb_teams
    UNION ALL
    SELECT * FROM unknown_team
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['team_id', 'season']) }}
                        AS team_key,
    team_id,
    team_name,
    team_code,
    file_code,
    abbreviation,
    COALESCE(location_name, 'Unknown') AS location_name,
    franchise_name,
    club_name,
    short_name,
    league_id,
    COALESCE(league_name, 'Unknown') AS league_name,
    division_id,
    COALESCE(division_name, 'Unknown') AS division_name,
    sport_id,
    sport_name,
    CASE sport_id
        WHEN 0  THEN 'Unknown'
        WHEN 1  THEN 'MLB'
        WHEN 11 THEN 'AAA'
        WHEN 12 THEN 'AA'
        WHEN 13 THEN 'High-A'
        WHEN 14 THEN 'Single-A'
        WHEN 16 THEN 'Rookie'
        ELSE 'Other'
    END AS level,
    parent_org_id,
    parent_org_name,
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
