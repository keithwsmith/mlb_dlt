{{
    config(
        materialized='incremental',
        unique_key='game_pk'
    )
}}

WITH games AS (
    SELECT *
    FROM {{ ref('dim_games') }}
    {% if is_incremental() %}
        WHERE load_id > (SELECT MAX(load_id) FROM {{ this }})
    {% endif %}
)

-- FIXES APPLIED:
-- 1. home_team_key/away_team_key now select dt_home.team_key /
--    dt_away.team_key (the genuine surrogate key) instead of
--    dt_home.team_id / dt_away.team_id -- every other "_key" column in
--    this model already outputs a real surrogate key; these two were the
--    only ones actually outputting a natural key under a "_key" name.
--    This also means the join to dim_teams below no longer needs the
--    compound (team_id, season) pattern -- team_key alone is already
--    unique.
-- 2. home_team_errors/away_team_errors renamed to
--    home_pitching_runs_allowed/away_pitching_runs_allowed -- the
--    underlying data was always pitching runs allowed, not fielding
--    errors (dim_game_details has no errors column at all). Kept the
--    data, fixed the misleading name instead of inventing an errors stat
--    that doesn't exist upstream.
-- 3. The dim_game_status join now matches on BOTH coded_game_state and
--    status_code -- dim_game_status's real unique key is that pair
--    together, not status_code alone. The old join could match the
--    wrong row (or fan out) if any status_code repeats across different
--    coded game states.

SELECT
    g.game_pk,
	g.season,

    -- Date dimension keys
    dd_game.date_key                            AS game_date_key,
    dd_official.date_key                        AS official_date_key,

    -- Team dimension keys
    dt_home.team_key                            AS home_team_key,
    dt_away.team_key                            AS away_team_key,

    -- Venue dimension key
    dv.venue_key,

    -- Season dimension key
    ds.season_key,

    -- Game type dimension key
    dgt.game_type_key,

    -- Game status dimension key
    dgst.game_status_key,

    -- Scores (from game_details instead of NULL)
    gd.home_runs                                AS home_team_runs,
    gd.away_runs                                AS away_team_runs,
    gd.home_hits                                AS home_team_hits,
    gd.away_hits                                AS away_team_hits,
    gd.home_pitching_runs_allowed                AS home_pitching_runs_allowed,
    gd.away_pitching_runs_allowed                AS away_pitching_runs_allowed,

    -- Batting details
    gd.home_doubles,
    gd.home_triples,
    gd.home_home_runs,
    gd.home_rbi,
    gd.home_stolen_bases,
    gd.home_strikeouts                          AS home_batting_strikeouts,
    gd.home_walks                               AS home_batting_walks,
    gd.home_left_on_base,
    gd.away_doubles,
    gd.away_triples,
    gd.away_home_runs,
    gd.away_rbi,
    gd.away_stolen_bases,
    gd.away_strikeouts                          AS away_batting_strikeouts,
    gd.away_walks                               AS away_batting_walks,
    gd.away_left_on_base,

    -- Pitching details
    gd.home_pitching_strikeouts,
    gd.home_pitching_walks,
    gd.home_pitching_hits_allowed,
    gd.home_pitching_earned_runs,
    gd.home_pitching_home_runs_allowed,
    gd.away_pitching_strikeouts,
    gd.away_pitching_walks,
    gd.away_pitching_hits_allowed,
    gd.away_pitching_earned_runs,
    gd.away_pitching_home_runs_allowed,

    -- Winners/losers
    g.winning_team_id,
    g.losing_team_id,
    g.run_diff,
    g.is_final,

    -- Pitcher decisions (from game_details)
    gd.winning_pitcher_id,
    gd.winning_pitcher_name,
    gd.losing_pitcher_id,
    gd.losing_pitcher_name,
    gd.save_pitcher_id,
    gd.save_pitcher_name,

    -- Attendance & weather (from game_details)
    gd.attendance,
    gd.game_duration_minutes,
    gd.first_pitch,
    gd.weather_condition,
    gd.weather_temp,
    gd.weather_wind,

    -- Flags
    -- FIX (edge case): these previously had no guard on the game actually
    -- being finished, so a rained-out/suspended game with a partial 0-hit
    -- or 0-0 line was flagged the same as a real no-hitter/shutout.
    CASE
        WHEN g.is_final = 1
         AND (gd.home_hits = 0 OR gd.away_hits = 0)
        THEN 1 ELSE 0
    END                                         AS is_no_hitter,
    -- NOTE: dw.game_details has no hit-by-pitch, error, or catcher's-
    -- interference columns, so a batter who reached base by one of those
    -- means with 0 hits/0 walks still can't be distinguished here. This
    -- flag is "no hits and no walks in a completed game", which is a
    -- necessary but not sufficient condition for an actual perfect game —
    -- closing that gap requires adding those columns upstream, not just a
    -- SQL change.
    CASE
        WHEN g.is_final = 1
         AND (
                (gd.home_hits = 0 AND gd.home_walks = 0)
             OR (gd.away_hits = 0 AND gd.away_walks = 0)
             )
        THEN 1 ELSE 0
    END                                         AS is_perfect_game,
    CASE
        WHEN g.is_final = 1
         AND (g.home_score = 0 OR g.away_score = 0)
        THEN 1 ELSE 0
    END                                         AS is_shutout,
   
    g.is_tie,

    -- Audit
    g.load_id,
    g._dlt_id,
    GETDATE()                                   AS created_at,
    GETDATE()                                   AS updated_at

FROM games g

-- Game details (weather, attendance, decisions, stats)
LEFT JOIN {{ ref('dim_game_details') }} gd
    ON g.game_pk = gd.game_pk

-- Date dimension: game_date
LEFT JOIN {{ ref('dim_date') }} dd_game
    ON CAST(g.game_date AS DATE) = dd_game.date_actual

-- Date dimension: official_date
LEFT JOIN {{ ref('dim_date') }} dd_official
    ON CAST(g.official_date AS DATE) = dd_official.date_actual

-- Team dimension: home team (single-column join on the surrogate key,
-- now that team_key is selected above instead of team_id -- team_key
-- already encodes team_id+season uniquely)
LEFT JOIN {{ ref('dim_teams') }} dt_home
    ON g.home_team_id = dt_home.team_id
    AND CAST(g.season AS INT) = dt_home.season

-- Team dimension: away team
LEFT JOIN {{ ref('dim_teams') }} dt_away
    ON g.away_team_id = dt_away.team_id
    AND CAST(g.season AS INT) = dt_away.season

-- Venue dimension
LEFT JOIN {{ ref('dim_venue') }} dv
    ON g.venue_id = dv.venue_id

-- Season dimension
LEFT JOIN {{ ref('dim_season') }} ds
    ON CAST(g.season AS INT) = ds.season

-- Game type dimension
LEFT JOIN {{ ref('dim_game_type') }} dgt
    ON g.game_type = dgt.game_type_code

-- Game status dimension: matches BOTH parts of dim_game_status's real
-- unique key (coded_game_state, status_code) -- see FIX 3 note above.
LEFT JOIN {{ ref('dim_game_status') }} dgst
    ON g.coded_game_state = dgst.status__coded_game_state
    AND g.status_code = dgst.status__status_code

