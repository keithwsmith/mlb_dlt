{{
    config(
        materialized='table',
        unique_key='game_pk'
    )
}}

-- One row per game: each team's box score, built by adding up the
-- player-by-player lines in int_player_game_stats.
--
-- Column names match dim_game_details (home_hits, away_pitching_walks, ...)
-- so fact_games can use either source. Extra columns that dim_game_details
-- doesn't have: {side}_at_bats, {side}_pitching_outs (outs recorded;
-- innings = outs / 3), and {side}_errors -- real fielding errors, which the
-- team-level feed never provided.
--
-- Deliberately NOT included: left_on_base. A team's box-score LOB is NOT
-- the sum of its players' LOB (the player figure counts runners stranded
-- per at-bat, so the same runner is counted repeatedly), so summing it
-- would silently overstate team LOB.
--
-- player_rows is how many player lines went into each game -- useful for
-- spotting duplicate loads (a normal game is roughly 20-50).

SELECT
    game_pk,

    -- home team: batting
    SUM(CASE WHEN is_home = 1 THEN batting_runs END)                AS home_runs,
    SUM(CASE WHEN is_home = 1 THEN batting_hits END)                AS home_hits,
    SUM(CASE WHEN is_home = 1 THEN batting_doubles END)             AS home_doubles,
    SUM(CASE WHEN is_home = 1 THEN batting_triples END)             AS home_triples,
    SUM(CASE WHEN is_home = 1 THEN batting_home_runs END)           AS home_home_runs,
    SUM(CASE WHEN is_home = 1 THEN batting_rbi END)                 AS home_rbi,
    SUM(CASE WHEN is_home = 1 THEN batting_stolen_bases END)        AS home_stolen_bases,
    SUM(CASE WHEN is_home = 1 THEN batting_strikeouts END)          AS home_strikeouts,
    SUM(CASE WHEN is_home = 1 THEN batting_walks END)               AS home_walks,
    SUM(CASE WHEN is_home = 1 THEN batting_at_bats END)             AS home_at_bats,
    -- home team: pitching staff / fielding
    SUM(CASE WHEN is_home = 1 THEN pitching_strikeouts END)         AS home_pitching_strikeouts,
    SUM(CASE WHEN is_home = 1 THEN pitching_walks END)              AS home_pitching_walks,
    SUM(CASE WHEN is_home = 1 THEN pitching_hits_allowed END)       AS home_pitching_hits_allowed,
    SUM(CASE WHEN is_home = 1 THEN pitching_runs_allowed END)       AS home_pitching_runs_allowed,
    SUM(CASE WHEN is_home = 1 THEN pitching_earned_runs END)        AS home_pitching_earned_runs,
    SUM(CASE WHEN is_home = 1 THEN pitching_home_runs_allowed END)  AS home_pitching_home_runs_allowed,
    SUM(CASE WHEN is_home = 1 THEN pitching_outs_recorded END)      AS home_pitching_outs,
    SUM(CASE WHEN is_home = 1 THEN fielding_errors END)             AS home_errors,

    -- away team: batting
    SUM(CASE WHEN is_home = 0 THEN batting_runs END)                AS away_runs,
    SUM(CASE WHEN is_home = 0 THEN batting_hits END)                AS away_hits,
    SUM(CASE WHEN is_home = 0 THEN batting_doubles END)             AS away_doubles,
    SUM(CASE WHEN is_home = 0 THEN batting_triples END)             AS away_triples,
    SUM(CASE WHEN is_home = 0 THEN batting_home_runs END)           AS away_home_runs,
    SUM(CASE WHEN is_home = 0 THEN batting_rbi END)                 AS away_rbi,
    SUM(CASE WHEN is_home = 0 THEN batting_stolen_bases END)        AS away_stolen_bases,
    SUM(CASE WHEN is_home = 0 THEN batting_strikeouts END)          AS away_strikeouts,
    SUM(CASE WHEN is_home = 0 THEN batting_walks END)               AS away_walks,
    SUM(CASE WHEN is_home = 0 THEN batting_at_bats END)             AS away_at_bats,
    -- away team: pitching staff / fielding
    SUM(CASE WHEN is_home = 0 THEN pitching_strikeouts END)         AS away_pitching_strikeouts,
    SUM(CASE WHEN is_home = 0 THEN pitching_walks END)              AS away_pitching_walks,
    SUM(CASE WHEN is_home = 0 THEN pitching_hits_allowed END)       AS away_pitching_hits_allowed,
    SUM(CASE WHEN is_home = 0 THEN pitching_runs_allowed END)       AS away_pitching_runs_allowed,
    SUM(CASE WHEN is_home = 0 THEN pitching_earned_runs END)        AS away_pitching_earned_runs,
    SUM(CASE WHEN is_home = 0 THEN pitching_home_runs_allowed END)  AS away_pitching_home_runs_allowed,
    SUM(CASE WHEN is_home = 0 THEN pitching_outs_recorded END)      AS away_pitching_outs,
    SUM(CASE WHEN is_home = 0 THEN fielding_errors END)             AS away_errors,

    COUNT(*)                                                    AS player_rows
FROM {{ ref('int_player_game_stats') }}
GROUP BY game_pk
