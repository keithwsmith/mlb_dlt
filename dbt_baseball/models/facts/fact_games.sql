{{
    config(
        materialized='table'
    )
}}

{#
    fact_games -- one row per game.

    CHANGES (this revision)
    -----------------------
    1. materialized='table' (was incremental on dim_games.load_id).
       The incremental filter only saw a game again when its dim_games row
       changed, so a box score (int_team_game_box) or game_details row that
       landed in a LATER load than the game itself was never picked up and
       the game stayed has_box_score = 0 forever. At ~150k rows a full
       rebuild takes seconds and removes the "needs --full-refresh" class of
       problem entirely.

    2. is_no_hitter / is_perfect_game now cross-check the OTHER team's
       pitching lines. Batting and pitching sums come from different
       players' rows, so missing/stale batting rows for a team that scored
       0 (which still pass the runs-match guard, since 0 = 0) can no longer
       fake a 0-hit game. Both also require the official 27-out minimum,
       so an 8-inning road no-hitter in a loss is no longer counted.
       is_perfect_game additionally requires 0 fielding errors by the
       pitching team and (when known) 0 left on base, which catches most
       hit-by-pitch / catcher's-interference baserunners.

    3. Final runs are computed once (CROSS APPLY r) and reused by the runs
       columns, is_shutout, and the LOB guard, so they can't drift apart.

    4. Stale FIX comments removed (they described a single-column team
       join that the code never used, and an errors column that is now
       real).

    Sources
    -------
      dim_games         schedule feed: teams, scores, status, winner/loser
      int_team_game_box player lines summed per team (all box-score stats)
      dim_game_details  LOB, weather, attendance, pitcher decisions

    The box score is only joined when its runs agree with the final score
    (see join below); a rejected box score shows as NULL stats with
    has_box_score = 0. Note int_player_game_stats is regular-season only,
    so non-regular-season games always have has_box_score = 0.
#}

SELECT
    g.game_pk,
    g.season,

    -- Dimension keys
    dd_game.date_key                            AS game_date_key,
    dd_official.date_key                        AS official_date_key,
    dt_home.team_key                            AS home_team_key,
    dt_away.team_key                            AS away_team_key,
    dv.venue_key,
    ds.season_key,
    dgt.game_type_key,
    dgst.game_status_key,

    -- Scores: schedule feed first, box score only when the feed has none
    r.home_runs                                 AS home_team_runs,
    r.away_runs                                 AS away_team_runs,
    bx.home_hits                                AS home_team_hits,
    bx.away_hits                                AS away_team_hits,
    bx.home_pitching_runs_allowed,
    bx.away_pitching_runs_allowed,

    -- Batting
    bx.home_doubles,
    bx.home_triples,
    bx.home_home_runs,
    bx.home_rbi,
    bx.home_stolen_bases,
    bx.home_strikeouts                          AS home_batting_strikeouts,
    bx.home_walks                               AS home_batting_walks,
    CASE WHEN r.gd_agrees = 1 THEN gd.home_left_on_base END AS home_left_on_base,
    bx.away_doubles,
    bx.away_triples,
    bx.away_home_runs,
    bx.away_rbi,
    bx.away_stolen_bases,
    bx.away_strikeouts                          AS away_batting_strikeouts,
    bx.away_walks                               AS away_batting_walks,
    CASE WHEN r.gd_agrees = 1 THEN gd.away_left_on_base END AS away_left_on_base,

    -- Pitching (home_pitching_* = the HOME team's staff)
    bx.home_pitching_strikeouts,
    bx.home_pitching_walks,
    bx.home_pitching_hits_allowed,
    bx.home_pitching_earned_runs,
    bx.home_pitching_home_runs_allowed,
    bx.away_pitching_strikeouts,
    bx.away_pitching_walks,
    bx.away_pitching_hits_allowed,
    bx.away_pitching_earned_runs,
    bx.away_pitching_home_runs_allowed,

    -- Only available from the player lines
    bx.home_at_bats,
    bx.away_at_bats,
    bx.home_pitching_outs,
    bx.away_pitching_outs,
    bx.home_errors,
    bx.away_errors,

    -- Outcome
    g.winning_team_id,
    g.losing_team_id,
    g.run_diff,
    g.is_final,

    -- Pitcher decisions
    gd.winning_pitcher_id,
    gd.winning_pitcher_name,
    gd.losing_pitcher_id,
    gd.losing_pitcher_name,
    gd.save_pitcher_id,
    gd.save_pitcher_name,

    -- Attendance & weather
    gd.attendance,
    gd.game_duration_minutes,
    gd.first_pitch,
    gd.weather_condition,
    gd.weather_temp,
    gd.weather_wind,

    -- Flags ---------------------------------------------------------------
    -- No-hitter: a side with 0 hits, confirmed by the opposing staff's
    -- hits allowed, over at least 27 outs (official definition).
    CASE
        WHEN g.is_final = 1 AND (
               (bx.home_hits = 0 AND bx.away_pitching_hits_allowed = 0 AND bx.away_pitching_outs >= 27)
            OR (bx.away_hits = 0 AND bx.home_pitching_hits_allowed = 0 AND bx.home_pitching_outs >= 27)
        )
        THEN 1 ELSE 0
    END                                         AS is_no_hitter,

    -- Perfect game: no-hitter conditions + no walks, no errors by the
    -- pitching team, and (when known) nobody left on base. HBP still isn't
    -- in the source, but a hit batter who isn't erased leaves LOB > 0.
    CASE
        WHEN g.is_final = 1 AND (
               (bx.home_hits = 0 AND bx.home_walks = 0
                AND bx.away_pitching_hits_allowed = 0 AND bx.away_pitching_walks = 0
                AND bx.away_errors = 0 AND bx.away_pitching_outs >= 27
                AND ISNULL(gd.home_left_on_base, 0) = 0)
            OR (bx.away_hits = 0 AND bx.away_walks = 0
                AND bx.home_pitching_hits_allowed = 0 AND bx.home_pitching_walks = 0
                AND bx.home_errors = 0 AND bx.home_pitching_outs >= 27
                AND ISNULL(gd.away_left_on_base, 0) = 0)
        )
        THEN 1 ELSE 0
    END                                         AS is_perfect_game,

    CASE
        WHEN g.is_final = 1 AND (r.home_runs = 0 OR r.away_runs = 0)
        THEN 1 ELSE 0
    END                                         AS is_shutout,

    g.is_tie,

    CASE WHEN bx.game_pk IS NOT NULL THEN 1 ELSE 0 END AS has_box_score,

    -- Audit
    g.load_id,
    g._dlt_id,
    GETDATE()                                   AS created_at,
    GETDATE()                                   AS updated_at

FROM {{ ref('dim_games') }} g

LEFT JOIN {{ ref('dim_game_details') }} gd
    ON g.game_pk = gd.game_pk

-- Box score only when its runs agree with the final score. This rejects
-- stale, partial, or double-loaded player data. Games that aren't final,
-- or have no final score yet, keep whatever box score exists.
LEFT JOIN {{ ref('int_team_game_box') }} bx
    ON  g.game_pk = bx.game_pk
    AND (   g.is_final = 0
         OR g.home_score IS NULL
         OR g.away_score IS NULL
         OR (bx.home_runs = g.home_score AND bx.away_runs = g.away_score))

CROSS APPLY (
    SELECT
        COALESCE(g.home_score, bx.home_runs) AS home_runs,
        COALESCE(g.away_score, bx.away_runs) AS away_runs,
        -- LOB is only trusted when game_details agrees with the final score
        CASE WHEN gd.home_runs = g.home_score AND gd.away_runs = g.away_score
             THEN 1 ELSE 0 END               AS gd_agrees
) r

LEFT JOIN {{ ref('dim_date') }} dd_game
    ON CAST(g.game_date AS DATE) = dd_game.date_actual

LEFT JOIN {{ ref('dim_date') }} dd_official
    ON g.official_date = dd_official.date_actual

-- dim_teams is unique on (team_id, season); team_key is the hash of that pair
LEFT JOIN {{ ref('dim_teams') }} dt_home
    ON  g.home_team_id = dt_home.team_id
    AND g.season       = dt_home.season

LEFT JOIN {{ ref('dim_teams') }} dt_away
    ON  g.away_team_id = dt_away.team_id
    AND g.season       = dt_away.season

LEFT JOIN {{ ref('dim_venue') }} dv
    ON g.venue_id = dv.venue_id

LEFT JOIN {{ ref('dim_season') }} ds
    ON g.season = ds.season

LEFT JOIN {{ ref('dim_game_type') }} dgt
    ON g.game_type = dgt.game_type_code

-- dim_game_status is unique on (coded_game_state, status_code)
LEFT JOIN {{ ref('dim_game_status') }} dgst
    ON  g.coded_game_state = dgst.status__coded_game_state
    AND g.status_code      = dgst.status__status_code
