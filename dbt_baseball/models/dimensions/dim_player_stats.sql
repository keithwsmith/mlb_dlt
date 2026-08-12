-- models/marts/player_stats/dim_player_stats.sql
--
-- One row per PLAYER per COMPLETED GAME (the general-purpose player stats
-- fact). Countable stats are raw per-game counts so downstream consumers
-- (BI tools, or dim_player_stats_by_date below) can SUM() them directly.
-- batting_avg_raw / pitching_era_raw are the per-game rates as reported --
-- reference only, do not SUM these; they don't aggregate meaningfully
-- across games (see dim_player_stats_by_date for a correct cumulative
-- average/ERA).

select
    game_pk,
    season,
    game_type_desc,
    game_date,
    game_datetime,
    player_id,
    player_name,
    team_id,
    team_name,
    opponent_team_id,
    opponent_team_name,
    is_home,
    position_code,
    position_name,
    position_abbreviation,
    pitching_outs_recorded,
    pitching_hits_allowed,
    pitching_runs_allowed,
    pitching_earned_runs,
    pitching_walks,
    pitching_strikeouts,
    pitching_home_runs_allowed,
    fielding_putouts,
    fielding_assists,
    fielding_errors,
    fielding_chances,
    batting_at_bats,
    batting_runs,
    batting_hits,
    batting_doubles,
    batting_triples,
    batting_home_runs,
    batting_rbi,
    batting_walks,
    batting_strikeouts,
    batting_stolen_bases,
    batting_left_on_base,
    batting_avg_raw,
    pitching_era_raw,
    start_hour
from {{ ref('int_player_game_stats') }}
