-- models/marts/player_stats/dim_player_stats_by_date.sql
--
-- One row per PLAYER per GAME_DATE: end-of-day cumulative stats, i.e. the
-- player's running SEASON totals as of that date (reset each season --
-- see the games_played_to_date-style bug we hit and fixed in
-- dim_team_standings_by_date; this model partitions by season from the
-- start to avoid repeating that).
--
-- Grain note: if a player appears in a doubleheader, both games share the
-- same game_date. This model collapses those to a single row per date --
-- the daily counting stats reflect both games combined, and the *_to_date
-- running totals reflect the player's cumulative total at the END of that
-- date. If you need a row per individual game instead, use
-- dim_player_stats directly.
--
-- Cumulative totals are partitioned by (player_id, season) -- NOT team_id
-- -- so a player traded mid-season still accumulates one continuous
-- season total (matching how season stats are conventionally reported).
-- Each row still carries that date's own team_id/team_name for context.
-- If you'd rather cumulative totals reset at a trade (i.e. partition by
-- team too), that's a one-line change to the partition by clauses below.
--
-- Rate stats (batting average, ERA) are NEVER summed directly -- they're
-- derived fresh at each row from the cumulative counting stats
-- (e.g. cumulative_hits / cumulative_at_bats), the only mathematically
-- correct way to get a running average/ERA.

with daily as (

    select
        player_id,
        player_name,
        team_id,
        team_name,
        season,
        game_date,

        count(*)                                          as games_played,
        count(batting_at_bats)                             as games_batted,
        count(pitching_outs_recorded)                       as games_pitched,
        count(fielding_chances)                             as games_fielded,

        sum(pitching_outs_recorded)                         as pitching_outs_recorded,
        sum(pitching_hits_allowed)                          as pitching_hits_allowed,
        sum(pitching_runs_allowed)                          as pitching_runs_allowed,
        sum(pitching_earned_runs)                           as pitching_earned_runs,
        sum(pitching_walks)                                 as pitching_walks,
        sum(pitching_strikeouts)                            as pitching_strikeouts,
        sum(pitching_home_runs_allowed)                     as pitching_home_runs_allowed,

        sum(fielding_putouts)                               as fielding_putouts,
        sum(fielding_assists)                               as fielding_assists,
        sum(fielding_errors)                                as fielding_errors,
        sum(fielding_chances)                               as fielding_chances,

        sum(batting_at_bats)                                as batting_at_bats,
        sum(batting_runs)                                   as batting_runs,
        sum(batting_hits)                                   as batting_hits,
        sum(batting_doubles)                                as batting_doubles,
        sum(batting_triples)                                as batting_triples,
        sum(batting_home_runs)                              as batting_home_runs,
        sum(batting_rbi)                                    as batting_rbi,
        sum(batting_walks)                                  as batting_walks,
        sum(batting_strikeouts)                             as batting_strikeouts,
        sum(batting_stolen_bases)                           as batting_stolen_bases,
        sum(batting_left_on_base)                           as batting_left_on_base

    from {{ ref('dim_player_stats') }}
    group by player_id, player_name, team_id, team_name, season, game_date

),

cumulative as (

    select
        d.*,

        sum(games_played) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as games_played_to_date,
        sum(games_batted) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as games_batted_to_date,
        sum(games_pitched) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as games_pitched_to_date,
        sum(games_fielded) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as games_fielded_to_date,

        sum(pitching_outs_recorded) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as pitching_outs_recorded_to_date,
        sum(pitching_hits_allowed) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as pitching_hits_allowed_to_date,
        sum(pitching_runs_allowed) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as pitching_runs_allowed_to_date,
        sum(pitching_earned_runs) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as pitching_earned_runs_to_date,
        sum(pitching_walks) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as pitching_walks_to_date,
        sum(pitching_strikeouts) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as pitching_strikeouts_to_date,
        sum(pitching_home_runs_allowed) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as pitching_home_runs_allowed_to_date,

        sum(fielding_putouts) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as fielding_putouts_to_date,
        sum(fielding_assists) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as fielding_assists_to_date,
        sum(fielding_errors) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as fielding_errors_to_date,
        sum(fielding_chances) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as fielding_chances_to_date,

        sum(batting_at_bats) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as batting_at_bats_to_date,
        sum(batting_runs) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as batting_runs_to_date,
        sum(batting_hits) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as batting_hits_to_date,
        sum(batting_doubles) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as batting_doubles_to_date,
        sum(batting_triples) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as batting_triples_to_date,
        sum(batting_home_runs) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as batting_home_runs_to_date,
        sum(batting_rbi) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as batting_rbi_to_date,
        sum(batting_walks) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as batting_walks_to_date,
        sum(batting_strikeouts) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as batting_strikeouts_to_date,
        sum(batting_stolen_bases) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as batting_stolen_bases_to_date,
        sum(batting_left_on_base) over (
            partition by player_id, season order by game_date
            rows unbounded preceding
        ) as batting_left_on_base_to_date

    from daily d

)

select
    player_id,
    player_name,
    team_id,
    team_name,
    season,
    game_date,

    games_played,
    games_played_to_date,
    games_batted_to_date,
    games_pitched_to_date,
    games_fielded_to_date,

    -- Pitching -- cumulative counts
    pitching_outs_recorded_to_date,
    -- Display-formatted cumulative innings pitched, converted back from
    -- outs into MLB's whole.partial-outs notation (e.g. 41 outs -> '13.2').
    cast(pitching_outs_recorded_to_date / 3 as varchar(10))
        + '.' + cast(pitching_outs_recorded_to_date % 3 as varchar(1))
        as pitching_innings_pitched_to_date,
    pitching_hits_allowed_to_date,
    pitching_runs_allowed_to_date,
    pitching_earned_runs_to_date,
    pitching_walks_to_date,
    pitching_strikeouts_to_date,
    pitching_home_runs_allowed_to_date,
    -- ERA = 9 * earned runs / innings pitched. Derived fresh from
    -- cumulative counts, never summed directly -- see header note.
    case
        when pitching_outs_recorded_to_date > 0
        then round(9.0 * pitching_earned_runs_to_date / (pitching_outs_recorded_to_date / 3.0), 2)
        else null
    end as pitching_era_to_date,

    -- Fielding -- cumulative counts
    fielding_putouts_to_date,
    fielding_assists_to_date,
    fielding_errors_to_date,
    fielding_chances_to_date,
    -- Fielding percentage = (putouts + assists) / chances
    case
        when fielding_chances_to_date > 0
        then round(1.0 * (fielding_putouts_to_date + fielding_assists_to_date) / fielding_chances_to_date, 3)
        else null
    end as fielding_pct_to_date,

    -- Batting -- cumulative counts
    batting_at_bats_to_date,
    batting_runs_to_date,
    batting_hits_to_date,
    batting_doubles_to_date,
    batting_triples_to_date,
    batting_home_runs_to_date,
    batting_rbi_to_date,
    batting_walks_to_date,
    batting_strikeouts_to_date,
    batting_stolen_bases_to_date,
    batting_left_on_base_to_date,
    -- AVG = hits / at-bats. Derived fresh from cumulative counts, never
    -- summed directly -- see header note.
    case
        when batting_at_bats_to_date > 0
        then round(1.0 * batting_hits_to_date / batting_at_bats_to_date, 3)
        else null
    end as batting_avg_to_date

from cumulative

