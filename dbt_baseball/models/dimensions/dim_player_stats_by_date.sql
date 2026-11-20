-- models/marts/player_stats/dim_player_stats_by_date.sql
--
-- One row per PLAYER per SEASON per GAME_DATE: that date's own stats (the
-- *_day columns) plus the player's running SEASON totals as of the END of
-- that date (the *_to_date columns). Regular season only --
-- int_player_game_stats keeps only final regular-season games.
--
-- Grain: exactly one row per (player_id, season, game_date), enforced here,
-- not assumed from upstream. A doubleheader is collapsed into one row (its
-- *_day stats are both games combined). player_name / team_id / team_name
-- are NOT part of the grouping -- they come from the player's LAST game that
-- date (highest game_pk), so a name variant or a same-day trade can't split
-- a date into several rows. (Grouping by them did exactly that, which also
-- made the running totals non-deterministic: rows tied on game_date were
-- summed in an arbitrary order.)
--
-- game_pk is the player's LAST game that date (the same game that supplies
-- the name/team); first_game_pk is the first. They differ only on a
-- doubleheader (games_played = 2). game_pk is NOT unique on its own here --
-- the key is still (player_id, season, game_date). Use dim_player_stats for
-- one row per game.
--
-- Only real appearances count: a row with no batting, pitching or fielding
-- stats at all (a bench / roster-only listing) is ignored, so it can't
-- inflate games_played.
--
-- Cumulative totals are partitioned by (player_id, season) -- NOT team_id
-- -- so a player traded mid-season still accumulates one continuous season
-- total (matching how season stats are conventionally reported). If you'd
-- rather totals reset at a trade, add team_id to the partition by clauses.
--
-- Rate stats (batting average, ERA, fielding %) are NEVER summed directly --
-- they're derived fresh at each row from the cumulative counting stats, the
-- only mathematically correct way to get a running average.

{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['player_id', 'season', 'game_date']},
            {'columns': ['game_date']}
        ]
    )
}}

with base as (

    -- Real appearances only: at least one stat line for the game.
    select *
    from {{ ref('dim_player_stats') }}
    where batting_at_bats        is not null
       or pitching_outs_recorded is not null
       or fielding_chances       is not null

),

day_context as (

    -- One game_pk/name/team per player per date: from the day's last game.
    select player_id, season, game_date, game_pk, player_name, team_id, team_name
    from (
        select
            player_id, season, game_date, game_pk, player_name, team_id, team_name,
            row_number() over (
                partition by player_id, season, game_date
                order by game_pk desc
            ) as day_rank
        from base
    ) ranked
    where day_rank = 1

),

daily as (

    select
        s.player_id,
        c.player_name,
        c.team_id,
        c.team_name,
        s.season,
        s.game_date,
        c.game_pk,
        min(s.game_pk)                             as first_game_pk,

        -- count(distinct game_pk): one game is counted once, whatever the rows.
        count(distinct s.game_pk)                                                        as games_played,
        count(distinct case when s.batting_at_bats        is not null then s.game_pk end) as games_batted,
        count(distinct case when s.pitching_outs_recorded is not null then s.game_pk end) as games_pitched,
        count(distinct case when s.fielding_chances       is not null then s.game_pk end) as games_fielded,

        sum(s.pitching_outs_recorded)              as pitching_outs_recorded,
        sum(s.pitching_hits_allowed)               as pitching_hits_allowed,
        sum(s.pitching_runs_allowed)               as pitching_runs_allowed,
        sum(s.pitching_earned_runs)                as pitching_earned_runs,
        sum(s.pitching_walks)                      as pitching_walks,
        sum(s.pitching_strikeouts)                 as pitching_strikeouts,
        sum(s.pitching_home_runs_allowed)          as pitching_home_runs_allowed,

        sum(s.fielding_putouts)                    as fielding_putouts,
        sum(s.fielding_assists)                    as fielding_assists,
        sum(s.fielding_errors)                     as fielding_errors,
        sum(s.fielding_chances)                    as fielding_chances,

        sum(s.batting_at_bats)                     as batting_at_bats,
        sum(s.batting_runs)                        as batting_runs,
        sum(s.batting_hits)                        as batting_hits,
        sum(s.batting_doubles)                     as batting_doubles,
        sum(s.batting_triples)                     as batting_triples,
        sum(s.batting_home_runs)                   as batting_home_runs,
        sum(s.batting_rbi)                         as batting_rbi,
        sum(s.batting_walks)                       as batting_walks,
        sum(s.batting_strikeouts)                  as batting_strikeouts,
        sum(s.batting_stolen_bases)                as batting_stolen_bases,
        sum(s.batting_left_on_base)                as batting_left_on_base

    from base s
    join day_context c
      on  c.player_id = s.player_id
      and c.season    = s.season
      and c.game_date = s.game_date
    group by s.player_id, c.player_name, c.team_id, c.team_name, s.season, s.game_date, c.game_pk

),

cumulative as (

    -- One row per (player_id, season, game_date), so the ORDER BY game_date
    -- below has no ties and the running totals are deterministic.
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
    game_pk,          -- last game that date (see header)
    first_game_pk,    -- first game that date; differs only on a doubleheader

    -- This date only (both games of a doubleheader combined)
    games_played,
    pitching_outs_recorded              as pitching_outs_recorded_day,
    pitching_hits_allowed               as pitching_hits_allowed_day,
    pitching_runs_allowed               as pitching_runs_allowed_day,
    pitching_earned_runs                as pitching_earned_runs_day,
    pitching_walks                      as pitching_walks_day,
    pitching_strikeouts                 as pitching_strikeouts_day,
    pitching_home_runs_allowed          as pitching_home_runs_allowed_day,
    fielding_putouts                    as fielding_putouts_day,
    fielding_assists                    as fielding_assists_day,
    fielding_errors                     as fielding_errors_day,
    fielding_chances                    as fielding_chances_day,
    batting_at_bats                     as batting_at_bats_day,
    batting_runs                        as batting_runs_day,
    batting_hits                        as batting_hits_day,
    batting_doubles                     as batting_doubles_day,
    batting_triples                     as batting_triples_day,
    batting_home_runs                   as batting_home_runs_day,
    batting_rbi                         as batting_rbi_day,
    batting_walks                       as batting_walks_day,
    batting_strikeouts                  as batting_strikeouts_day,
    batting_stolen_bases                as batting_stolen_bases_day,
    batting_left_on_base                as batting_left_on_base_day,

    -- Games -- cumulative
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
    -- ERA = 9 * earned runs / innings pitched.
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
    -- Fielding percentage = (putouts + assists) / (putouts + assists + errors).
    -- Computed from its parts, so it doesn't depend on whether the API's
    -- fielding_chances includes errors.
    round(
        1.0 * (isnull(fielding_putouts_to_date, 0) + isnull(fielding_assists_to_date, 0))
        / nullif(
            isnull(fielding_putouts_to_date, 0)
          + isnull(fielding_assists_to_date, 0)
          + isnull(fielding_errors_to_date, 0), 0),
        3) as fielding_pct_to_date,

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
    -- AVG = hits / at-bats.
    case
        when batting_at_bats_to_date > 0
        then round(1.0 * batting_hits_to_date / batting_at_bats_to_date, 3)
        else null
    end as batting_avg_to_date

from cumulative
