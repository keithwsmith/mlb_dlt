-- models/marts/standings/dim_team_standings_by_date.sql
--
-- One row per TEAM per GAME_DATE: end-of-day standings, i.e. the team's
-- cumulative record as of that date WITHIN THAT SEASON (running total,
-- ordered by date, reset every season).
--
-- Grain note: if a team plays a doubleheader, both games share the same
-- game_date. This model collapses those to a single row per date — the
-- daily wins/losses/ties reflect both games combined, and the *_to_date
-- running totals reflect the team's record at the END of that date (after
-- both games), not after each individual game. If you need a row per
-- individual game instead, use dim_team_standings directly and add a
-- window function partitioned by team_id, season ordered by game_date,
-- game_datetime.

with daily as (

    select
        team_id,
        team_name,
        season,
        game_date,
        count(*)     as games_played,
        sum(is_win)  as wins,
        sum(is_loss) as losses,
        sum(is_tie)  as ties
    from {{ ref('dim_team_standings') }}
    group by team_id, team_name, season, game_date

)

select
    team_id,
    team_name,
    season,
    game_date,
    games_played,
    wins,
    losses,
    ties,
    sum(games_played) over (
        partition by team_id, season
        order by game_date
        rows unbounded preceding
    ) as games_played_to_date,
    sum(wins) over (
        partition by team_id, season
        order by game_date
        rows unbounded preceding
    ) as wins_to_date,
    sum(losses) over (
        partition by team_id, season
        order by game_date
        rows unbounded preceding
    ) as losses_to_date,
    sum(ties) over (
        partition by team_id, season
        order by game_date
        rows unbounded preceding
    ) as ties_to_date
from daily