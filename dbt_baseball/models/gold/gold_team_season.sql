{{
    config(
        materialized='table',
        schema='gold'
    )
}}

/*
    GOLD: one row per team per regular season.

    The season record and the splits fans and analysts ask about: home/away,
    one-run games, interleague, day/night; runs scored and allowed; the
    Pythagorean expected record; division rank and games back; home
    attendance.

    Built from int_team_game_results (regular season, final games only,
    one row per team per game), so every number here can be traced back to
    individual games.

    Pythagorean win % uses the 1.83 exponent (Baseball-Reference's
    "Pythagenport"-style constant): RS^1.83 / (RS^1.83 + RA^1.83).
    luck = actual wins - expected wins; a large value usually means a good
    or bad record in close games, not a data problem.
*/

with results as (

    select * from {{ ref('int_team_game_results') }}

),

home_attendance as (

    -- Attendance is per game, so it is counted once, for the home team.
    select
        r.season,
        r.team_id,
        sum(cast(gd.attendance as bigint))  as home_attendance,
        avg(cast(gd.attendance as float))   as avg_home_attendance,
        count(*)                            as home_games_with_attendance
    from results r
    join {{ ref('dim_game_details') }} gd
        on gd.game_pk = r.game_pk
    where r.is_home = 1
      and gd.attendance > 0
    group by r.season, r.team_id

),

season_record as (

    select
        season,
        team_id,
        max(team_name)                                                              as team_name,
        count(*)                                                                    as games,
        sum(case when result = 'W' then 1 else 0 end)                               as wins,
        sum(case when result = 'L' then 1 else 0 end)                               as losses,
        sum(case when result = 'T' then 1 else 0 end)                               as ties,
        sum(team_score)                                                             as runs_scored,
        sum(opponent_score)                                                         as runs_allowed,

        sum(case when is_home = 1 and result = 'W' then 1 else 0 end)               as home_wins,
        sum(case when is_home = 1 and result = 'L' then 1 else 0 end)               as home_losses,
        sum(case when is_home = 0 and result = 'W' then 1 else 0 end)               as away_wins,
        sum(case when is_home = 0 and result = 'L' then 1 else 0 end)               as away_losses,

        sum(case when abs(team_score - opponent_score) = 1 and result = 'W' then 1 else 0 end) as one_run_wins,
        sum(case when abs(team_score - opponent_score) = 1 and result = 'L' then 1 else 0 end) as one_run_losses,

        sum(case when is_interleague = 1 and result = 'W' then 1 else 0 end)        as interleague_wins,
        sum(case when is_interleague = 1 and result = 'L' then 1 else 0 end)        as interleague_losses,

        sum(case when day_night = 'day'   and result = 'W' then 1 else 0 end)       as day_wins,
        sum(case when day_night = 'day'   and result = 'L' then 1 else 0 end)       as day_losses,
        sum(case when day_night = 'night' and result = 'W' then 1 else 0 end)       as night_wins,
        sum(case when day_night = 'night' and result = 'L' then 1 else 0 end)       as night_losses,

        sum(case when opponent_score = 0 then 1 else 0 end)                         as shutouts_thrown,
        max(game_date)                                                              as last_game_date
    from results
    group by season, team_id

),

with_team as (

    select
        s.*,
        t.abbreviation,
        t.league_name,
        t.division_name,
        power(cast(s.runs_scored as float), 1.83)
            / nullif(power(cast(s.runs_scored as float), 1.83)
                   + power(cast(s.runs_allowed as float), 1.83), 0)                as pythag_raw
    from season_record s
    left join {{ ref('dim_teams') }} t
        on  t.team_id = s.team_id
        and t.season  = s.season

)

select
    w.season,
    w.team_id,
    team_name,
    abbreviation,
    league_name,
    division_name,

    games,
    wins,
    losses,
    ties,
    round(cast(wins as float) / nullif(wins + losses, 0), 3)                        as win_pct,

    -- Division standing (regular season). Games back = ((leader W - W) + (L - leader L)) / 2.
    rank() over (
        partition by w.season, league_name, division_name
        order by cast(wins as float) / nullif(wins + losses, 0) desc
    )                                                                               as division_rank,
    (
        max(wins - losses) over (partition by w.season, league_name, division_name)
        - (wins - losses)
    ) / 2.0                                                                         as games_back,

    runs_scored,
    runs_allowed,
    runs_scored - runs_allowed                                                      as run_differential,
    round(cast(runs_scored  as float) / nullif(games, 0), 2)                        as runs_per_game,
    round(cast(runs_allowed as float) / nullif(games, 0), 2)                        as runs_allowed_per_game,

    round(pythag_raw, 3)                                                            as pythag_win_pct,
    cast(round(pythag_raw * (wins + losses), 0) as int)                             as pythag_wins,
    wins - cast(round(pythag_raw * (wins + losses), 0) as int)                      as luck,

    home_wins,
    home_losses,
    away_wins,
    away_losses,
    one_run_wins,
    one_run_losses,
    interleague_wins,
    interleague_losses,
    day_wins,
    day_losses,
    night_wins,
    night_losses,
    shutouts_thrown,

    ha.home_attendance,
    round(ha.avg_home_attendance, 0)                                                as avg_home_attendance,

    last_game_date
from with_team w
left join home_attendance ha
    on  ha.season  = w.season
    and ha.team_id = w.team_id
