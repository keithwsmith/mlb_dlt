{{
    config(
        materialized='table',
        schema='gold'
    )
}}

/*
    GOLD: one row per home-plate umpire per regular season -- the scorecard.

    Sources:
      - fact_umpire_performance: one row per game per home-plate umpire, with
        called strikes outside the zone and replay challenges.
      - mart_pitcher_game: missed calls in the umpire's favour of the pitcher
        or the batter, summed over every pitcher in the game. Those are pitch
        level, so only games with tracking data count (tracked_games).
      - dim_game_umpires: the umpire's name.

    Rates are computed from season totals, so an umpire with 30 challenges is
    weighted correctly against one with 3 (fact_umpire_performance's per-game
    overturn_rate_pct must not be averaged).
*/

with regular_season_games as (

    select game_pk, season
    from {{ ref('dim_games') }}
    where game_type = 'R'
      and is_final  = 1

),

umpire_games as (

    select
        g.season,
        f.official_id,
        f.game_pk,
        f.called_strikes_outside_zone,
        f.total_challenges,
        f.overturned_challenges,
        f.upheld_challenges
    from {{ ref('fact_umpire_performance') }} f
    join regular_season_games g
        on g.game_pk = f.game_pk

),

game_calls as (

    select
        game_pk,
        sum(tracked_pitches)                                                as tracked_pitches,
        sum(called_strikes)                                                 as called_strikes,
        sum(ump_missed_calls_favor_pitcher)                                 as missed_calls_favor_pitcher,
        sum(ump_missed_calls_favor_batter)                                  as missed_calls_favor_batter
    from {{ ref('mart_pitcher_game') }}
    group by game_pk

),

umpire_names as (

    select official_id, full_name
    from (
        select
            official_id,
            full_name,
            row_number() over (partition by official_id order by game_pk desc) as rn
        from {{ ref('dim_game_umpires') }}
    ) n
    where rn = 1

),

season_totals as (

    select
        u.season,
        u.official_id,
        count(*)                                                            as games_behind_plate,
        sum(case when c.tracked_pitches > 0 then 1 else 0 end)              as tracked_games,
        sum(u.called_strikes_outside_zone)                                  as called_strikes_outside_zone,
        sum(u.total_challenges)                                             as challenges,
        sum(u.overturned_challenges)                                        as overturned_challenges,
        sum(u.upheld_challenges)                                            as upheld_challenges,
        sum(c.tracked_pitches)                                              as tracked_pitches,
        sum(c.called_strikes)                                               as called_strikes,
        sum(c.missed_calls_favor_pitcher)                                   as missed_calls_favor_pitcher,
        sum(c.missed_calls_favor_batter)                                    as missed_calls_favor_batter
    from umpire_games u
    left join game_calls c
        on c.game_pk = u.game_pk
    group by u.season, u.official_id

)

select
    s.season,
    s.official_id,
    n.full_name                                                             as umpire_name,
    s.games_behind_plate,
    s.tracked_games,

    s.called_strikes_outside_zone,
    round(cast(s.called_strikes_outside_zone as float)
          / nullif(s.games_behind_plate, 0), 2)                             as called_strikes_outside_zone_per_game,

    s.tracked_pitches,
    s.missed_calls_favor_pitcher,
    s.missed_calls_favor_batter,
    s.missed_calls_favor_pitcher + s.missed_calls_favor_batter              as missed_calls,
    round(cast(s.missed_calls_favor_pitcher + s.missed_calls_favor_batter as float)
          / nullif(s.tracked_pitches, 0), 4)                                as missed_call_rate,
    -- positive = calls lean toward pitchers, negative = toward batters
    s.missed_calls_favor_pitcher - s.missed_calls_favor_batter              as net_favor_pitcher,

    s.challenges,
    s.overturned_challenges,
    s.upheld_challenges,
    round(cast(s.overturned_challenges as float) / nullif(s.challenges, 0), 3) as overturn_rate
from season_totals s
left join umpire_names n
    on n.official_id = s.official_id
