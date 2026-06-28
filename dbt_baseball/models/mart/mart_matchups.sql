{{
    config(
        materialized='incremental',
        unique_key=['season', 'pitcher_id', 'batter_id']
    )
}}

/*
    Pitcher vs batter matchup aggregation.
    Useful for scouting reports: how does pitcher X attack batter Y?
*/
with pitches as (
    select * from {{ ref('int_pitches_enriched') }}
    where 1=1
    {% if is_incremental() %}
        and load_id > (select max(load_id) from {{ this }})
    {% endif %}
),

batted_balls as (
    select * from {{ ref('fact_batted_balls') }}
    where 1=1
    {% if is_incremental() %}
        and load_id > (select max(load_id) from {{ this }})
    {% endif %}
),

matchup_pitches as (
    select
        season,
        pitcher_id,
        batter_id,
        max(load_id)                                    as load_id,
        count(*)                                        as total_pitches,
        count(distinct concat(cast(game_pk as varchar), '-', cast(at_bat_index as varchar))) as plate_appearances,
        -- pitch mix
        sum(case when pitch_type_code = 'FF' then 1 else 0 end) as fastballs,
        sum(case when pitch_type_code in ('SL', 'CU', 'KC', 'SV', 'ST') then 1 else 0 end) as breaking_balls,
        sum(case when pitch_type_code in ('CH', 'FS', 'FO') then 1 else 0 end) as offspeed,
        -- results
        sum(cast(is_swing as int))                      as swings,
        sum(cast(is_whiff as int))                      as whiffs,
        sum(cast(is_chase as int))                      as chases,
        sum(cast(is_in_play as int))                    as balls_in_play,
        sum(cast(is_strike as int))                     as strikes,
        sum(cast(is_ball as int))                       as balls_total,
        -- zone
        sum(is_in_strike_zone)                          as in_zone,
        -- avg location
        avg(plate_x)                                    as avg_plate_x,
        avg(plate_z)                                    as avg_plate_z
    from pitches
    group by season, pitcher_id, batter_id
),

matchup_bb as (
    select
        season,
        pitcher_id,
        batter_id,
        count(*)                                        as batted_ball_events,
        avg(exit_velocity)                              as avg_exit_velo,
        max(exit_velocity)                              as max_exit_velo,
        avg(launch_angle)                               as avg_launch_angle,
        sum(is_hard_hit)                                as hard_hits,
        sum(is_barrel)                                  as barrels
    from batted_balls
    group by season, pitcher_id, batter_id
)

select
    mp.*,
    coalesce(mb.batted_ball_events, 0)  as batted_ball_events,
    mb.avg_exit_velo,
    mb.max_exit_velo,
    mb.avg_launch_angle,
    coalesce(mb.hard_hits, 0)           as hard_hits,
    coalesce(mb.barrels, 0)             as barrels,
    -- rates
    round(cast(mp.whiffs as float) / nullif(mp.swings, 0), 3)              as whiff_pct,
    round(cast(mp.chases as float) / nullif(mp.total_pitches - mp.in_zone, 0), 3) as chase_pct,
    round(cast(mp.strikes as float) / nullif(mp.total_pitches, 0), 3)      as strike_pct,
    round(cast(mb.hard_hits as float) / nullif(mb.batted_ball_events, 0), 3) as hard_hit_pct,
    round(cast(mp.fastballs as float) / nullif(mp.total_pitches, 0), 3)    as fastball_pct,
    round(cast(mp.breaking_balls as float) / nullif(mp.total_pitches, 0), 3) as breaking_pct,
    round(cast(mp.offspeed as float) / nullif(mp.total_pitches, 0), 3)     as offspeed_pct
from matchup_pitches mp
left join matchup_bb mb
    on mp.season = mb.season
   and mp.pitcher_id = mb.pitcher_id
   and mp.batter_id = mb.batter_id