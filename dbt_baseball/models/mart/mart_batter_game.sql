{{
    config(
        materialized='incremental',
        unique_key=['game_pk', 'batter_id']
    )
}}

/*
    Batter performance per game.
    Plate discipline, quality of contact, and batted ball profile.
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

pitch_agg as (

    select
        game_pk,
        season,
        batter_id,
        max(load_id)                                    as load_id,
        max(batter_side)                                as batter_side,

        -- volume
        count(*)                                        as pitches_seen,
        count(distinct at_bat_index)                    as plate_appearances,

        -- discipline
        sum(is_swing)                                   as swings,
        sum(is_whiff)                                   as whiffs,
        sum(is_chase)                                   as chases,
        sum(is_zone_swing)                              as zone_swings,
        sum(is_called_strike)                           as called_strikes_taken,
        sum(cast(is_ball as int))                       as balls_taken,
        sum(cast(is_foul as int))                       as fouls,
        sum(cast(is_in_play as int))                    as balls_in_play,

        -- zone awareness
        sum(cast(is_in_strike_zone as int))             as pitches_in_zone,
        sum(case
            when is_in_strike_zone = 1 and is_swing = 0
            then 1 else 0
        end)                                            as takes_in_zone,

        -- first pitch
        sum(cast(is_first_pitch as int))                as first_pitches,
        sum(case when is_first_pitch = 1 and is_swing = 1 then 1 else 0 end) as first_pitch_swings,
        sum(case when is_first_pitch = 1 and is_in_play = 1 then 1 else 0 end) as first_pitch_in_play,

        -- count
        sum(cast(is_two_strike as int))                 as two_strike_pitches,
        sum(case when is_two_strike = 1 and is_whiff = 1 then 1 else 0 end) as two_strike_whiffs

    from pitches
    group by game_pk, season, batter_id

),

bb_agg as (

    select
        game_pk,
        batter_id,

        count(*)                                        as batted_ball_events,
        avg(exit_velocity)                              as avg_exit_velo,
        max(exit_velocity)                              as max_exit_velo,
        avg(launch_angle)                               as avg_launch_angle,
        avg(hit_distance)                               as avg_hit_distance,
        max(hit_distance)                               as max_hit_distance,
        sum(is_hard_hit)                                as hard_hits,
        sum(is_barrel)                                  as barrels,
        sum(is_sweet_spot)                              as sweet_spot_events,
        sum(case when trajectory_abbrev = 'GB' then 1 else 0 end) as ground_balls,
        sum(case when trajectory_abbrev = 'LD' then 1 else 0 end) as line_drives,
        sum(case when trajectory_abbrev = 'FB' then 1 else 0 end) as fly_balls,
        sum(case when trajectory_abbrev = 'PU' then 1 else 0 end) as popups

    from batted_balls
    group by game_pk, batter_id

)

select
    p.*,

    -- batted ball stats
    coalesce(b.batted_ball_events, 0)   as batted_ball_events,
    b.avg_exit_velo,
    b.max_exit_velo,
    b.avg_launch_angle,
    b.avg_hit_distance,
    b.max_hit_distance,
    coalesce(b.hard_hits, 0)            as hard_hits,
    coalesce(b.barrels, 0)              as barrels,
    coalesce(b.sweet_spot_events, 0)    as sweet_spot_events,
    coalesce(b.ground_balls, 0)         as ground_balls,
    coalesce(b.line_drives, 0)          as line_drives,
    coalesce(b.fly_balls, 0)            as fly_balls,
    coalesce(b.popups, 0)               as popups,

    -- rates
    round(cast(p.swings as float) / nullif(p.pitches_seen, 0), 3)           as swing_pct,
    round(cast(p.whiffs as float) / nullif(p.swings, 0), 3)                 as whiff_pct,
    round(cast(p.chases as float) / nullif(p.pitches_seen - p.pitches_in_zone, 0), 3) as chase_pct,
    round(cast(p.zone_swings as float) / nullif(p.pitches_in_zone, 0), 3)   as zone_swing_pct,
    round(cast(b.hard_hits as float) / nullif(b.batted_ball_events, 0), 3)  as hard_hit_pct,
    round(cast(b.barrels as float) / nullif(b.batted_ball_events, 0), 3)    as barrel_pct,
    round(cast(b.ground_balls as float) / nullif(b.batted_ball_events, 0), 3) as gb_pct,
    round(cast(b.line_drives as float) / nullif(b.batted_ball_events, 0), 3)  as ld_pct,
    round(cast(b.fly_balls as float) / nullif(b.batted_ball_events, 0), 3)    as fb_pct

from pitch_agg p
left join bb_agg b
    on p.game_pk = b.game_pk
   and p.batter_id = b.batter_id