{{
    config(
        materialized='incremental',
        unique_key=['season', 'pitcher_id', 'batter_id']
    )
}}

/*
    Pitcher vs batter matchup aggregation.
    Useful for scouting reports: how does pitcher X attack batter Y?

    FIX (incremental): a matchup row is a whole-season total, but the old
    version aggregated ONLY the pitches from new loads and then merged that
    over the existing row -- so every run replaced a pitcher/batter's season
    totals with just the newest games. Incremental runs now find the
    (season, pitcher, batter) pairs touched by new loads and re-aggregate
    ALL of their pitches, so the merged row is the full-season total.

    NEW: plate-appearance outcomes (hits, at-bats, home runs, strikeouts,
    walks, RBI, total bases) and batting average / OBP / slugging for the
    matchup, from the pa_* flags in int_pitches_enriched. These are 0 / NULL
    for games loaded before mlb_load.py saved plate-appearance results.
*/

{% if is_incremental() %}
with changed_matchups as (
    select distinct season, pitcher_id, batter_id
    from {{ ref('int_pitches_enriched') }}
    where load_id > (select max(load_id) from {{ this }})
),

pitches as (
    select p.*
    from {{ ref('int_pitches_enriched') }} p
    join changed_matchups c
      on  c.season     = p.season
      and c.pitcher_id = p.pitcher_id
      and c.batter_id  = p.batter_id
),

batted_balls as (
    select b.*
    from {{ ref('fact_batted_balls') }} b
    join changed_matchups c
      on  c.season     = b.season
      and c.pitcher_id = b.pitcher_id
      and c.batter_id  = b.batter_id
),
{% else %}
with pitches as (
    select * from {{ ref('int_pitches_enriched') }}
),

batted_balls as (
    select * from {{ ref('fact_batted_balls') }}
),
{% endif %}

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
        -- FIX (edge case): is_in_strike_zone is NULL for untracked pitches,
        -- so count out-of-zone pitches explicitly rather than deriving them
        -- as total_pitches - in_zone, which would fold untracked pitches
        -- into "out of zone" and inflate chase_pct's denominator.
        sum(case when is_in_strike_zone = 0 then 1 else 0 end) as out_of_zone_pitches,
        -- avg location
        avg(plate_x)                                    as avg_plate_x,
        avg(plate_z)                                    as avg_plate_z,
        -- plate-appearance outcomes (flags are set only on the final pitch)
        sum(pa_is_complete)                             as completed_pas,
        sum(pa_is_at_bat)                               as at_bats,
        sum(pa_is_hit)                                  as hits,
        sum(pa_is_home_run)                             as home_runs,
        sum(pa_is_strikeout)                            as strikeouts,
        sum(pa_is_walk)                                 as walks,
        sum(pa_is_hit_by_pitch)                         as hit_by_pitch,
        sum(pa_total_bases)                             as total_bases,
        sum(pa_rbi)                                     as rbi,
        sum(case when pa_is_complete = 1 and result_event_type = 'sac_fly' then 1 else 0 end) as sac_flies
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
    round(cast(mp.chases as float) / nullif(mp.out_of_zone_pitches, 0), 3) as chase_pct,
    round(cast(mp.strikes as float) / nullif(mp.total_pitches, 0), 3)      as strike_pct,
    round(cast(mb.hard_hits as float) / nullif(mb.batted_ball_events, 0), 3) as hard_hit_pct,
    round(cast(mp.fastballs as float) / nullif(mp.total_pitches, 0), 3)    as fastball_pct,
    round(cast(mp.breaking_balls as float) / nullif(mp.total_pitches, 0), 3) as breaking_pct,
    round(cast(mp.offspeed as float) / nullif(mp.total_pitches, 0), 3)     as offspeed_pct,
    -- outcome rates for the matchup
    round(cast(mp.hits as float) / nullif(mp.at_bats, 0), 3)               as batting_avg,
    round(cast(mp.hits + mp.walks + mp.hit_by_pitch as float)
          / nullif(mp.at_bats + mp.walks + mp.hit_by_pitch + mp.sac_flies, 0), 3) as on_base_pct,
    round(cast(mp.total_bases as float) / nullif(mp.at_bats, 0), 3)        as slugging_pct,
    round(cast(mp.strikeouts as float) / nullif(mp.completed_pas, 0), 3)   as strikeout_pct,
    round(cast(mp.walks as float) / nullif(mp.completed_pas, 0), 3)        as walk_pct
from matchup_pitches mp
left join matchup_bb mb
    on mp.season = mb.season
   and mp.pitcher_id = mb.pitcher_id
   and mp.batter_id = mb.batter_id
