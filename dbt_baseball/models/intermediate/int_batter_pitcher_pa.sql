{{
    config(
        materialized='table'
    )
}}

{#
    int_batter_pitcher_pa -- one row per plate appearance (regular season,
    final games), with the batter, the pitcher, the result and per-PA pitch
    and contact numbers. Both gold_batter_pitcher_season and
    gold_batter_pitcher_career aggregate from this model.

    Sources
      fact_at_bats          one row per at-bat: batter, pitcher, hands, exit
                            velocity, launch angle, hard-hit flag
      stg_play_events       the plate-appearance result (result_event_type,
                            e.g. single, walk, strikeout), loaded by
                            mlb_load.py on every pitch of the at-bat
      int_pitches_enriched  swings, whiffs, called strikes, zone, velocity
      fact_batted_balls     barrel flag
      dim_games             regular season (game_type 'R'), final games only

    The pitcher is the one MLB credits with the plate appearance (the
    at-bat's matchup), so a mid-at-bat pitching change is charged to the
    pitcher who finished it, as in official scoring.

    Known gaps
      - Since 2017 an intentional walk can be issued with no pitch. dw.play_events
        only holds pitches, so those walks are not in fact_at_bats and are
        missing here (intentional_walks undercounts for 2017+).
      - 'truncated_pa' (the inning ended on the bases mid at-bat) is not a
        plate appearance and is excluded.
      - A plate appearance with no result_event_type still counts as a plate
        appearance, with has_result = 0 and every result flag 0. Rates use
        pa_with_result / at_bats, so these rows don't distort them.
#}

with games as (

    select game_pk, season, game_date
    from {{ ref('dim_games') }}
    where game_type = 'R'
      and is_final = 1

),

at_bats as (

    select
        ab.game_pk,
        g.season,
        g.game_date,
        ab.at_bat_index,
        ab.batter_id,
        ab.batter_name,
        ab.batter_side,
        ab.pitcher_id,
        ab.pitcher_name,
        ab.pitcher_hand,
        ab.exit_velocity,
        ab.launch_angle,
        cast(ab.is_hard_hit as int)                                     as is_hard_hit
    from {{ ref('fact_at_bats') }} ab
    join games g
        on g.game_pk = ab.game_pk

),

-- Plate-appearance result. The same value is stored on every pitch of the
-- at-bat, so the last pitch is taken. VERIFY: the column names below
-- (result_event_type, pitch_number) must match stg_play_events.
pa_results as (

    select game_pk, at_bat_index, lower(result_event_type) as event_type
    from (
        select
            game_pk,
            at_bat_index,
            result_event_type,
            row_number() over (
                partition by game_pk, at_bat_index
                order by pitch_number desc
            )                                                           as rn
        from {{ ref('stg_play_events') }}
        where result_event_type is not null
    ) r
    where rn = 1

),

pitch_metrics as (

    select
        game_pk,
        at_bat_index,
        count(*)                                                        as pitches,
        sum(cast(is_swing as int))                                      as swings,
        sum(cast(is_whiff as int))                                      as whiffs,
        sum(cast(is_called_strike as int))                              as called_strikes,
        sum(cast(has_location as int))                                  as tracked_pitches,
        sum(case when has_location = 1 and is_in_strike_zone = 0
                 then 1 else 0 end)                                     as pitches_outside_zone,
        sum(case when has_location = 1 and is_in_strike_zone = 0 and is_swing = 1
                 then 1 else 0 end)                                     as chases,
        sum(cast(release_speed as float))                               as release_speed_sum,
        count(release_speed)                                            as release_speed_count
    from {{ ref('int_pitches_enriched') }}
    group by game_pk, at_bat_index

),

barrels as (

    select game_pk, at_bat_index, max(cast(is_barrel as int)) as is_barrel
    from {{ ref('fact_batted_balls') }}
    group by game_pk, at_bat_index

),

flagged as (

    select
        ab.*,
        r.event_type,

        case when r.event_type is null then 0 else 1 end                as has_result,
        case when r.event_type = 'single'    then 1 else 0 end          as is_single,
        case when r.event_type = 'double'    then 1 else 0 end          as is_double,
        case when r.event_type = 'triple'    then 1 else 0 end          as is_triple,
        case when r.event_type = 'home_run'  then 1 else 0 end          as is_home_run,
        case when r.event_type in ('walk', 'intent_walk')
             then 1 else 0 end                                          as is_walk,
        case when r.event_type = 'intent_walk' then 1 else 0 end        as is_intentional_walk,
        case when r.event_type in ('strikeout', 'strikeout_double_play',
                                   'strikeout_triple_play')
             then 1 else 0 end                                          as is_strikeout,
        case when r.event_type = 'hit_by_pitch' then 1 else 0 end       as is_hit_by_pitch,
        case when r.event_type in ('sac_fly', 'sac_fly_double_play')
             then 1 else 0 end                                          as is_sac_fly,
        case when r.event_type in ('sac_bunt', 'sac_bunt_double_play')
             then 1 else 0 end                                          as is_sac_bunt,
        case when r.event_type = 'catcher_interf' then 1 else 0 end     as is_catcher_interference,
        case when r.event_type = 'grounded_into_double_play'
             then 1 else 0 end                                          as is_gidp,

        coalesce(p.pitches, 0)                                          as pitches,
        coalesce(p.swings, 0)                                           as swings,
        coalesce(p.whiffs, 0)                                           as whiffs,
        coalesce(p.called_strikes, 0)                                   as called_strikes,
        coalesce(p.tracked_pitches, 0)                                  as tracked_pitches,
        coalesce(p.pitches_outside_zone, 0)                             as pitches_outside_zone,
        coalesce(p.chases, 0)                                           as chases,
        p.release_speed_sum,
        coalesce(p.release_speed_count, 0)                              as release_speed_count,
        coalesce(b.is_barrel, 0)                                        as is_barrel
    from at_bats ab
    left join pa_results r
        on r.game_pk = ab.game_pk and r.at_bat_index = ab.at_bat_index
    left join pitch_metrics p
        on p.game_pk = ab.game_pk and p.at_bat_index = ab.at_bat_index
    left join barrels b
        on b.game_pk = ab.game_pk and b.at_bat_index = ab.at_bat_index
    where coalesce(r.event_type, '') <> 'truncated_pa'

)

select
    {{ dbt_utils.generate_surrogate_key(['game_pk', 'at_bat_index']) }} as pa_key,
    *,
    is_single + is_double + is_triple + is_home_run                     as is_hit,
    is_single + 2 * is_double + 3 * is_triple + 4 * is_home_run         as total_bases,
    -- An at-bat is a plate appearance that isn't a walk, HBP, sacrifice or
    -- catcher's interference (official scoring rule 9.02).
    case when has_result = 1
          and is_walk = 0 and is_hit_by_pitch = 0 and is_sac_fly = 0
          and is_sac_bunt = 0 and is_catcher_interference = 0
         then 1 else 0 end                                              as is_at_bat
from flagged
