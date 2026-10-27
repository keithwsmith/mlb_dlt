{{
    config(
        materialized         = 'incremental',
        unique_key           = ['game_pk', 'at_bat_index', 'batter_id'],
        incremental_strategy = 'merge'
    )
}}

{#
    fact_at_bats -- one row per (game, at-bat, batter).

    CHANGES (this revision)
    -----------------------
    1. BUG: exit_velocity / launch_angle / hit_distance were MAX() over
       EVERY pitch in the at-bat. Statcast measures foul balls too, so a
       hard-hit foul followed by a strikeout produced is_hard_hit = 1. They
       now come only from the in-play pitch.

    2. The nine first_value/last_value windows are replaced by two
       row_number()s plus conditional aggregation, and the
       dense_rank -> max() trick for counting pitchers is replaced by
       COUNT(DISTINCT pitcher_id), which works fine inside a GROUP BY.

    3. "First" / "final" are now per BATTER within the at-bat (the windows
       used to partition by at-bat only). Normally identical; the
       difference is a mid-at-bat pinch hitter, where each batter's row
       previously showed the FIRST batter's name/side and the final
       pitcher of the whole at-bat. Each row now describes its own batter.

    4. NEW: plate-appearance result columns (result_event_type, result_event,
       result_description, result_rbi) taken from the batter's last pitch,
       plus is_hit / is_home_run / is_strikeout / is_walk / is_at_bat /
       total_bases / is_complete_pa flags derived from result_event_type.
       All NULL / 0 for games loaded before mlb_load.py saved results.

    Incremental note: a game's pitches are assumed to arrive in a single
    load (true for mlb_load.py, which fetches each game once). If an
    at-bat ever spanned two loads, the merge would overwrite it with only
    the newer load's pitches.
#}

with pitches as (
    select
        *,
        row_number() over (
            partition by game_pk, at_bat_index, batter_id
            order by pitch_number
        ) as rn_first,
        row_number() over (
            partition by game_pk, at_bat_index, batter_id
            order by pitch_number desc
        ) as rn_last
    from {{ ref('stg_play_events') }}
    where is_pitch = 1
    {% if is_incremental() %}
        and load_id > (select max(load_id) from {{ this }})
    {% endif %}
),

at_bat_agg as (
    select
        max(load_id)                                                as load_id,
        game_pk,
        season,
        at_bat_index,
        batter_id,

        -- credited pitcher = the one who threw the final pitch
        max(case when rn_last  = 1 then pitcher_id   end)           as pitcher_id,
        max(case when rn_last  = 1 then pitcher_name end)           as pitcher_name,
        max(case when rn_last  = 1 then pitcher_hand end)           as pitcher_hand,

        -- mid-at-bat pitching change visibility
        max(case when rn_first = 1 then pitcher_id   end)           as first_pitcher_id,
        count(distinct pitcher_id)                                  as pitchers_in_ab,

        max(case when rn_first = 1 then batter_name  end)           as batter_name,
        max(case when rn_first = 1 then batter_side  end)           as batter_side,

        -- pitch counts
        count(*)                                                    as total_pitches,
        sum(cast(is_strike as int))                                 as strikes_total,
        sum(cast(is_ball   as int))                                 as balls_total,
        sum(case when pitch_result_code = 'F' then 1 else 0 end)    as fouls,
        sum(case when pitch_result_code in ('S', 'W', 'T') then 1 else 0 end) as swinging_strikes,
        sum(case when call_code = 'C' then 1 else 0 end)            as called_strikes,

        -- final state
        max(balls)                                                  as final_balls,
        max(strikes)                                                as final_strikes,
        max(outs_when_up)                                           as outs_when_up,

        -- outcome
        max(cast(is_in_play as int))                                as ended_in_play,
        max(cast(is_out     as int))                                as ended_in_out,

        -- velocity
        avg(release_speed)                                          as avg_pitch_speed,
        max(release_speed)                                          as max_pitch_speed,

        -- batted ball: ONLY the in-play pitch (fouls excluded)
        max(case when cast(is_in_play as int) = 1 then exit_velocity end) as exit_velocity,
        max(case when cast(is_in_play as int) = 1 then launch_angle  end) as launch_angle,
        max(case when cast(is_in_play as int) = 1 then hit_distance  end) as hit_distance,
        max(case when rn_last = 1 then trajectory end)              as trajectory,

        -- plate-appearance result, only on the row that holds the at-bat's
        -- final pitch (NULL for a batter replaced mid-at-bat)
        max(case when cast(is_final_pitch as int) = 1 then result_event_type  end) as result_event_type,
        max(case when cast(is_final_pitch as int) = 1 then result_event       end) as result_event,
        max(case when cast(is_final_pitch as int) = 1 then result_description end) as result_description,
        max(case when cast(is_final_pitch as int) = 1 then result_rbi         end) as result_rbi,
        max(case when cast(is_final_pitch as int) = 1 then cast(result_is_hit as int) end) as result_is_hit,
        max(cast(is_final_pitch as int))                            as has_final_pitch,

        -- timing
        min(start_time)                                             as ab_start_time,
        max(end_time)                                               as ab_end_time

    from pitches
    group by game_pk, season, at_bat_index, batter_id
),

pa_flags as (
    select
        *,
        -- A plate appearance is complete when this batter's row includes the
        -- at-bat's final pitch (a batter replaced mid-at-bat by a pinch hitter
        -- doesn't get the result) and the play didn't end on a runner event
        -- (caught stealing, pickoff, ...), after which the batter bats again.
        case
            when has_final_pitch = 1
             and result_event_type is not null
             and result_event_type not like 'caught_stealing%'
             and result_event_type not like 'pickoff%'
             and result_event_type not like 'stolen_base%'
             and result_event_type not in ('wild_pitch', 'passed_ball', 'balk',
                                           'other_advance', 'runner_double_play',
                                           'defensive_indiff')
            then 1 else 0
        end as is_complete_pa
    from at_bat_agg
)

select
    *,
    case when exit_velocity >= 95.0 then 1 else 0 end              as is_hard_hit,
    case when total_pitches >= 6     then 1 else 0 end              as is_long_ab,

    -- outcome flags (0 when the plate appearance isn't complete)
    case when is_complete_pa = 1 and result_is_hit = 1 then 1 else 0 end            as is_hit,
    case when is_complete_pa = 1 and result_event_type = 'home_run' then 1 else 0 end as is_home_run,
    case when is_complete_pa = 1
          and result_event_type in ('strikeout', 'strikeout_double_play', 'strikeout_triple_play')
         then 1 else 0 end                                                           as is_strikeout,
    case when is_complete_pa = 1 and result_event_type in ('walk', 'intent_walk') then 1 else 0 end
                                                                                     as is_walk,
    -- official at-bat: excludes walks, HBP, sacrifices and catcher's interference
    case when is_complete_pa = 1
          and result_event_type not in ('walk', 'intent_walk', 'hit_by_pitch',
                                        'sac_fly', 'sac_bunt', 'sac_fly_double_play',
                                        'sac_bunt_double_play', 'catcher_interf')
         then 1 else 0 end                                                           as is_at_bat,
    case when is_complete_pa = 1 then
        case result_event_type
            when 'single'   then 1
            when 'double'   then 2
            when 'triple'   then 3
            when 'home_run' then 4
            else 0
        end
    else 0 end                                                                       as total_bases
from pa_flags
