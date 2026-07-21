{{
    config(
        materialized='incremental',
        unique_key=['game_pk', 'at_bat_index']
    )
}}

/*
    FIX (edge case #1 — grain): the model used to GROUP BY pitcher_id in
    addition to game_pk/at_bat_index, so a mid-at-bat pitching change
    (reliever enters on a 1-1 count) silently produced TWO rows for what
    was really one at-bat. The grain is now correctly game_pk +
    at_bat_index only. `pitchers_in_ab` surfaces the substitution as data
    instead of hiding it as a duplicate row.

    FIX (edge case #2 — MAX() as "first non-null"): pitcher_name /
    batter_name / pitcher_hand / batter_side / trajectory used
    MAX(column), which for a string returns the alphabetically-greatest
    value, not the first (or otherwise correct) one. These are now pulled
    deterministically via FIRST_VALUE/LAST_VALUE windowed on pitch_number,
    which is guaranteed constant within the group and therefore safe to
    collapse with MAX() afterward.
*/

with pitches as (
    select
        *,
        first_value(pitcher_id)   over (
            partition by game_pk, at_bat_index
            order by pitch_number
            rows between unbounded preceding and unbounded following
        ) as ab_first_pitcher_id,
        first_value(pitcher_name) over (
            partition by game_pk, at_bat_index
            order by pitch_number
            rows between unbounded preceding and unbounded following
        ) as ab_first_pitcher_name,
        last_value(pitcher_id)    over (
            partition by game_pk, at_bat_index
            order by pitch_number
            rows between unbounded preceding and unbounded following
        ) as ab_final_pitcher_id,
        last_value(pitcher_name)  over (
            partition by game_pk, at_bat_index
            order by pitch_number
            rows between unbounded preceding and unbounded following
        ) as ab_final_pitcher_name,
        last_value(pitcher_hand)  over (
            partition by game_pk, at_bat_index
            order by pitch_number
            rows between unbounded preceding and unbounded following
        ) as ab_final_pitcher_hand,
        first_value(batter_name)  over (
            partition by game_pk, at_bat_index
            order by pitch_number
            rows between unbounded preceding and unbounded following
        ) as ab_batter_name,
        first_value(batter_side)  over (
            partition by game_pk, at_bat_index
            order by pitch_number
            rows between unbounded preceding and unbounded following
        ) as ab_batter_side,
        last_value(trajectory)    over (
            partition by game_pk, at_bat_index
            order by pitch_number
            rows between unbounded preceding and unbounded following
        ) as ab_trajectory,
        count(distinct pitcher_id) over (
            partition by game_pk, at_bat_index
        ) as pitchers_in_ab

    from {{ ref('stg_play_events') }}
    where is_pitch = 1
    {% if is_incremental() %}
        and load_id > (select max(load_id) from {{ this }})
    {% endif %}
),

at_bat_agg as (

    select
        max(load_id)  as load_id,
        game_pk,
        season,
        at_bat_index,
        batter_id,

        -- credited pitcher = the one who threw the final (deciding) pitch
        max(ab_final_pitcher_id)   as pitcher_id,
        max(ab_final_pitcher_name) as pitcher_name,
        max(ab_final_pitcher_hand) as pitcher_hand,

        -- flags a substitution mid-at-bat instead of hiding it via a split row
        max(ab_first_pitcher_id)   as first_pitcher_id,
        max(pitchers_in_ab)        as pitchers_in_ab,

        max(ab_batter_name)        as batter_name,
        max(ab_batter_side)        as batter_side,

        -- pitch counts
        count(*)                                            as total_pitches,
        sum(cast(is_strike as int))                         as strikes_total,
        sum(cast(is_ball as int))                           as balls_total,
        sum(case when pitch_result_code = 'F' then 1 else 0 end) as fouls,
        sum(case when pitch_result_code in ('S', 'W', 'T') then 1 else 0 end) as swinging_strikes,
        sum(case when call_code = 'C' then 1 else 0 end)   as called_strikes,

        -- final state
        max(balls)                                          as final_balls,
        max(strikes)                                        as final_strikes,
        max(outs_when_up)                                   as outs_when_up,

        -- outcome
        max(cast(is_in_play as int))                        as ended_in_play,
        max(cast(is_out as int))                            as ended_in_out,

        -- velocity
        avg(release_speed)                                  as avg_pitch_speed,
        max(release_speed)                                  as max_pitch_speed,

        -- batted ball (from final pitch if in play)
        max(exit_velocity)                                  as exit_velocity,
        max(launch_angle)                                   as launch_angle,
        max(hit_distance)                                   as hit_distance,
        max(ab_trajectory)                                  as trajectory,

        -- timing
        min(start_time)                                     as ab_start_time,
        max(end_time)                                       as ab_end_time

    from pitches
    group by game_pk, season, at_bat_index, batter_id

)

select
    *,
    case
        when exit_velocity >= 95.0 then 1
        else 0
    end as is_hard_hit,
    case
        when total_pitches >= 6 then 1
        else 0
    end as is_long_ab

from at_bat_agg
