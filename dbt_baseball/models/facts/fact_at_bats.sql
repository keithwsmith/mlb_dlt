{{
    config(
        materialized='incremental',
        unique_key=['game_pk', 'at_bat_index']
    )
}}
with pitches as (
    select * from {{ ref('stg_play_events') }}
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
        pitcher_id,
        batter_id,

        -- take first non-null (consistent within an AB)
       
		
		cast(max(pitcher_name) as nvarchar(255)) as pitcher_name,
		cast(max(batter_name) as nvarchar(255)) as batter_name,
		cast(max(pitcher_hand) as nvarchar(10))  as pitcher_hand,
		cast(max(batter_side) as nvarchar(10))   as batter_side,


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
		cast(max(trajectory)  as nvarchar(50))   			as trajectory,

        -- timing
        min(start_time)                                     as ab_start_time,
        max(end_time)                                       as ab_end_time

    from pitches
    group by game_pk, season, at_bat_index, pitcher_id, batter_id

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
