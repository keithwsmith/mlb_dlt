{{
    config(
        materialized='incremental',
        unique_key='event_id',
        on_schema_change='sync_all_columns'
    )
}}

/*
    Enriches each pitch with derived analytical flags used across marts.
    This is the analytical backbone — chase rate, whiff rate, zone rate,
    edge %, called-strike probability, etc. all start here.
*/

with pitches as (

    select
        -- identifiers
        event_id                                     as event_id,
		load_id								 as load_id,
        game_pk,
        at_bat_index,
        play_id,
        pfx_id,
        pitch_number,
        event_index                                   as event_index,
        season,

        -- timestamps
        start_time,
        end_time,

        -- event classification
        event_type                                     as event_type,
        is_pitch,
        is_in_play,

        -- pitch result
        pitch_result_code                               as pitch_result_code,
        pitch_result_description                        as pitch_result_description,
        call_code                         as call_code,
        call_description                  as call_description,
        ball_color                         as ball_color,
        is_in_play_detail                         as is_in_play_detail,
        is_strike                          as is_strike,
        is_ball                            as is_ball,
        is_out                             as is_out,

        -- count state
        balls                                as balls,
        strikes                              as strikes,
        outs_when_up                                 as outs_when_up,

        -- batter info
        batter_id,
        batter_name,
        batter_side,

        -- pitcher info
        pitcher_id,
        pitcher_name,
        pitcher_hand,

        -- pitch type
        pitch_type_code                         as pitch_type_code,
        pitch_type_description                  as pitch_type_description,
        pitch_trail_color                        as pitch_trail_color,

        -- pitch velocity
        release_speed                     as release_speed,
        plate_speed                       as plate_speed,
        plate_time                      as plate_time,
        extension                       as extension,

        -- pitch location (plate crossing)
        plate_x                as plate_x,
        plate_z                as plate_z,

        -- pitch release point
        release_pos_x                 as release_pos_x,
        release_pos_y                 as release_pos_y,
        release_pos_z                 as release_pos_z,

        -- pitch initial velocity components
        vx0               as vx0,
        vy0               as vy0,
        vz0               as vz0,

        -- pitch acceleration components
        ax                as ax,
        ay                as ay,
        az                as az,

        -- movement
        pfx_x              as pfx_x,
        pfx_z              as pfx_z,

        -- breaks
        break_angle             as break_angle,
        break_length            as break_length,
        break_y                 as break_y,
        break_vertical          as break_vertical,
        break_vertical_induced  as break_vertical_induced,
        break_horizontal        as break_horizontal,
        spin_rate               as spin_rate,
        spin_direction          as spin_direction,

        -- strike zone
        zone                            as zone,
        sz_top                 as sz_top,
        sz_bot              as sz_bot,
        sz_width               as sz_width,
        sz_depth               as sz_depth,
        pitch_type_confidence                 as pitch_type_confidence,

        -- hit data
        exit_velocity                      as exit_velocity,
        launch_angle                      as launch_angle,
        hit_distance                    as hit_distance,

        -- review
        has_review                         as has_review

    from {{ ref('stg_play_events') }}

    {% if is_incremental() %}

        where load_id >
            (
                select max(load_id)
                from {{ this }}
            )

    {% endif %}

),

zones as (

    select *
    from {{ ref('dim_zone') }}

),

enriched as (

    select
        p.*,

        z.zone_description,
        z.zone_region,

        coalesce(z.is_in_zone, 0) as is_in_zone,

        case
            when p.plate_x between -0.83 and 0.83
             and p.plate_z between p.sz_bot and p.sz_top
            then 1 else 0
        end as is_in_strike_zone,

        case
            when p.pitch_result_code in ('S','W','T','F','L','D','E','X')
            then 1 else 0
        end as is_swing,

        case
            when p.pitch_result_code in ('S','W','T')
            then 1 else 0
        end as is_whiff,

        case
            when p.pitch_result_code = 'C'
            then 1 else 0
        end as is_called_strike,

        case
            when p.pitch_result_code in ('F','L')
            then 1 else 0
        end as is_foul,

        case
            when p.pitch_result_code in ('S','W','T','F','L','D','E','X')
             and (
                    p.plate_x < -0.83
                 or p.plate_x > 0.83
                 or p.plate_z < p.sz_bot
                 or p.plate_z > p.sz_top
             )
            then 1 else 0
        end as is_chase,
		-- zone swing = swing at pitch inside the zone
        case
            when p.pitch_result_code in ('S', 'W', 'T', 'F', 'L', 'D', 'E', 'X')
             and p.plate_x between -0.83 and 0.83
             and p.plate_z between p.sz_bot and p.sz_top
            then 1
            else 0
        end as is_zone_swing,
		 -- zone whiff = swing and miss at pitch inside the zone
        case
            when p.pitch_result_code in ('S', 'W', 'T')
             and p.plate_x between -0.83 and 0.83
             and p.plate_z between p.sz_bot and p.sz_top
            then 1
            else 0
        end as is_zone_whiff,
		 -- edge pitch (within ~2 inches of zone boundary)
        case
            when abs(p.plate_x) between 0.65 and 1.0
              or abs(p.plate_z - p.sz_top) < 0.25
              or abs(p.plate_z - p.sz_bot) < 0.25
            then 1
            else 0
        end as is_edge_pitch,
  -- called strike outside zone (umpire missed call favoring pitcher)
        case
            when p.pitch_result_code = 'C'
             and (p.plate_x < -0.83 or p.plate_x > 0.83
                  or p.plate_z < p.sz_bot or p.plate_z > p.sz_top)
            then 1
            else 0
        end as is_called_strike_outside_zone,
		 -- ball inside zone (umpire missed call favoring batter)
        case
            when p.pitch_result_code in ('B', '*B')
             and p.plate_x between -0.83 and 0.83
             and p.plate_z between p.sz_bot and p.sz_top
            then 1
            else 0
        end as is_ball_inside_zone,
		
        case
            when p.strikes = 2 then 1 else 0
        end as is_two_strike,

        case
            when p.pitch_number = 1 then 1 else 0
        end as is_first_pitch,

        case
            when p.strikes > p.balls then 'Ahead'
            when p.balls > p.strikes then 'Behind'
            else 'Even'
        end as count_leverage

    from pitches p
    left join zones z
        on p.zone = z.zone_id

)

select *
from enriched