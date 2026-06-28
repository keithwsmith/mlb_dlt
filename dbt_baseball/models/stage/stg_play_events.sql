{{ config(materialized='view') }}
with source as (

    select * from {{ source('dw', 'play_events') }}

),

renamed as (

    select
        -- identifiers
        _dlt_id                                     as event_id,
        game_pk,
        at_bat_index,
        play_id,
        pfx_id,
        pitch_number,
        [index]                                     as event_index,
        season,
        _dlt_load_id                                as load_id,

        -- timestamps
        start_time,
        end_time,

        -- event classification
        [type]                                      as event_type,
        is_pitch,
        is_in_play,

        -- pitch result
        details__code                               as pitch_result_code,
        details__description                        as pitch_result_description,
        details__call__code                         as call_code,
        details__call__description                  as call_description,
        details__ball_color                         as ball_color,
        details__is_in_play                         as is_in_play_detail,
        details__is_strike                          as is_strike,
        details__is_ball                            as is_ball,
        details__is_out                             as is_out,

        -- count state
        count__balls                                as balls,
        count__strikes                              as strikes,
        count__outs                                 as outs_when_up,

        -- batter info
        batter_id,
		ISNULL(NULLIF(batter_name, ''), 'unknown') AS batter_name,
        batter_side,

        -- pitcher info
        pitcher_id,
		ISNULL(NULLIF(pitcher_name, ''), 'unknown') AS pitcher_name,
        pitcher_hand,

        -- pitch type
        details__type__code                         as pitch_type_code,
        details__type__description                  as pitch_type_description,
        details__trail_color                        as pitch_trail_color,

        -- pitch velocity
        pitch_data__start_speed                     as release_speed,
        pitch_data__end_speed                       as plate_speed,
        pitch_data__plate_time                      as plate_time,
        pitch_data__extension                       as extension,

        -- pitch location (plate crossing)
        pitch_data__coordinates__p_x                as plate_x,
        pitch_data__coordinates__p_z                as plate_z,

        -- pitch release point
        pitch_data__coordinates__x0                 as release_pos_x,
        pitch_data__coordinates__y0                 as release_pos_y,
        pitch_data__coordinates__z0                 as release_pos_z,

        -- pitch initial velocity components
        pitch_data__coordinates__v_x0               as vx0,
        pitch_data__coordinates__v_y0               as vy0,
        pitch_data__coordinates__v_z0               as vz0,

        -- pitch acceleration components
        pitch_data__coordinates__a_x                as ax,
        pitch_data__coordinates__a_y                as ay,
        pitch_data__coordinates__a_z                as az,

        -- pitch movement (pfx)
        pitch_data__coordinates__pfx_x              as pfx_x,
        pitch_data__coordinates__pfx_z              as pfx_z,

        -- pitch breaks
        pitch_data__breaks__break_angle             as break_angle,
        pitch_data__breaks__break_length            as break_length,
        pitch_data__breaks__break_y                 as break_y,
        pitch_data__breaks__break_vertical          as break_vertical,
        pitch_data__breaks__break_vertical_induced  as break_vertical_induced,
        pitch_data__breaks__break_horizontal        as break_horizontal,
        pitch_data__breaks__spin_rate               as spin_rate,
        pitch_data__breaks__spin_direction          as spin_direction,

        -- strike zone
        pitch_data__zone                            as zone,
        pitch_data__strike_zone_top                 as sz_top,
        pitch_data__strike_zone_bottom              as sz_bot,
        pitch_data__strike_zone_width               as sz_width,
        pitch_data__strike_zone_depth               as sz_depth,
        pitch_data__type_confidence                 as pitch_type_confidence,

        -- strike zone info (detailed)
        pitch_data__strike_zone_info__is_strike              as sz_info_is_strike,
        pitch_data__strike_zone_info__strike_zone_top        as sz_info_top,
        pitch_data__strike_zone_info__strike_zone_bottom     as sz_info_bot,
        pitch_data__strike_zone_info__width_inches           as sz_info_width_inches,
        pitch_data__strike_zone_info__depth_inches           as sz_info_depth_inches,
        pitch_data__strike_zone_info__edge_distance          as sz_info_edge_distance,
        pitch_data__strike_zone_info__baseball_diameter_inches as baseball_diameter_inches,
        pitch_data__strike_zone_info__plate_x                as sz_info_plate_x,
        pitch_data__strike_zone_info__plate_y                as sz_info_plate_y,
        pitch_data__strike_zone_info__plate_z                as sz_info_plate_z,

        -- raw coordinates (legacy/broadcast view)
        pitch_data__coordinates__x                  as broadcast_x,
        pitch_data__coordinates__y                  as broadcast_y,

        -- hit data
        hit_data__launch_speed                      as exit_velocity,
        hit_data__launch_angle                      as launch_angle,
        hit_data__total_distance                    as hit_distance,
		ISNULL(NULLIF(hit_data__trajectory, ''), 'unknown') AS trajectory,
        hit_data__hardness                          as hit_hardness,
        hit_data__location                          as hit_location,
        hit_data__coordinates__coord_x              as hit_coord_x,
        hit_data__coordinates__coord_y              as hit_coord_y,

        -- runner / violation
        details__runner_going                       as is_runner_going,
        details__disengagement_num                  as disengagement_num,
        details__violation__type                    as violation_type,
        details__violation__description             as violation_description,
        details__violation__player__id              as violation_player_id,

        -- review
        details__has_review                         as has_review,
        review_details__is_overturned               as review_is_overturned,
        review_details__in_progress                 as review_in_progress,
        review_details__review_type                 as review_type,
        review_details__challenge_team_id           as review_challenge_team_id,
        review_details__player__id                  as review_player_id,
        review_details__player__full_name           as review_player_name,
        review_details__player__link                as review_player_link

    from source

)

select * from renamed
