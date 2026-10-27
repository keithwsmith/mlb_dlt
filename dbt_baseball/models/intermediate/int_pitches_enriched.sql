{{
    config(
       
		materialized='view',
        unique_key='event_id',
        on_schema_change='append_new_columns'
    )
}}

/*
    Enriches each pitch with derived analytical flags used across marts.
    This is the analytical backbone — chase rate, whiff rate, zone rate,
    edge %, called-strike probability, etc. all start here.

    FIX (edge case): every zone/chase/edge flag below is now guarded by
    `has_location`. If plate_x, plate_z, sz_top, or sz_bot is NULL (the
    tracking system missed the pitch), the flag returns NULL instead of a
    hard 0. Previously a missing measurement was indistinguishable from a
    real "not in zone" / "not a chase" observation, which quietly deflated
    every zone%/chase% rate computed downstream.
*/

with pitches as (

    select
        event_id                                     as event_id,
        load_id                                       as load_id,
        game_pk,
        at_bat_index,
        play_id,
        pfx_id,
        pitch_number,
        event_index                                   as event_index,
        season,

        start_time,
        end_time,

        event_type                                     as event_type,
        is_pitch,
        is_in_play,

        pitch_result_code                               as pitch_result_code,
        pitch_result_description                        as pitch_result_description,
        call_code                         as call_code,
        call_description                  as call_description,
        ball_color                         as ball_color,
        is_in_play_detail                         as is_in_play_detail,
        is_strike                          as is_strike,
        is_ball                            as is_ball,
        is_out                             as is_out,

        balls                                as balls,
        strikes                              as strikes,
        outs_when_up                                 as outs_when_up,

        batter_id,
        batter_name,
        batter_side,

        pitcher_id,
        pitcher_name,
        pitcher_hand,
		cast(pitch_type_code as nvarchar(200)) as pitch_type_code,
        pitch_type_description                  as pitch_type_description,
        pitch_trail_color                        as pitch_trail_color,

        release_speed                     as release_speed,
        plate_speed                       as plate_speed,
        plate_time                      as plate_time,
        extension                       as extension,

        plate_x                as plate_x,
        plate_z                as plate_z,

        release_pos_x                 as release_pos_x,
        release_pos_y                 as release_pos_y,
        release_pos_z                 as release_pos_z,

        vx0               as vx0,
        vy0               as vy0,
        vz0               as vz0,

        ax                as ax,
        ay                as ay,
        az                as az,

        pfx_x              as pfx_x,
        pfx_z              as pfx_z,

        break_angle             as break_angle,
        break_length            as break_length,
        break_y                 as break_y,
        break_vertical          as break_vertical,
        break_vertical_induced  as break_vertical_induced,
        break_horizontal        as break_horizontal,
        spin_rate               as spin_rate,
        spin_direction          as spin_direction,
		COALESCE(zone, 99) 		AS zone,
        sz_top                  as sz_top,
        sz_bot                  as sz_bot,
        sz_width                as sz_width,
        sz_depth                as sz_depth,
        pitch_type_confidence                 as pitch_type_confidence,

        exit_velocity                      as exit_velocity,
        launch_angle                      as launch_angle,
        hit_distance                    as hit_distance,

        has_review                         as has_review,

        -- plate-appearance result (same value on every pitch of the at-bat)
        result_event_type,
        result_event,
        result_description,
        result_rbi,
        cast(result_is_hit as int)         as result_is_hit,
        cast(is_final_pitch as int)        as is_final_pitch,

        -- FIX: single source of truth for "do we have enough location data
        -- to classify this pitch relative to the zone at all?"
        case
            when plate_x is not null
             and plate_z is not null
             and sz_top  is not null
             and sz_bot  is not null
            then 1 else 0
        end as has_location

    from {{ ref('stg_play_events') }}

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
            when p.has_location = 0 then null
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
            when p.has_location = 0 then null
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
            when p.has_location = 0 then null
            when p.pitch_result_code in ('S', 'W', 'T', 'F', 'L', 'D', 'E', 'X')
             and p.plate_x between -0.83 and 0.83
             and p.plate_z between p.sz_bot and p.sz_top
            then 1
            else 0
        end as is_zone_swing,

        -- zone whiff = swing and miss at pitch inside the zone
        case
            when p.has_location = 0 then null
            when p.pitch_result_code in ('S', 'W', 'T')
             and p.plate_x between -0.83 and 0.83
             and p.plate_z between p.sz_bot and p.sz_top
            then 1
            else 0
        end as is_zone_whiff,

        -- edge pitch (within ~2 inches of zone boundary)
        case
            when p.has_location = 0 then null
            when abs(p.plate_x) between 0.65 and 1.0
              or abs(p.plate_z - p.sz_top) < 0.25
              or abs(p.plate_z - p.sz_bot) < 0.25
            then 1
            else 0
        end as is_edge_pitch,

        -- called strike outside zone (umpire missed call favoring pitcher)
        case
            when p.has_location = 0 then null
            when p.pitch_result_code = 'C'
             and (p.plate_x < -0.83 or p.plate_x > 0.83
                  or p.plate_z < p.sz_bot or p.plate_z > p.sz_top)
            then 1
            else 0
        end as is_called_strike_outside_zone,

        -- ball inside zone (umpire missed call favoring batter)
        case
            when p.has_location = 0 then null
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
        end as count_leverage,

        -- ── plate-appearance outcome flags ────────────────────────────
        -- Set ONLY on the final pitch of a completed plate appearance (0 on
        -- every other pitch), so marts can simply SUM them. A play that ends
        -- on a runner event (caught stealing, pickoff, ...) is not a
        -- completed plate appearance: the batter comes back up later.
        case
            when p.is_final_pitch = 1
             and p.result_event_type is not null
             and p.result_event_type not like 'caught_stealing%'
             and p.result_event_type not like 'pickoff%'
             and p.result_event_type not like 'stolen_base%'
             and p.result_event_type not in ('wild_pitch', 'passed_ball', 'balk',
                                             'other_advance', 'runner_double_play',
                                             'defensive_indiff')
            then 1 else 0
        end as pa_is_complete

    from pitches p
    left join zones z
        on p.zone = z.zone_id

)

select
    e.*,

    -- outcome flags for completed plate appearances (0 on all other pitches)
    case when e.pa_is_complete = 1 and e.result_is_hit = 1 then 1 else 0 end
        as pa_is_hit,
    case when e.pa_is_complete = 1 and e.result_event_type = 'home_run' then 1 else 0 end
        as pa_is_home_run,
    case when e.pa_is_complete = 1 and e.result_event_type in ('strikeout', 'strikeout_double_play', 'strikeout_triple_play')
        then 1 else 0 end
        as pa_is_strikeout,
    case when e.pa_is_complete = 1 and e.result_event_type in ('walk', 'intent_walk') then 1 else 0 end
        as pa_is_walk,
    case when e.pa_is_complete = 1 and e.result_event_type = 'hit_by_pitch' then 1 else 0 end
        as pa_is_hit_by_pitch,
    -- official at-bat: every completed PA except walks, HBP, sacrifices and interference
    case when e.pa_is_complete = 1
          and e.result_event_type not in ('walk', 'intent_walk', 'hit_by_pitch',
                                          'sac_fly', 'sac_bunt', 'sac_fly_double_play',
                                          'sac_bunt_double_play', 'catcher_interf')
        then 1 else 0 end
        as pa_is_at_bat,
    case when e.pa_is_complete = 1 then
        case e.result_event_type
            when 'single'   then 1
            when 'double'   then 2
            when 'triple'   then 3
            when 'home_run' then 4
            else 0
        end
    else 0 end
        as pa_total_bases,
    case when e.pa_is_complete = 1 then coalesce(e.result_rbi, 0) else 0 end
        as pa_rbi

from enriched e
