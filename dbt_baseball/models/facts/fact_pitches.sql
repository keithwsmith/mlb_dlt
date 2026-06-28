{{
    config(
        materialized='incremental',
        unique_key=['load_id']
    )
}}
with pitches as (

    select * from {{ ref('stg_play_events') }}
    where is_pitch = 1
	 {% if is_incremental() %}
        and load_id > (select max(load_id) from {{ this }})
    {% endif %}

)

select
    -- keys
	load_id as load_id,
    event_id,
    game_pk,
    season,
    at_bat_index,
    pitch_number,
    play_id,

    -- foreign keys
    pitcher_id,
    batter_id,
    pitch_type_code,
    pitch_result_code,
    zone,

    -- matchup context
    batter_side,
    pitcher_hand,
    balls,
    strikes,
    outs_when_up,

    -- timestamps
    start_time,
    end_time,

    -- velocity
    release_speed,
    plate_speed,
    plate_time,
    extension,

    -- location at plate
    plate_x,
    plate_z,

    -- strike zone bounds (batter-specific)
    sz_top,
    sz_bot,

    -- movement
    pfx_x,
    pfx_z,
    break_angle,
    break_length,
    break_vertical,
    break_vertical_induced,
    break_horizontal,

    -- spin
    spin_rate,
    spin_direction,

    -- release point
    release_pos_x,
    release_pos_y,
    release_pos_z,

    -- classification confidence
    pitch_type_confidence,

    -- result flags
    is_strike,
    is_ball,
    is_in_play,
    is_out,
    is_runner_going,

    -- strike zone detailed info
    sz_info_is_strike,
    sz_info_edge_distance

from pitches
