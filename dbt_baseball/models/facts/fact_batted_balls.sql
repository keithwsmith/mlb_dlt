{{
    config(
        materialized='incremental',
        unique_key='event_id'
    )
}}

-- FIX: switched from a run_query()-based pre-fetch of max load_id to the
-- same in-query subquery pattern used everywhere else in this project
-- (dim_award_recipient, fact_award_recipient, fact_at_bats, etc.):
-- WHERE load_id > (SELECT MAX(load_id) FROM {{ this }}), evaluated by
-- the warehouse as part of the main query rather than as a separate
-- dbt-orchestrated query beforehand. Same result, just consistent with
-- the rest of the project.

with batted_balls as (
    select * from {{ ref('stg_play_events') }}
    where is_in_play = 1
      and exit_velocity is not null
    {% if is_incremental() %}
        and load_id > (select max(load_id) from {{ this }})
    {% endif %}
)
select
    -- keys
    event_id,
    game_pk,
    season,
    at_bat_index,
    pitch_number,
	 load_id,

    -- foreign keys
    pitcher_id,
    batter_id,
    pitch_type_code,

    -- matchup
    batter_side,
    pitcher_hand,
    balls,
    strikes,
    outs_when_up,

    -- batted ball measures
    exit_velocity,
    launch_angle,
    hit_distance,

    -- batted ball classification
    trajectory,
    hit_hardness,
    hit_location,

    -- spray chart coordinates
    hit_coord_x,
    hit_coord_y,

    -- pitch that was hit
    release_speed,
    plate_x,
    plate_z,
    pitch_type_description,

    -- derived quality-of-contact flags
    case
        when exit_velocity >= 95.0 then 1
        else 0
    end as is_hard_hit,

    case
        when exit_velocity >= 98.0
         and launch_angle between 26 and 30
        then 1
        else 0
    end as is_barrel,

    case
        when launch_angle between 8 and 32 then 1
        else 0
    end as is_sweet_spot,

    case
        when trajectory = 'ground_ball' then 'GB'
        when trajectory = 'line_drive'  then 'LD'
        when trajectory = 'fly_ball'    then 'FB'
        when trajectory = 'popup'       then 'PU'
        else 'UNK'
    end as trajectory_abbrev,

    -- outcome
    is_out

from batted_balls