{{
    config(
        materialized='incremental',
        unique_key=['season', 'pitcher_id', 'pitch_type_code']
    )
}}

/*
    Pitcher arsenal breakdown by pitch type per season.
    How each pitch type performs: usage, velocity, movement, results.
*/
with pitches as (
    select * from {{ ref('int_pitches_enriched') }}
    where pitch_type_code is not null
    {% if is_incremental() %}
        and load_id > (select max(load_id) from {{ this }})
    {% endif %}
),

pitch_types as (
    select * from {{ ref('dim_pitch_type') }}
)

select
    p.season,
    p.pitcher_id,
    p.pitch_type_code,
    pt.pitch_type_description,
    pt.pitch_category,
    max(p.load_id)                                              as load_id,
    -- usage
    count(*)                                                    as total_thrown,
    -- velocity
    avg(p.release_speed)                                        as avg_velo,
    max(p.release_speed)                                        as max_velo,
    min(p.release_speed)                                        as min_velo,
    -- movement
    avg(p.pfx_x)                                                as avg_h_break,
    avg(p.pfx_z)                                                as avg_v_break,
    avg(p.break_vertical_induced)                               as avg_ivb,
    -- spin
    avg(cast(p.spin_rate as float))                             as avg_spin_rate,
    avg(cast(p.spin_direction as float))                        as avg_spin_direction,
    -- release
    avg(p.release_pos_x)                                        as avg_release_x,
    avg(p.release_pos_z)                                        as avg_release_z,
    avg(p.extension)                                            as avg_extension,
    -- results
    sum(cast(p.is_swing as int))                                as swings,
    sum(cast(p.is_whiff as int))                                as whiffs,
    sum(cast(p.is_called_strike as int))                        as called_strikes,
    sum(cast(p.is_chase as int))                                as chases,
    sum(cast(p.is_in_play as int))                              as in_play,
    sum(cast(p.is_foul as int))                                 as fouls,
    sum(cast(p.is_strike as int))                               as strikes,
    -- zone
    sum(cast(p.is_in_strike_zone as int))                       as in_zone,
    -- FIX (edge case): is_in_strike_zone is NULL for untracked pitches, so
    -- count out-of-zone pitches explicitly rather than deriving them as
    -- count(*) - in_zone, which would fold untracked pitches into "out of
    -- zone" and inflate chase_pct's denominator.
    sum(case when p.is_in_strike_zone = 0 then 1 else 0 end)   as out_of_zone_pitches,
    sum(cast(p.is_edge_pitch as int))                           as edge_pitches,
    -- rates
    round(cast(sum(p.is_whiff) as float) / nullif(sum(p.is_swing), 0), 3)   as whiff_pct,
    round(cast(sum(p.is_chase) as float)
        / nullif(sum(case when p.is_in_strike_zone = 0 then 1 else 0 end), 0), 3) as chase_pct,
    round(cast(sum(p.is_in_strike_zone) as float) / nullif(count(*), 0), 3) as zone_pct,
    round(cast(sum(cast(p.is_strike as int)) as float) / nullif(count(*), 0), 3) as strike_pct,
    round(cast(count(*) as float)
        / nullif(sum(count(*)) over (partition by p.season, p.pitcher_id), 0), 3) as usage_pct
from pitches p
left join pitch_types pt
    on p.pitch_type_code = pt.pitch_type_code
group by p.season, p.pitcher_id, p.pitch_type_code, pt.pitch_type_description, pt.pitch_category