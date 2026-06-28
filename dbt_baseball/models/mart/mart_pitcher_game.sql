{{
    config(
        materialized='incremental',
        unique_key=['game_pk', 'pitcher_id']
    )
}}

/*
    Pitcher performance per game.
    Core rate stats: K%, BB%, whiff%, chase%, zone%, first-pitch strike%.
*/
with pitches as (
    select * from {{ ref('int_pitches_enriched') }}
    where 1=1
    {% if is_incremental() %}
        and load_id > (select max(load_id) from {{ this }})
    {% endif %}
),

pitcher_game as (
    select
        game_pk,
        season,
        pitcher_id,
        max(load_id)                                    as load_id,
        max(pitcher_hand)                               as pitcher_hand,
        -- volume
        count(*)                                        as total_pitches,
        sum(cast(is_in_play as int))                    as balls_in_play,
        -- results
        sum(cast(is_strike as int))                     as strikes,
        sum(cast(is_ball as int))                       as balls_total,
        sum(cast(is_called_strike as int))              as called_strikes,
        sum(cast(is_whiff as int))                      as whiffs,
        sum(cast(is_foul as int))                       as fouls,
        -- swing / contact
        sum(cast(is_swing as int))                      as swings,
        sum(cast(is_swing as int)) - sum(cast(is_whiff as int)) as contact,
        -- zone / chase
        sum(cast(is_in_strike_zone as int))             as in_zone_pitches,
        sum(cast(is_chase as int))                      as chases,
        sum(cast(is_zone_swing as int))                 as zone_swings,
        sum(cast(is_zone_whiff as int))                 as zone_whiffs,
        sum(cast(is_edge_pitch as int))                 as edge_pitches,
        -- umpire
        sum(cast(is_called_strike_outside_zone as int)) as ump_missed_calls_favor_pitcher,
        sum(cast(is_ball_inside_zone as int))           as ump_missed_calls_favor_batter,
        -- count context
        sum(cast(is_first_pitch as int))                as first_pitches,
        sum(case when is_first_pitch = 1 and is_strike = 1 then 1 else 0 end) as first_pitch_strikes,
        sum(cast(is_two_strike as int))                 as two_strike_pitches,
        sum(case when is_two_strike = 1 and is_whiff = 1 then 1 else 0 end) as two_strike_whiffs,
        -- velocity
        avg(release_speed)                              as avg_velo,
        max(release_speed)                              as max_velo,
        min(release_speed)                              as min_velo,
        -- spin
        avg(cast(spin_rate as float))                   as avg_spin_rate,
        -- movement
        avg(pfx_x)                                      as avg_h_break,
        avg(pfx_z)                                      as avg_v_break,
        -- extension
        avg(extension)                                  as avg_extension
    from pitches
    group by game_pk, season, pitcher_id
)

select
    *,
    -- rates
    round(cast(strikes as float) / nullif(total_pitches, 0), 3)         as strike_pct,
    round(cast(whiffs as float) / nullif(swings, 0), 3)                 as whiff_pct,
    round(cast(in_zone_pitches as float) / nullif(total_pitches, 0), 3) as zone_pct,
    round(cast(chases as float) / nullif(total_pitches - in_zone_pitches, 0), 3) as chase_pct,
    round(cast(contact as float) / nullif(swings, 0), 3)               as contact_pct,
    round(cast(first_pitch_strikes as float) / nullif(first_pitches, 0), 3) as first_pitch_strike_pct,
    round(cast(swings as float) / nullif(total_pitches, 0), 3)         as swing_pct,
    round(cast(called_strikes as float) / nullif(total_pitches, 0), 3) as csw_pct,
    round(cast(edge_pitches as float) / nullif(total_pitches, 0), 3)   as edge_pct
from pitcher_game