{{
    config(
        materialized='incremental',
        unique_key=['game_pk', 'pitcher_id']
    )
}}

/*
    Pitcher performance per game.
    Core rate stats: K%, BB%, whiff%, chase%, zone%, first-pitch strike%.

    NEW: batters faced, hits / home runs / walks / strikeouts allowed and the
    K% and BB% the header always promised, from the pa_* flags in
    int_pitches_enriched. A plate appearance is credited to whoever threw
    its final pitch. 0 / NULL for games loaded before mlb_load.py saved
    plate-appearance results.

    FIX (NULL zone counts): the zone/chase/edge/umpire flags in
    int_pitches_enriched are NULL when a pitch has no tracking data, and
    SUM() returns NULL when every value in the group is NULL. So any
    pitcher-game with no tracked pitches -- untracked games/levels, or a
    "pitcher-game" made only of non-pitch events -- got NULL in_zone_pitches,
    chases, zone_swings, zone_whiffs, edge_pitches and ump_* counts.
    Those counts now use CASE ... = 1 THEN 1 ELSE 0, so they are 0, never NULL.

    FIX (pitch counts): stg_play_events holds every play event, not just
    pitches (pickoff throws, mound visits, substitutions, stolen bases...).
    total_pitches was count(*), which counted those too. It now counts
    is_pitch = 1 only. Non-pitch rows are still read, because a plate
    appearance can end on one (an automatic intentional walk has no pitch).

    NEW: tracked_pitches = pitches with location data. zone_pct and edge_pct
    now divide by it instead of total_pitches, so untracked pitches no
    longer drag those rates down; both are NULL when nothing was tracked.
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
        -- volume (real pitches only, not pickoffs / mound visits / etc.)
        sum(case when is_pitch = 1 then 1 else 0 end)   as total_pitches,
        sum(case when is_pitch = 1 and has_location = 1 then 1 else 0 end) as tracked_pitches,
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
        -- zone / chase: "= 1 THEN 1 ELSE 0" so untracked (NULL) pitches
        -- count as 0 instead of turning the whole sum NULL
        sum(case when is_in_strike_zone = 1 then 1 else 0 end) as in_zone_pitches,
        -- FIX (edge case): is_in_strike_zone is NULL for pitches with no
        -- tracking data, so this explicitly counts only pitches we could
        -- actually classify as outside the zone -- NOT total_pitches minus
        -- in_zone_pitches, which would silently fold untracked pitches
        -- into the "out of zone" bucket and inflate chase_pct's denominator.
        sum(case when is_in_strike_zone = 0 then 1 else 0 end) as out_of_zone_pitches,
        sum(case when is_chase = 1 then 1 else 0 end)          as chases,
        sum(case when is_zone_swing = 1 then 1 else 0 end)     as zone_swings,
        sum(case when is_zone_whiff = 1 then 1 else 0 end)     as zone_whiffs,
        sum(case when is_edge_pitch = 1 then 1 else 0 end)     as edge_pitches,
        -- umpire
        sum(case when is_called_strike_outside_zone = 1 then 1 else 0 end) as ump_missed_calls_favor_pitcher,
        sum(case when is_ball_inside_zone = 1 then 1 else 0 end)           as ump_missed_calls_favor_batter,
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
        avg(extension)                                  as avg_extension,
        -- plate-appearance outcomes allowed (final pitch only)
        sum(pa_is_complete)                             as batters_faced,
        sum(pa_is_at_bat)                               as at_bats_against,
        sum(pa_is_hit)                                  as hits_allowed,
        sum(pa_is_home_run)                             as home_runs_allowed,
        sum(pa_is_walk)                                 as walks_allowed,
        sum(pa_is_hit_by_pitch)                         as hit_batters,
        sum(pa_is_strikeout)                            as strikeouts
    from pitches
    group by game_pk, season, pitcher_id
    -- drop "pitcher-games" made only of non-pitch events (e.g. a pickoff
    -- attempt or substitution row) that threw no pitch and faced no batter
    having sum(case when is_pitch = 1 then 1 else 0 end) > 0
        or sum(pa_is_complete) > 0
)

select
    *,
    -- rates
    round(cast(strikes as float) / nullif(total_pitches, 0), 3)         as strike_pct,
    round(cast(whiffs as float) / nullif(swings, 0), 3)                 as whiff_pct,
    -- zone-based rates divide by tracked pitches only
    round(cast(in_zone_pitches as float) / nullif(tracked_pitches, 0), 3) as zone_pct,
    round(cast(chases as float) / nullif(out_of_zone_pitches, 0), 3)    as chase_pct,
    round(cast(contact as float) / nullif(swings, 0), 3)               as contact_pct,
    round(cast(first_pitch_strikes as float) / nullif(first_pitches, 0), 3) as first_pitch_strike_pct,
    round(cast(swings as float) / nullif(total_pitches, 0), 3)         as swing_pct,
    round(cast(called_strikes as float) / nullif(total_pitches, 0), 3) as csw_pct,
    round(cast(edge_pitches as float) / nullif(tracked_pitches, 0), 3) as edge_pct,
    -- outcome rates
    round(cast(strikeouts as float) / nullif(batters_faced, 0), 3)      as strikeout_pct,
    round(cast(walks_allowed as float) / nullif(batters_faced, 0), 3)   as walk_pct,
    round(cast(hits_allowed as float) / nullif(at_bats_against, 0), 3)  as batting_avg_against
from pitcher_game
