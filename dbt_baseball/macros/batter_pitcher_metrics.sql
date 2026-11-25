{#
    Shared stat definitions for the batter-vs-pitcher gold models, so the
    season and career versions can never disagree.

    bvp_counts(): aggregate columns, used in a GROUP BY over
                  int_batter_pitcher_pa (alias the model "pa").
    bvp_rates():  rate columns, computed from bvp_counts()' columns after
                  aggregation (never averaged from per-game rates).
#}

{% macro bvp_counts() %}
        count(distinct pa.game_pk)                                      as games_faced,
        min(pa.game_date)                                               as first_faced_date,
        max(pa.game_date)                                               as last_faced_date,

        count(*)                                                        as plate_appearances,
        sum(pa.has_result)                                              as pa_with_result,
        sum(pa.is_at_bat)                                               as at_bats,
        sum(pa.is_hit)                                                  as hits,
        sum(pa.is_single)                                               as singles,
        sum(pa.is_double)                                               as doubles,
        sum(pa.is_triple)                                               as triples,
        sum(pa.is_home_run)                                             as home_runs,
        sum(pa.total_bases)                                             as total_bases,
        sum(pa.is_walk)                                                 as walks,
        sum(pa.is_intentional_walk)                                     as intentional_walks,
        sum(pa.is_strikeout)                                            as strikeouts,
        sum(pa.is_hit_by_pitch)                                         as hit_by_pitch,
        sum(pa.is_sac_fly)                                              as sac_flies,
        sum(pa.is_sac_bunt)                                             as sac_bunts,
        sum(pa.is_gidp)                                                 as gidp,

        sum(pa.pitches)                                                 as pitches,
        sum(pa.swings)                                                  as swings,
        sum(pa.whiffs)                                                  as whiffs,
        sum(pa.called_strikes)                                          as called_strikes,
        sum(pa.tracked_pitches)                                         as tracked_pitches,
        sum(pa.pitches_outside_zone)                                    as pitches_outside_zone,
        sum(pa.chases)                                                  as chases,
        round(sum(pa.release_speed_sum)
              / nullif(sum(pa.release_speed_count), 0), 1)              as avg_pitch_velo,

        count(pa.exit_velocity)                                         as batted_ball_events,
        round(avg(cast(pa.exit_velocity as float)), 1)                  as avg_exit_velo,
        max(pa.exit_velocity)                                           as max_exit_velo,
        round(avg(cast(pa.launch_angle as float)), 1)                   as avg_launch_angle,
        sum(case when pa.exit_velocity is not null
                 then pa.is_hard_hit else 0 end)                        as hard_hit_balls,
        sum(case when pa.exit_velocity is not null
                 then pa.is_barrel else 0 end)                          as barrels
{% endmacro %}


{% macro bvp_rates() %}
        round(1.0 * hits / nullif(at_bats, 0), 3)                       as batting_avg,
        round(1.0 * (hits + walks + hit_by_pitch)
              / nullif(at_bats + walks + hit_by_pitch + sac_flies, 0), 3) as on_base_pct,
        round(1.0 * total_bases / nullif(at_bats, 0), 3)                as slugging_pct,
        round(1.0 * (hits + walks + hit_by_pitch)
              / nullif(at_bats + walks + hit_by_pitch + sac_flies, 0)
              + 1.0 * total_bases / nullif(at_bats, 0), 3)              as ops,
        round(1.0 * (total_bases - hits) / nullif(at_bats, 0), 3)       as isolated_power,
        round(1.0 * strikeouts / nullif(pa_with_result, 0), 3)          as strikeout_pct,
        round(1.0 * walks / nullif(pa_with_result, 0), 3)               as walk_pct,
        round(1.0 * pitches / nullif(plate_appearances, 0), 2)          as pitches_per_pa,
        round(1.0 * swings / nullif(pitches, 0), 3)                     as swing_pct,
        round(1.0 * whiffs / nullif(swings, 0), 3)                      as whiff_pct,
        round(1.0 * chases / nullif(pitches_outside_zone, 0), 3)        as chase_pct,
        round(1.0 * (called_strikes + whiffs) / nullif(pitches, 0), 3)  as csw_pct,
        round(1.0 * hard_hit_balls / nullif(batted_ball_events, 0), 3)  as hard_hit_pct,
        round(1.0 * barrels / nullif(batted_ball_events, 0), 3)         as barrel_pct
{% endmacro %}
