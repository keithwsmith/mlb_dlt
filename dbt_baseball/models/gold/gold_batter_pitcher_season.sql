{{
    config(
        materialized='table',
        schema='gold'
    )
}}

{#
    GOLD: one row per batter x pitcher x regular season -- how a batter did
    against one pitcher that season: the full slash line, strikeouts and
    walks, plate discipline (swing, whiff, chase, CSW) and quality of
    contact (exit velocity, hard-hit, barrels).

    Built from int_batter_pitcher_pa (one row per plate appearance). Counts
    and rates use the macros in macros/batter_pitcher_metrics.sql, shared
    with gold_batter_pitcher_career. Rates come from the season counts.

    Samples are small: most pairs meet only a few times a season. Filter on
    plate_appearances (e.g. >= 10) before ranking or comparing rates.
#}

with pa as (

    select * from {{ ref('int_batter_pitcher_pa') }}

),

agg as (

    select
        pa.season,
        pa.batter_id,
        max(pa.batter_name)                                             as batter_name,
        max(pa.batter_side)                                             as batter_side,
        pa.pitcher_id,
        max(pa.pitcher_name)                                            as pitcher_name,
        max(pa.pitcher_hand)                                            as pitcher_hand,
{{ bvp_counts() }}
    from pa
    group by pa.season, pa.batter_id, pa.pitcher_id

)

select
    {{ dbt_utils.generate_surrogate_key(['season', 'batter_id', 'pitcher_id']) }}
                                                                        as matchup_key,
    agg.*,
{{ bvp_rates() }}
from agg
