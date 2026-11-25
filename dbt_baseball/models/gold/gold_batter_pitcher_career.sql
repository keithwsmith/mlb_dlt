{{
    config(
        materialized='table',
        schema='gold'
    )
}}

{#
    GOLD: one row per batter x pitcher, all regular seasons combined -- the
    career head-to-head record. Same columns and stat definitions as
    gold_batter_pitcher_season (shared macros), plus the seasons the two
    faced each other.

    Built from int_batter_pitcher_pa, not by summing the season model, so
    averages such as exit velocity are true career averages. Coverage
    starts with the earliest season in dw.play_events.
#}

with pa as (

    select * from {{ ref('int_batter_pitcher_pa') }}

),

agg as (

    select
        pa.batter_id,
        max(pa.batter_name)                                             as batter_name,
        max(pa.batter_side)                                             as batter_side,
        pa.pitcher_id,
        max(pa.pitcher_name)                                            as pitcher_name,
        max(pa.pitcher_hand)                                            as pitcher_hand,
        count(distinct pa.season)                                       as seasons_faced,
        min(pa.season)                                                  as first_season,
        max(pa.season)                                                  as last_season,
{{ bvp_counts() }}
    from pa
    group by pa.batter_id, pa.pitcher_id

)

select
    {{ dbt_utils.generate_surrogate_key(['batter_id', 'pitcher_id']) }} as matchup_key,
    agg.*,
{{ bvp_rates() }}
from agg
