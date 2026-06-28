{{
    config(
        materialized='incremental',
        unique_key=['game_pk', 'official_id']
    )
}}

/*
  fact_umpire_performance
  ─────────────────────────────────────────────────────────────────────────────
  Grain : one row per game × home-plate umpire.

  Sources
  ──────
  dim_challenges        – manager challenge outcomes for the game
  int_pitches_enriched  – pitch-level data with zone/strike flags (intermediate model)
  dim_game_umpires      – maps umpire official_id → game_pk

  Metrics
  ──────
  Challenge metrics (rolled up from dim_challenges):
    total_challenges          – total manager challenges in the game
    overturned_challenges     – challenges where the call was reversed
    upheld_challenges         – challenges where the original call stood
    overturn_rate             – overturned / total (NULL when 0 challenges)

  Called-strike accuracy metrics (from int_pitches_enriched):
    called_strikes_outside_zone   – pitches called strikes that were outside
                                    the zone (sum across all zones)
    zones_with_bad_calls          – distinct pitch zones where at least one
                                    bad call occurred
    worst_zone                    – zone with the highest count of bad calls
    worst_zone_count              – count of bad calls in that worst zone
*/

-- ── 1. Challenge summary – one row per game ──────────────────────────────────
with challenge_summary as (

    select
        game_pk,
        count(*)                                                        as total_challenges,
        sum(case when review_is_overturned = 1 then 1 else 0 end)      as overturned_challenges,
        sum(case when review_is_overturned = 0 then 1 else 0 end)      as upheld_challenges
    from {{ ref('dim_challenges') }}
    group by game_pk

),

-- ── 2. Called-strike-outside-zone by game + umpire + zone ───────────────────
bad_calls_by_zone as (

    select
        pe.game_pk,
        u.official_id,
        pe.zone,
        sum(pe.is_called_strike_outside_zone)   as called_strikes_outside_zone
    from {{ ref('int_pitches_enriched') }}               pe
    join {{ ref('dim_game_umpires') }}                   u
      on  u.game_pk       = pe.game_pk
      and u.official_type = 'Home Plate'
    where pe.is_called_strike_outside_zone <> 0
    group by
        pe.game_pk,
        u.official_id,
        pe.zone

),

-- ── 3. Roll zone detail up to game + umpire ──────────────────────────────────
bad_calls_summary as (

    select
        game_pk,
        official_id,
        sum(called_strikes_outside_zone)                    as called_strikes_outside_zone,
        count(distinct zone)                                as zones_with_bad_calls
    from bad_calls_by_zone
    group by
        game_pk,
        official_id

),

-- ── 4. Worst zone per game + umpire (zone with most bad calls) ───────────────
worst_zone as (

    select
        game_pk,
        official_id,
        zone                            as worst_zone,
        called_strikes_outside_zone     as worst_zone_count,
        row_number() over (
            partition by game_pk, official_id
            order by called_strikes_outside_zone desc
        )                               as rn
    from bad_calls_by_zone

),

-- ── 5. Combine everything ────────────────────────────────────────────────────
final as (

    select
        {{ dbt_utils.generate_surrogate_key(['bcs.game_pk', 'bcs.official_id']) }}
                                                        as umpire_performance_key,

        -- dimensions
        bcs.game_pk,
        bcs.official_id,

        -- called-strike accuracy
        bcs.called_strikes_outside_zone,
        bcs.zones_with_bad_calls,
        wz.worst_zone,
        wz.worst_zone_count,

        -- challenge outcomes (NULL-safe: a game may have zero challenges)
        coalesce(cs.total_challenges,      0)           as total_challenges,
        coalesce(cs.overturned_challenges, 0)           as overturned_challenges,
        coalesce(cs.upheld_challenges,     0)           as upheld_challenges,
        case
            when coalesce(cs.total_challenges, 0) = 0  then null
            else round(
                    cast(cs.overturned_challenges as float)
                    / cs.total_challenges * 100, 2)
        end                                             as overturn_rate_pct

    from bad_calls_summary       bcs
    left join challenge_summary  cs  on cs.game_pk    = bcs.game_pk
    left join worst_zone         wz  on wz.game_pk    = bcs.game_pk
                                    and wz.official_id = bcs.official_id
                                    and wz.rn          = 1

)

select *
from final

{% if is_incremental() %}
-- Only process games not yet in the fact table.
-- Because bad_calls_summary has no load_id we join back to the enriched
-- source to find the high-water mark.
where game_pk not in (select game_pk from {{ this }})
{% endif %}
