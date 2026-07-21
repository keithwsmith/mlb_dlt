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

  FIX (edge case #1 — survivorship bias): the model used to be driven off
  bad_calls_summary, which only exists for umpire/game pairs with at least
  one is_called_strike_outside_zone = 1. An umpire who worked a perfectly
  called game was invisible in this table, biasing any "average umpire
  accuracy" rolled up from it. The model now drives from
  home_plate_umpires (every umpire who worked a game), LEFT JOINing pitch
  data in, so a clean game produces a row with called_strikes_outside_zone
  = 0.

  FIX (edge case #2 — join fan-out): dim_game_umpires' surrogate key
  includes load_id, so the same umpire can appear more than once per game
  across incremental loads without being deduplicated. Joining pitches to
  that table directly could double (or triple) count a single missed call.
  umpires_deduped below keeps only the latest load per game/umpire/type
  before anything is joined to pitch data.

  FIX (edge case #3 — incremental never reprocesses a game): the old
  `where game_pk not in (select game_pk from {{ this }})` filter meant a
  late-arriving review reversal or corrected call for an already-loaded
  game would never be picked up. The incremental filter is now based on
  dim_game_umpires.load_id (added to the final select) like every other
  incremental model in this project, so a game is reprocessed whenever new
  umpire/pitch data lands for it.
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

-- ── 2. Dedup the umpire dimension before it drives anything ─────────────────
umpires_deduped as (

    select
        game_pk,
        official_id,
        official_type,
        load_id,
        row_number() over (
            partition by game_pk, official_id, official_type
            order by load_id desc
        ) as rn
    from {{ ref('dim_game_umpires') }}
    where official_type = 'Home Plate'

),

home_plate_umpires as (

    select
        game_pk,
        official_id,
        load_id
    from umpires_deduped
    where rn = 1
    {% if is_incremental() %}
        and load_id > (select coalesce(max(load_id), '0') from {{ this }})
    {% endif %}

),

-- ── 3. Called-strike-outside-zone by game + umpire + zone ───────────────────
--      LEFT JOIN so an umpire with zero missed calls still produces a row.
bad_calls_by_zone as (

    select
        u.game_pk,
        u.official_id,
        u.load_id,
        pe.zone,
        count(pe.event_id)   as called_strikes_outside_zone
    from home_plate_umpires u
    left join {{ ref('int_pitches_enriched') }} pe
        on  pe.game_pk = u.game_pk
        and pe.is_called_strike_outside_zone = 1
    group by
        u.game_pk,
        u.official_id,
        u.load_id,
        pe.zone

),

-- ── 4. Roll zone detail up to game + umpire ──────────────────────────────────
bad_calls_summary as (

    select
        game_pk,
        official_id,
        max(load_id)                                        as load_id,
        sum(called_strikes_outside_zone)                    as called_strikes_outside_zone,
        count(distinct zone)                                as zones_with_bad_calls
    from bad_calls_by_zone
    group by
        game_pk,
        official_id

),

-- ── 5. Worst zone per game + umpire (zone with most bad calls) ───────────────
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
    where zone is not null

),

-- ── 6. Combine everything ────────────────────────────────────────────────────
final as (

    select
        {{ dbt_utils.generate_surrogate_key(['bcs.game_pk', 'bcs.official_id']) }}
                                                        as umpire_performance_key,

        -- dimensions
        bcs.game_pk,
        bcs.official_id,
        bcs.load_id,

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
