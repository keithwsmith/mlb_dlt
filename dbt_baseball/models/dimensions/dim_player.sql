{{
    config(
        materialized='table'
    )
}}

{#
    SCD Type 2 — dim_player

    Tracks changes to a player's team, position, status, and jersey number
    over time. Each row represents one "version" of a player while their
    tracked attributes remained constant.

    Change detection:
      • Roster attributes (position, status, jersey, team) are hashed into
        a change_hash per player per season/load.
      • Consecutive identical hashes are collapsed so only genuine state
        changes produce new rows.
      • Transaction effective_dates are joined in when the team changed,
        giving real calendar dates rather than load timestamps.

    Grain: one row per player per distinct attribute state.
#}


-- ============================================================
-- 1. Deduplicate rosters to one record per player per season,
--    keeping the latest load per season.
-- ============================================================
with roster_latest as (
    select
        *,
        row_number() over (
            partition by player_id, season
            order by _dlt_load_id desc
        ) as rn
    from {{ ref('stg_rosters') }}
),

roster_deduped as (
    select * from roster_latest where rn = 1
),


-- ============================================================
-- 2. Hash the tracked attributes so we can detect changes
--    across seasons.
-- ============================================================
roster_hashed as (
    select
        player_id,
        full_name,
        team_id,
        jersey_number,
        position__code,
        position__name,
        position__type,
        position__abbreviation,
        status__code,
        status_description,
        season,
        _dlt_load_id,

        {{ dbt_utils.generate_surrogate_key([
            'team_id',
            'jersey_number',
            'position__code',
            'status__code'
        ]) }} as change_hash,

        lag({{ dbt_utils.generate_surrogate_key([
            'team_id',
            'jersey_number',
            'position__code',
            'status__code'
        ]) }}) over (
            partition by player_id
            order by season, _dlt_load_id
        ) as prev_change_hash

    from roster_deduped
),


-- ============================================================
-- 3. Keep only rows where the player's state actually changed
--    (or it is their first appearance).
-- ============================================================
state_changes as (
    select *
    from roster_hashed
    where prev_change_hash is null          -- first record
       or change_hash != prev_change_hash   -- attribute changed
),


-- ============================================================
-- 4. Pull the earliest transaction effective_date for each
--    player + destination team to use as the real effective
--    date when a team change is detected.
-- ============================================================
team_move_dates as (
    select
        player_id,
        to_team_id              as team_id,
        min(effective_date)     as move_effective_date
    from {{ ref('stg_player_transactions') }}
    where to_team_id is not null
    group by player_id, to_team_id
),


-- ============================================================
-- 5. Join transaction dates and build SCD2 window columns.
-- ============================================================
versioned as (
    select
        sc.player_id,
        sc.full_name,
        sc.team_id,
        sc.jersey_number,
        sc.position__code,
        sc.position__name,
        sc.position__type,
        sc.position__abbreviation,
        sc.status__code,
        sc.status_description,
        sc.season,
        sc.change_hash,

        -- effective_date: prefer the transaction date; fall back to
        -- a constructed season start date when no transaction exists.
        coalesce(
            tmd.move_effective_date,
            cast(cast(sc.season as varchar) + '-01-01' as date)
        ) as effective_date,

        -- end_date: day before the next version's effective_date
        dateadd(
            day,
            -1,
            lead(
                coalesce(
                    tmd.move_effective_date,
                    cast(cast(sc.season as varchar) + '-01-01' as date)
                )
            ) over (
                partition by sc.player_id
                order by sc.season, sc._dlt_load_id
            )
        ) as end_date,

        row_number() over (
            partition by sc.player_id
            order by sc.season, sc._dlt_load_id
        ) as version_number

    from state_changes sc
    left join team_move_dates tmd
        on  sc.player_id = tmd.player_id
        and sc.team_id   = tmd.team_id
),


-- ============================================================
-- 6. Final select with surrogate key and is_current flag.
-- ============================================================
final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'player_id',
            'version_number'
        ]) }}                               as player_version_key,

        player_id,
        full_name,
        team_id,
        jersey_number,
        position__code,
        position__name,
        position__type,
        position__abbreviation,
        status__code,
        status_description,
        season                              as effective_season,
        change_hash,
        version_number,
        effective_date,
        coalesce(end_date, '9999-12-31')    as end_date,
        case
            when end_date is null then 1
            else 0
        end                                 as is_current

    from versioned
)

select * from final
