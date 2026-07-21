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

    FIX (edge case): team_move_dates previously grouped transactions by
    (player_id, to_team_id) and took MIN(effective_date), so a player's
    SECOND stint with a team (traded away, then traded back) reused the
    FIRST stint's trade date. Transactions are now numbered per
    (player_id, to_team_id) with ROW_NUMBER, and each state-change row is
    matched to the transaction with the same *occurrence number* for that
    team — the Nth time a player joins team X is matched to the Nth
    recorded transaction into team X.
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
-- 3b. Number each state-change row by which "stint" with its
--     team_id it represents for that player. A player back
--     with the same team for a 2nd time gets stint_number = 2.
-- ============================================================
state_changes_numbered as (
    select
        sc.*,
        row_number() over (
            partition by sc.player_id, sc.team_id
            order by sc.season, sc._dlt_load_id
        ) as team_stint_number
    from state_changes sc
),


-- ============================================================
-- 4. Number every transaction into a given team per player the
--    same way, so stint N of the roster can be matched to
--    transaction N into that team.
-- ============================================================
team_move_dates as (
    select
        player_id,
        to_team_id              as team_id,
        effective_date,
        row_number() over (
            partition by player_id, to_team_id
            order by effective_date
        ) as team_stint_number
    from {{ ref('stg_player_transactions') }}
    where to_team_id is not null
),


-- ============================================================
-- 5. Join transaction dates by matching stint number and build
--    SCD2 window columns.
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

        -- effective_date: prefer the matching-stint transaction date;
        -- fall back to a constructed season start date when no
        -- transaction exists for this stint.
        coalesce(
            tmd.effective_date,
            cast(cast(sc.season as varchar) + '-01-01' as date)
        ) as effective_date,

        -- end_date: day before the next version's effective_date
        dateadd(
            day,
            -1,
            lead(
                coalesce(
                    tmd.effective_date,
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

    from state_changes_numbered sc
    left join team_move_dates tmd
        on  sc.player_id         = tmd.player_id
        and sc.team_id           = tmd.team_id
        and sc.team_stint_number = tmd.team_stint_number
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
