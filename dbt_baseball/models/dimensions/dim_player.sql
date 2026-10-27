{{
    config(
        materialized='table'
    )
}}

{#
    SCD Type 2 -- dim_player

    Tracks changes to a player's team, position, status, and jersey number
    over time. Each row is one "version" of a player while those tracked
    attributes stayed the same.

    CHANGES (this revision) -- fixes the inverted date ranges flagged by
    uniqueness_dim_player_one_current_row (3,127 players):

    1. Transaction dates are applied ONLY to rows where the player arrived
       at a new team. Previously every row in a same-team block shared the
       block's stint number, so a jersey/position/status change re-used the
       ARRIVAL transaction date -- giving the earlier version an end_date
       the day before its own effective_date.

    2. Transactions are matched by DATE WINDOW, not by "Nth transaction
       into team X = Nth stint with team X". That occurrence matching broke
       whenever a transaction into the same team wasn't a real arrival
       (IL placements/activations, option/recall records), shifting every
       later stint onto an older date. The window -- Oct 1 of the prior
       year through Dec 31 of the roster season -- covers offseason
       acquisitions for that season; widen it if you find misses.
       This also removes the three stint-numbering CTEs.

    3. effective_date is forced to be non-decreasing per player (running
       MAX), and a version superseded on the same day it started is
       dropped. Together these guarantee end_date >= effective_date and no
       overlapping ranges.

    NOTE: version_number is renumbered after step 3, so player_version_key
    values change on the first build. fact_player_stats re-resolves keys on
    every run, but anything that stored the old keys elsewhere will need a
    refresh.
#}


-- 1. One roster record per player per season (latest load wins)
with roster_deduped as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by player_id, season
                order by _dlt_load_id desc
            ) as rn
        from {{ ref('stg_rosters') }}
    ) r
    where rn = 1
),


-- 2. Hash the tracked attributes
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
        cast(season as int) as season,
        _dlt_load_id,
        {{ dbt_utils.generate_surrogate_key([
            'team_id',
            'jersey_number',
            'position__code',
            'status__code'
        ]) }} as change_hash
    from roster_deduped
),


-- 3. Keep only rows where something tracked changed (or first appearance),
--    and flag the ones where the TEAM changed.
with_prev as (
    select
        *,
        lag(change_hash) over (partition by player_id order by season, _dlt_load_id) as prev_change_hash,
        lag(team_id)     over (partition by player_id order by season, _dlt_load_id) as prev_team_id
    from roster_hashed
),

state_changes as (
    select
        *,
        case
            when prev_change_hash is null                  then 1   -- first record
            when team_id is distinct from prev_team_id     then 1   -- team changed
            else 0
        end as is_new_team_arrival
    from with_prev
    where prev_change_hash is null
       or change_hash != prev_change_hash
),


-- 4. Candidate effective date: the latest transaction into this team in
--    the season window, for arrivals only; otherwise Jan 1 of the season.
versioned_base as (
    select
        sc.*,
        coalesce(
            case when sc.is_new_team_arrival = 1 then tx.effective_date end,
            datefromparts(sc.season, 1, 1)
        ) as candidate_date
    from state_changes sc
    outer apply (
        select top 1 cast(t.effective_date as date) as effective_date
        from {{ ref('stg_player_transactions') }} t
        where t.player_id  = sc.player_id
          and t.to_team_id = sc.team_id
          and t.effective_date >= datefromparts(sc.season - 1, 10, 1)
          and t.effective_date <  datefromparts(sc.season + 1, 1, 1)
        order by t.effective_date desc
    ) tx
),


-- 5. Make dates non-decreasing per player
monotonic as (
    select
        *,
        max(candidate_date) over (
            partition by player_id
            order by season, _dlt_load_id
            rows between unbounded preceding and current row
        ) as effective_date
    from versioned_base
),

with_next as (
    select
        *,
        lead(effective_date) over (
            partition by player_id
            order by season, _dlt_load_id
        ) as next_effective_date
    from monotonic
),

-- Drop versions superseded the same day they started (zero-length)
kept as (
    select *
    from with_next
    where next_effective_date is null
       or next_effective_date > effective_date
),


-- 6. SCD2 window columns on the surviving rows
versioned as (
    select
        *,
        dateadd(
            day, -1,
            lead(effective_date) over (partition by player_id order by season, _dlt_load_id)
        ) as end_date_raw,
        row_number() over (
            partition by player_id
            order by season, _dlt_load_id
        ) as version_number
    from kept
)

select
    {{ dbt_utils.generate_surrogate_key([
        'player_id',
        'version_number'
    ]) }}                                       as player_version_key,
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
    season                                      as effective_season,
    change_hash,
    version_number,
    effective_date,
    coalesce(end_date_raw, cast('9999-12-31' as date)) as end_date,
    case when end_date_raw is null then 1 else 0 end   as is_current
from versioned
