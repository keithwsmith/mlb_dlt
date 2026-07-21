{{
    config(
        materialized='table',
        unique_key='position_code'
    )
}}

/*
    FIX (edge case): the previous version unioned all three sources and
    took MAX(position_name)/MAX(position_type)/MAX(position_abbreviation)
    per code. MAX on a string returns whichever value sorts alphabetically
    highest, not a canonical one -- e.g. "Right Field" beat "Outfielder"
    purely because 'R' > 'O'. Sources are now explicitly prioritized
    (mlbplayers as the authoritative source, falling back to player_stats,
    then rosters) and the highest-priority non-null row per code wins.
*/

with all_positions as (

    select
        primary_position__code           as position_code,
        primary_position__name           as position_name,
        primary_position__type           as position_type,
        primary_position__abbreviation   as position_abbreviation,
        1                                 as source_priority
    from {{ source('dw', 'mlbplayers') }}
    where primary_position__code is not null

    union all

    select
        position__code,
        position__name,
        position__type,
        position__abbreviation,
        2                                 as source_priority
    from {{ source('dw', 'player_stats') }}
    where position__code is not null

    union all

    select
        position__code,
        position__name,
        position__type,
        position__abbreviation,
        3                                 as source_priority
    from {{ source('dw', 'rosters') }}
    where position__code is not null

),

ranked as (
    select
        *,
        row_number() over (
            partition by position_code
            order by source_priority asc
        ) as rn
    from all_positions
),

deduped as (
    select
        position_code,
        position_name,
        position_type,
        position_abbreviation
    from ranked
    where rn = 1
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['position_code']) }}
                                    as position_key,
        position_code,
        position_name,
        position_type,
        position_abbreviation
    from deduped
)

select * from final
