{{
    config(
        materialized='table',
        unique_key='position_code'
    )
}}

with all_positions as (
    select
        primary_position__code           as position_code,
        primary_position__name           as position_name,
        primary_position__type           as position_type,
        primary_position__abbreviation   as position_abbreviation
    from {{ source('dw', 'mlbplayers') }}
    where primary_position__code is not null

    union

    select
        position__code,
        position__name,
        position__type,
        position__abbreviation
    from {{ source('dw', 'player_stats') }}
    where position__code is not null

    union

    select
        position__code,
        position__name,
        position__type,
        position__abbreviation
    from {{ source('dw', 'rosters') }}
    where position__code is not null
),

deduped as (
    select
        position_code,
        max(position_name)          as position_name,
        max(position_type)          as position_type,
        max(position_abbreviation)  as position_abbreviation
    from all_positions
    group by position_code
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