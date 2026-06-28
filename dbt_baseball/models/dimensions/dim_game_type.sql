{{
    config(
        materialized='table',
        unique_key='game_type_code'
    )
}}

with game_types as (
    select distinct game_type as game_type_code
	from {{ source('dw', 'games') }}
    where game_type is not null
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['game_type_code']) }}
                                as game_type_key,
        game_type_code,
        case game_type_code
            when 'R' then 'Regular Season'
            when 'S' then 'Spring Training'
            when 'P' then 'Postseason'
            when 'D' then 'Division Series'
            when 'L' then 'League Championship Series'
            when 'W' then 'World Series'
            when 'F' then 'Wild Card'
            when 'A' then 'All-Star Game'
            when 'E' then 'Exhibition'
            else 'Unknown'
        end                     as game_type_name
    from game_types
)

select * from final
