{{
    config(
        materialized='table',
        unique_key='coded_game_state'
    )
}}

with statuses as (
    select distinct
        status__coded_game_state,
        status__abstract_game_state,
        status__detailed_state,
        status__status_code,
        status__abstract_game_code
		from {{ source('dw', 'games') }}
    where status__coded_game_state is not null
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['status__coded_game_state','status__status_code']) }}
                                as game_status_key,
        status__coded_game_state,
        status__abstract_game_state,
        status__detailed_state,
        status__status_code,
        status__abstract_game_code
    from statuses
)

select * from final
