{{
    config(
        materialized='table',
        unique_key='award_id'
    )
}}

with awards as (
    select distinct
        award_id,
        award_name
	from {{ source('dw', 'award_recipients') }}
    where award_id is not null
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['award_id']) }}
                        as award_key,
        award_id,
        award_name
    from awards
)

select * from final
