{{
    config(
        materialized='table',
        unique_key='venue_id'
    )
}}

with final as (
    select
        {{ dbt_utils.generate_surrogate_key(['venue_id']) }}
                                as venue_key,
        venue_id,
        venue_name,
        venue_link,
        location_name,
        first_year_of_play,
        spring_venue_id,
        spring_venue_link
	from {{ source('dw', 'venues') }}
)

select * from final
