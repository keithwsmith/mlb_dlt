{{
    config(
        materialized='table',
        unique_key='season'
    )
}}

with final as (
    select
        {{ dbt_utils.generate_surrogate_key(['season']) }}
                                        as season_key,
        season,
        season_id,
        has_wildcard,
        pre_season_start_date,
        pre_season_end_date,
        spring_start_date,
        spring_end_date,
        regular_season_start_date,
        last_date1st_half,
        all_star_date,
        first_date2nd_half,
        regular_season_end_date,
        post_season_start_date,
        post_season_end_date,
        season_start_date,
        season_end_date,
        offseason_start_date,
        off_season_end_date,
        qualifier_plate_appearances,
        qualifier_outs_pitched,
        datediff(
            day,
            regular_season_start_date,
            regular_season_end_date
        )                               as regular_season_days
	from {{ source('dw', 'seasons') }} s
)

select * from final
