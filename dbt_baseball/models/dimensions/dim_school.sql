{{
    config(
        materialized='table',
        unique_key='school_name'
    )
}}

with draft_schools as (
    select
        school__name,
        max(school__city)        as school__city,
        max(school__state)       as school__state,
        max(school__country)     as school__country,
        max(school__school_class)       as school__school_class
	from {{ source('dw', 'draft') }}
    where school__name is not null
    group by school__name
),

school_types as (
    select
        school_name,
        school_type,
        classified_by
    from {{ source('dw', 'school_type_lookup') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['d.school__name']) }}
                                as school_key,
        d.school__name,
        d.school__city,
        d.school__state,
        d.school__country,
        d.school__school_class,
        s.school_type,
        s.classified_by
    from draft_schools d
    left join school_types s on d.school__name = s.school_name
)

select * from final
