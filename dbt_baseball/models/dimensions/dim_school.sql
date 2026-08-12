{{
    config(
        materialized='table',
        unique_key='school_key'
    )
}}

/*
    FIX (edge case): the previous version used MAX(school__city) /
    MAX(school__state) / MAX(school__country) / MAX(school__school_class)
    to collapse multiple dw.draft records per school__name down to one
    row. MAX on a string returns whichever value sorts alphabetically
    highest, not a canonical one -- the exact same bug identified and
    fixed in dim_position.sql ("'Right Field' beat 'Outfielder' purely
    because 'R' > 'O'"). The most recently loaded record per school now
    wins instead (ROW_NUMBER on _dlt_load_id), matching the recency-based
    dedup convention used elsewhere in this project.

    Also fixed: unique_key was previously 'school_name', which doesn't
    match any actual output column (the real column is school__name,
    double underscore) -- inert under materialized='table' either way,
    but corrected to the real key (school_key) for clarity.
*/

with draft_schools as (

    select
        school__name,
        school__city,
        school__state,
        school__country,
        school__school_class,
        row_number() over (
            partition by school__name
            order by _dlt_load_id desc
        ) as rn
    from {{ source('dw', 'draft') }}
    where school__name is not null

),

draft_schools_deduped as (

    select
        school__name,
        school__city,
        school__state,
        school__country,
        school__school_class
    from draft_schools
    where rn = 1

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
    from draft_schools_deduped d
    left join school_types s on d.school__name = s.school_name
)

select * from final