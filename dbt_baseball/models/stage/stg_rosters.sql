{{
    config(materialized='view')
}}

with source as (
    select
        person__id              as player_id,
        person__full_name       as full_name,
        jersey_number,
        position__code,
        position__name,
        position__type,
        position__abbreviation,
        status__code,
        status__description     as status_description,
        season,
        parent_team_id          as team_id,
        note,
        _dlt_load_id,
        _dlt_id
    from {{ source('dw', 'rosters') }}
)

select * from source
