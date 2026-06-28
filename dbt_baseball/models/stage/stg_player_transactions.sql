{{
    config(materialized='view')
}}

with source as (
    select
        transaction_id,
        player_id,
        player_name,
        date            as transaction_date,
        effective_date,
        resolution_date,
        type_code,
        type_desc,
        description     as transaction_description,
        transaction_category,
        to_team_id,
        to_team_name,
        from_team_id,
        from_team_name,
        _dlt_load_id,
        _dlt_id
    from {{ source('dw', 'player_transactions') }}
)

select * from source
