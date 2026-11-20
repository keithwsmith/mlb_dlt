{{
    config(materialized='view')
}}

{#
    Minor league players loaded by mlb_load.py (resource milb_players).
    One row per player per level per season. Column names match
    stg_rosters so dim_player can stack the two.

    team_id is the MLB parent organization (parent_org_id), the same
    meaning as stg_rosters.team_id (rosters.parent_team_id), so it still
    joins to dim_teams. The minor league club itself is affiliate_team_id.
#}

with source as (
    select
        id                              as player_id,
        full_name,
        primary_number                  as jersey_number,
        primary_position__code          as position__code,
        primary_position__name          as position__name,
        primary_position__type          as position__type,
        primary_position__abbreviation  as position__abbreviation,
        season,
        sport_id,
        level,
        parent_org_id                   as team_id,
        current_team_id                 as affiliate_team_id,
        current_team_name               as affiliate_team_name,
        _dlt_load_id,
        _dlt_id
    from {{ source('dw', 'milb_players') }}
)

select * from source
