{{ config(materialized='table') }}

{#
    dim_person -- one row per MLB person id:
      - players (MLB and minor league, from dim_player)
      - coaches and managers (dw.team_coaches)
      - anyone else the MLB people registry knows (dw.people_details):
        executives, broadcasters, celebrities, independent / winter league
        players. Load these with:
            python mlb_load.py people_details --scope awards
    A person can be more than one of these (e.g. a former player who became
    a manager). is_other = 1 only for people who are none of player / coach.

    Use this for foreign keys that can hold anyone (award recipients).
    Keep player-only columns (batter_id, pitcher_id, ...) pointed at
    dim_player, which is the stricter check.

    People come from MLB's own registry (/api/v1/people), not from the award
    table itself, so the award relationships test still catches an id MLB
    doesn't know.
#}

with players as (
    select
        player_id                     as person_id,
        max(full_name)                as full_name,
        1                             as is_player,
        0                             as is_coach,
        0                             as is_manager,
        0                             as in_people_registry
    from {{ ref('dim_player') }}
    group by player_id
),

coaches as (
    select
        person_id,
        max(full_name)                as full_name,
        0                             as is_player,
        1                             as is_coach,
        max(case when job_id = 'MNGR' then 1 else 0 end) as is_manager,
        0                             as in_people_registry
    from {{ source('dw', 'team_coaches') }}
    where person_id is not null
    group by person_id
),

-- Everyone fetched from /people (coaches, award recipients, and players if
-- people_details was run with --scope all)
registry as (
    select
        id                            as person_id,
        max(full_name)                as full_name,
        0                             as is_player,
        0                             as is_coach,
        0                             as is_manager,
        1                             as in_people_registry
    from {{ source('dw', 'people_details') }}
    where id is not null
    group by id
),

combined as (
    select * from players
    union all
    select * from coaches
    union all
    select * from registry
),

rolled_up as (
    select
        person_id,
        max(full_name)          as full_name,
        max(is_player)          as is_player,
        max(is_coach)           as is_coach,
        max(is_manager)         as is_manager,
        max(in_people_registry) as in_people_registry
    from combined
    group by person_id
)

select
    {{ dbt_utils.generate_surrogate_key(['r.person_id']) }} as person_key,
    r.person_id,
    r.full_name,
    r.is_player,
    r.is_coach,
    r.is_manager,
    case when r.is_player = 0 and r.is_coach = 0 then 1 else 0 end as is_other,
    case
        when r.is_manager = 1 then 'Manager'
        when r.is_coach   = 1 then 'Coach'
        when r.is_player  = 1 then 'Player'
        else 'Other'
    end                                                       as person_type,
    pd.birth_date,
    pd.birth_country,
    pd.death_date,
    pd.mlb_debut_date
from rolled_up r
left join {{ source('dw', 'people_details') }} pd
    on pd.id = r.person_id
