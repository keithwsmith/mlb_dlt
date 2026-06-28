{{
    config(
        materialized = 'table'
    )
}}

/*
    dim_draft
    ---------
    Descriptive attributes for each draft pick context:
    player, team, school, position, and draft type.

    Source: dw.draft (raw landing table from dlt pipeline)
*/

with source as (

    select * from {{ source('dw', 'draft') }}
	where person__id IS NOT NULL

),
deduped as (

    select *
    from (

        select
            *,
            row_number() over (
                partition by
                    round_pick_number,
                    pick_number,
                    person__id,
                    person__draft_year
                order by _dlt_load_id desc
            ) as rn
        from source

    ) x
    where rn = 1

),


dim_draft as (

    select

        -- surrogate key
      {{ dbt_utils.generate_surrogate_key([
            'round_pick_number',
            'pick_number',
            'person__id',
            'person__draft_year'
        ]) }} as draft_key,       

        -- ── Player identifiers ──────────────────────────────────────────
        person__id                                          as person_id,
        bis_player_id                                       as bis_player_id,
		COALESCE(NULLIF(LTRIM(RTRIM(person__draft_year)), ''), '0') AS person__draft_year,
		round_pick_number									as round_pick_number,
		pick_number											as pick_number,

        -- ── Player name ─────────────────────────────────────────────────
		COALESCE(NULLIF(LTRIM(RTRIM(person__full_name)), ''), 'Unknown') AS person_full_name,
		COALESCE(NULLIF(LTRIM(RTRIM(person__first_name)), ''), 'Unknown') AS person_first_name,
		COALESCE(NULLIF(LTRIM(RTRIM(person__last_name)), ''), 'Unknown') AS person_last_name,
        person__middle_name                                 as person_middle_name,
        person__use_name                                    as person_use_name,
        person__use_last_name                               as person_use_last_name,
        person__nick_name                                   as person_nick_name,
        person__boxscore_name                               as person_boxscore_name,
        person__name_slug                                   as person_name_slug,
        person__name_title                                  as person_name_title,
        person__name_suffix                                 as person_name_suffix,
        person__name_matrilineal                            as person_name_matrilineal,
        person__pronunciation                               as person_pronunciation,

        -- ── Player demographics ─────────────────────────────────────────
        try_cast(person__birth_date as date)                as person_birth_date,
        person__birth_city                                  as person_birth_city,
        person__birth_state_province                        as person_birth_state_province,
        person__birth_country                               as person_birth_country,
        try_cast(person__death_date as date)                as person_death_date,
        person__death_city                                  as person_death_city,
        person__death_state_province                        as person_death_state_province,
        person__death_country                               as person_death_country,
        person__gender                                      as person_gender,

        -- ── Player physical ─────────────────────────────────────────────
        person__height                                      as person_height,
        cast(person__weight as int)                         as person_weight,
        person__strike_zone_top                             as person_strike_zone_top,
        person__strike_zone_bottom                          as person_strike_zone_bottom,

        -- ── Player handedness ───────────────────────────────────────────
        person__bat_side__code                              as person_bat_side_code,
        person__bat_side__description                       as person_bat_side_description,
        person__pitch_hand__code                            as person_pitch_hand_code,
        person__pitch_hand__description                     as person_pitch_hand_description,

        -- ── Player career ───────────────────────────────────────────────
        person__primary_number                              as person_primary_number,
        person__active                                      as person_is_active,
        person__is_player                                   as person_is_player,
        person__is_verified                                 as person_is_verified,
        try_cast(person__mlb_debut_date as date)            as person_mlb_debut_date,
        try_cast(person__last_played_date as date)          as person_last_played_date,
		COALESCE(NULLIF(LTRIM(RTRIM( cast(person__draft_year as int))), ''), 'Unknown') as person_draft_year,
        -- ── Player position ─────────────────────────────────────────────
        person__primary_position__code                      as person_primary_position_code,
        person__primary_position__name                      as person_primary_position_name,
        person__primary_position__type                      as person_primary_position_type,
        person__primary_position__abbreviation              as person_primary_position_abbreviation,

        -- ── Player home ─────────────────────────────────────────────────
        home__city                                          as home_city,
        home__state                                         as home_state,
        home__country                                       as home_country,

        -- ── Player links ────────────────────────────────────────────────
        person__link                                        as person_link,
        headshot_link                                       as headshot_link,
	

        -- ── Team ────────────────────────────────────────────────────────
        team__id                                            as team_id,
        team__name                                          as team_name,
        team__link                                          as team_link,
        team__all_star_status                               as team_all_star_status,
        team__spring_league__id                             as team_spring_league_id,
        team__spring_league__name                           as team_spring_league_name,
        team__spring_league__abbreviation                   as team_spring_league_abbreviation,
        team__spring_league__link                           as team_spring_league_link,

        -- ── School ──────────────────────────────────────────────────────
        school__name                                        as school_name,
        school__city                                        as school_city,
        school__state                                       as school_state,
        school__country                                     as school_country,
        school__school_class                                as school_class,

        -- ── Draft type ──────────────────────────────────────────────────
        draft_type__code                                    as draft_type_code,
        draft_type__description                             as draft_type_description,

        -- ── Audit ───────────────────────────────────────────────────────
        _dlt_id                                             as source_dlt_id

    from deduped


)

select * from dim_draft
