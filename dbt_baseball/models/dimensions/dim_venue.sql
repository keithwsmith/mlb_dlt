{{
    config(
        materialized='table',
        unique_key='venue_id'
    )
}}

-- One row per venue, built from dw.venues as loaded by mlb_load.py (venues resource).
--
-- Referenced venues the API has no record for: some parks used in games or
-- listed as a team's home (minor league, international, one-off neutral sites)
-- never come back from /venues. For those, a basic row is built from what
-- dw.games, dw.teams and dw.milb_teams carry about the venue -- id, name, link --
-- so every venue_id in dim_games and dim_teams has a dim_venue row. Location,
-- time zone and field columns are NULL on these rows. The name comes from the
-- most recent season that names the venue.
--
-- venue_source says where each row came from:
--   'api'          dw.venues (full details)
--   'referenced'   built from games / teams (name and link only)
--   'placeholder'  the Unknown Venue row (9999)
--
-- Venue 9999 is the MLB API's placeholder for an unknown / TBD park; the API
-- has no record for it, so an "Unknown Venue" row is added here so the
-- dim_games.venue_id -> dim_venue relationships test passes.

with venues as (

    select
        cast(id as int)                                     as venue_id,
        name                                                as venue_name,
        link                                                as venue_link,
        cast(active as bit)                                 as is_active,
        try_cast(season as int)                             as season,

        -- location
        location__address1                                  as address1,
        location__address2                                  as address2,
        location__city                                      as city,
        location__state                                     as state,
        location__state_abbrev                              as state_abbrev,
        location__postal_code                               as postal_code,
        location__country                                   as country,
        location__phone                                     as phone,
        concat_ws(', ', location__city,
                  coalesce(location__state_abbrev, location__state),
                  location__country)                        as location_name,
        location__default_coordinates__latitude             as latitude,
        location__default_coordinates__longitude            as longitude,
        location__azimuth_angle                             as azimuth_angle,
        cast(location__elevation as int)                    as elevation_ft,

        -- time zone
        time_zone__id                                       as time_zone_id,
        time_zone__tz                                       as time_zone_abbrev,
        cast(time_zone__offset as int)                      as time_zone_offset,
        cast(time_zone__offset_at_game_time as int)         as time_zone_offset_at_game_time,

        -- field
        cast(field_info__capacity as int)                   as capacity,
        field_info__turf_type                               as turf_type,
        field_info__roof_type                               as roof_type,
        cast(field_info__left_line as int)                  as left_line_ft,
        cast(field_info__left as int)                       as left_field_ft,
        cast(field_info__left_center as int)                as left_center_ft,
        cast(field_info__center as int)                     as center_field_ft,
        cast(field_info__right_center as int)               as right_center_ft,
        cast(field_info__right as int)                      as right_field_ft,
        cast(field_info__right_line as int)                 as right_line_ft,

        _dlt_load_id                                        as dlt_load_id,
        cast('api' as nvarchar(20))                         as venue_source
    from {{ source('dw', 'venues') }}

),

-- Every venue id used by a game or as a team's home / spring park, with the
-- name and link the source row gives it (spring parks carry only an id).
venue_references as (

    select cast(venue__id as int) as venue_id, venue__name as venue_name,
           venue__link as venue_link, try_cast(season as int) as season
    from {{ source('dw', 'games') }}

    union all
    select cast(venue__id as int), venue__name, venue__link, try_cast(season as int)
    from {{ source('dw', 'teams') }}

    union all
    select cast(venue__id as int), venue__name, venue__link, try_cast(season as int)
    from {{ source('dw', 'milb_teams') }}

    union all
    select cast(spring_venue__id as int), cast(null as nvarchar(max)),
           cast(null as nvarchar(max)), try_cast(season as int)
    from {{ source('dw', 'teams') }}

),

-- One row per referenced venue missing from dw.venues: the name from the
-- latest season that has one (rows with a name sort first).
referenced_missing as (

    select venue_id, venue_name, venue_link, season
    from (
        select
            r.*,
            row_number() over (
                partition by r.venue_id
                order by case when r.venue_name is null then 1 else 0 end,
                         r.season desc
            ) as rn
        from venue_references r
        where r.venue_id is not null
          and r.venue_id not in (0, 9999)     -- 0 = none; 9999 = Unknown Venue below
          and not exists (select 1 from venues v where v.venue_id = r.venue_id)
    ) x
    where rn = 1

),

referenced_venues as (

    select
        venue_id,
        coalesce(venue_name, concat('Venue ', venue_id))   as venue_name,
        cast(venue_link as nvarchar(max))  as venue_link,
        cast(null as bit)               as is_active,
        season,
        cast(null as nvarchar(max)) as address1,
        cast(null as nvarchar(max)) as address2,
        cast(null as nvarchar(max)) as city,
        cast(null as nvarchar(max)) as state,
        cast(null as nvarchar(max)) as state_abbrev,
        cast(null as nvarchar(max)) as postal_code,
        cast(null as nvarchar(max)) as country,
        cast(null as nvarchar(max)) as phone,
        'Unknown'                   as location_name,
        cast(null as float)         as latitude,
        cast(null as float)         as longitude,
        cast(null as float)         as azimuth_angle,
        cast(null as int)           as elevation_ft,
        cast(null as nvarchar(max)) as time_zone_id,
        cast(null as nvarchar(max)) as time_zone_abbrev,
        cast(null as int)           as time_zone_offset,
        cast(null as int)           as time_zone_offset_at_game_time,
        cast(null as int)           as capacity,
        cast(null as nvarchar(max)) as turf_type,
        cast(null as nvarchar(max)) as roof_type,
        cast(null as int)           as left_line_ft,
        cast(null as int)           as left_field_ft,
        cast(null as int)           as left_center_ft,
        cast(null as int)           as center_field_ft,
        cast(null as int)           as right_center_ft,
        cast(null as int)           as right_field_ft,
        cast(null as int)           as right_line_ft,
        cast(null as nvarchar(max)) as dlt_load_id,
        cast('referenced' as nvarchar(20)) as venue_source
    from referenced_missing

),

unknown_venue as (

    select
        9999                        as venue_id,
        'Unknown Venue'             as venue_name,
        cast(null as nvarchar(max)) as venue_link,
        cast(0 as bit)              as is_active,
        cast(null as int)           as season,
        cast(null as nvarchar(max)) as address1,
        cast(null as nvarchar(max)) as address2,
        cast(null as nvarchar(max)) as city,
        cast(null as nvarchar(max)) as state,
        cast(null as nvarchar(max)) as state_abbrev,
        cast(null as nvarchar(max)) as postal_code,
        cast(null as nvarchar(max)) as country,
        cast(null as nvarchar(max)) as phone,
        'Unknown'                   as location_name,
        cast(null as float)         as latitude,
        cast(null as float)         as longitude,
        cast(null as float)         as azimuth_angle,
        cast(null as int)           as elevation_ft,
        cast(null as nvarchar(max)) as time_zone_id,
        cast(null as nvarchar(max)) as time_zone_abbrev,
        cast(null as int)           as time_zone_offset,
        cast(null as int)           as time_zone_offset_at_game_time,
        cast(null as int)           as capacity,
        cast(null as nvarchar(max)) as turf_type,
        cast(null as nvarchar(max)) as roof_type,
        cast(null as int)           as left_line_ft,
        cast(null as int)           as left_field_ft,
        cast(null as int)           as left_center_ft,
        cast(null as int)           as center_field_ft,
        cast(null as int)           as right_center_ft,
        cast(null as int)           as right_field_ft,
        cast(null as int)           as right_line_ft,
        cast(null as nvarchar(max)) as dlt_load_id,
        cast('placeholder' as nvarchar(20)) as venue_source
    where not exists (select 1 from venues where venue_id = 9999)

),

combined as (
    select * from venues
    union all
    select * from referenced_venues
    union all
    select * from unknown_venue
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['venue_id']) }} as venue_key,
        *
    from combined
)

select * from final
