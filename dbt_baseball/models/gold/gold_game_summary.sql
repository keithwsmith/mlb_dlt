{{
    config(
        materialized='table',
        schema='gold'
    )
}}

/*
    GOLD: one row per final game (all game types) -- the "game card".

    Everything an end user needs to read a game in one row, with no joins:
    date, teams (names and abbreviations), final score, winner and loser,
    pitchers of record, venue, attendance, length, weather, home-plate
    umpire, and the run differential / one-run / shutout flags.

    Sources: dim_games (schedule, score, status), dim_game_details (box-score
    totals, decisions, attendance, weather), dim_teams (abbreviation, league;
    joined on team_id AND season because dim_teams has one row per team per
    season), dim_game_umpires (home-plate umpire).

    Only is_final = 1 games. dim_games already excludes postponed and
    cancelled games.
*/

with games as (

    select * from {{ ref('dim_games') }}
    where is_final = 1

),

home_plate_umpire as (

    select game_pk, full_name as home_plate_umpire
    from (
        select
            game_pk,
            full_name,
            row_number() over (partition by game_pk order by official_id) as rn
        from {{ ref('dim_game_umpires') }}
        where official_type = 'Home Plate'
    ) u
    where rn = 1

)

select
    g.game_pk,
    g.season,
    g.game_type,
    g.game_type_desc,
    g.game_date,
    g.game_datetime,
    g.day_night,
    g.double_header,
    g.game_number,
    g.series_description,
    g.series_game_number,
    g.games_in_series,

    g.away_team_id,
    g.away_team_name,
    awt.abbreviation                                                         as away_abbreviation,
    awt.league_name                                                          as away_league_name,
    g.home_team_id,
    g.home_team_name,
    hmt.abbreviation                                                         as home_abbreviation,
    hmt.league_name                                                          as home_league_name,

    g.away_score,
    g.home_score,
    gd.away_hits,
    gd.home_hits,
    case
        when g.away_score > g.home_score then g.away_team_name
        when g.home_score > g.away_score then g.home_team_name
    end                                                                     as winning_team_name,
    case
        when g.away_score > g.home_score then g.home_team_name
        when g.home_score > g.away_score then g.away_team_name
    end                                                                     as losing_team_name,
    -- e.g. "NYY 5 @ BOS 3"
    coalesce(awt.abbreviation, g.away_team_name) + ' '
        + cast(g.away_score as varchar(5)) + ' @ '
        + coalesce(hmt.abbreviation, g.home_team_name) + ' '
        + cast(g.home_score as varchar(5))                                  as score_line,
    abs(g.home_score - g.away_score)                                        as run_margin,
    case when abs(g.home_score - g.away_score) = 1 then 1 else 0 end        as is_one_run_game,
    case when g.home_score = 0 or g.away_score = 0 then 1 else 0 end        as is_shutout,
    g.is_tie,

    gd.winning_pitcher_id,
    gd.winning_pitcher_name,
    gd.losing_pitcher_id,
    gd.losing_pitcher_name,
    gd.save_pitcher_id,
    gd.save_pitcher_name,

    g.venue_id,
    g.venue_name,
    gd.attendance,
    gd.game_duration_minutes,
    gd.weather_condition,
    gd.weather_temp,
    gd.weather_wind,
    u.home_plate_umpire
from games g
left join {{ ref('dim_game_details') }} gd
    on gd.game_pk = g.game_pk
left join {{ ref('dim_teams') }} hmt
    on hmt.team_id = g.home_team_id and hmt.season = g.season
left join {{ ref('dim_teams') }} awt
    on awt.team_id = g.away_team_id and awt.season = g.season
left join home_plate_umpire u
    on u.game_pk = g.game_pk
