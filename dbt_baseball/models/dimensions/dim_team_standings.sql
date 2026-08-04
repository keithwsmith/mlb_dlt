-- models/marts/standings/dim_team_standings.sql
--
-- One row per TEAM per COMPLETED GAME (the general-purpose standings fact).
-- is_win / is_loss / is_tie are 0/1 flags so downstream consumers (BI tools,
-- or the more specific dim_team_standings_* splits below) can just SUM()
-- them without a CASE statement every time.

select
    game_pk,
    season,
    game_type_desc,
    game_date,
    game_datetime,
    team_id,
    team_name,
    opponent_team_id,
    opponent_team_name,
    is_home,
    venue_id,
    team_score,
    opponent_score,
    result,
    case when result = 'W' then 1 else 0 end as is_win,
    case when result = 'L' then 1 else 0 end as is_loss,
    case when result = 'T' then 1 else 0 end as is_tie,
    double_header,
    day_night,
    start_hour,
    is_interleague,
    series_id,
    games_in_series,
    series_game_number
from {{ ref('int_team_game_results') }}
