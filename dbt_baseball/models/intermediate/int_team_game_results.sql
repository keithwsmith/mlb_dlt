-- models/intermediate/int_team_game_results.sql
--
-- One row per TEAM per COMPLETED game (i.e. two rows per game: one from the
-- home team's perspective, one from the away team's), with every situational
-- attribute needed by the dim_team_standings_* marts already derived here:
--   - result (W/L/T) from the team's own point of view
--   - is_home
--   - venue_id
--   - double_header / day_night flags (raw values, passed through)
--   - start_hour (hour-of-day of first_pitch)
--   - is_interleague (home team's league_id <> away team's league_id)
--   - series_id (derived — see note below) and series_game_number
--
-- Every downstream dim_team_standings_* model aggregates on top of this one
-- model, each at the grain that split actually needs (team+opponent,
-- team+venue, team+series, etc).
--
-- ASSUMPTIONS TO VERIFY:
--   1. CONFIRMED: game_type = 'R' is regular season in this data (distinct
--      values found: D, F, L, R, S, W -- D/F/L/W are postseason rounds,
--      S is spring training, R is regular season).
--   2. Queries dw.games / dw.game_details / dw.teams directly via
--      source('dw', ...) (see models/sources.yml). These are RAW dlt-
--      loaded tables, not a cleaned staging layer -- most columns below are
--      dlt's flattened nested-JSON names (double-underscore = nesting),
--      not the tidy names this model exposes downstream. See the column
--      mapping comments in each CTE below.
--   3. CONFIRMED: status__abstract_game_state = 'Final' is the correct
--      literal for a completed game in this data.
--   4. Series are consecutive dates between the same two teams at the same
--      venue (confirmed assumption) — series_id is derived by incrementing
--      a counter every time series_game_number resets to 1, within a given
--      home_team_id/away_team_id pairing.
--
-- FIRST_PITCH SCHEMA-VARIANT NOTE (important):
--   dw.game_details has BOTH `first_pitch` (datetimeoffset) and
--   `first_pitch__v_text` (nvarchar). This is dlt's schema-evolution
--   behavior: dlt inferred `first_pitch` as a datetime type from an
--   earlier load, and once mlb_load.py was changed to emit a local
--   "HH:MM" wall-clock STRING (see fetch_game_details), those string
--   values no longer parse as that inferred type -- so dlt routes them
--   into a separate `first_pitch__v_text` variant column instead of the
--   original `first_pitch` column. In practice this means:
--     - Rows loaded AFTER the mlb_load.py fix: correct local "HH:MM"
--       string lives in first_pitch__v_text.
--     - Rows loaded BEFORE the fix: whatever old value (likely the
--       original UTC timestamp -- the actual root cause of the hour-23
--       bug from before) lives in the legacy first_pitch column.
--   start_hour below COALESCEs first_pitch__v_text (preferred) with
--   first_pitch (fallback) -- so newly-loaded games get the correct local
--   hour, but games loaded before the fix may still show the old
--   (possibly UTC) hour until game_details is reloaded/backfilled for
--   those game_pks.
--
-- INCREMENTAL STRATEGY:
--   series_id is a running counter computed with a window function ordered
--   by date, partitioned by (home_team_id, away_team_id). If an incremental
--   run only scanned brand-new games, that window would restart counting
--   from 1 for each pairing instead of continuing where the last series left
--   off — silently wrong series_ids on every incremental run.
--
--   On incremental runs, the `games` CTE re-scans a 14-day LOOKBACK_DAYS
--   window of already-loaded games -- enough to always include the most
--   recent, possibly-still-in-progress series for every pairing, so
--   series_game_number=1 events are never missed mid-series. 14 days
--   covers real-world usage (each pairing plays series of 2-4 games
--   roughly every 1-2 weeks); widen it if your league/schedule has longer
--   gaps between series for the same pairing.
--
--   That reduced window is NOT, by itself, enough to get the series_id
--   NUMBER right, though: two teams can easily go >14 days between series
--   (e.g. one series in April, next not until August), so a pairing's
--   count of "series so far" can't be derived from the window alone.
--   Fix (prior total + batch delta, same pattern as
--   int_team_game_results_to_date): a prior_series CTE reads the max
--   series_id already assigned to each pairing from {{ this }}, and
--   games_with_series adds that as a base offset on top of the batch's own
--   running count of series starts within the reduced window. The merge on
--   unique_key=(game_pk, team_id) then overwrites already-loaded rows with
--   these (now correct) recomputed values -- only genuinely new games add
--   new rows.

{{
    config(
        materialized='incremental',
        unique_key=['game_pk', 'team_id'],
        incremental_strategy='merge'
    )
}}

with games as (

    select
        game_pk,
        try_convert(int, season)                as season,
        game_type                               as game_type_desc,  -- raw code (e.g. 'R'), NOT the word "Regular Season" -- see assumption #1
        try_convert(date, official_date)        as game_date,       -- official_date is nvarchar in the raw table
        game_date                               as game_datetime,   -- dw.games.game_date is the actual full timestamp (confusingly named) -- NOT the same thing as official_date above
        double_header,
        day_night,
        venue__id                               as venue_id,
        teams__home__team__id                   as home_team_id,
        teams__home__team__name                 as home_team_name,
        teams__home__score                      as home_score,
        teams__home__is_winner                  as home_is_winner,
        teams__away__team__id                   as away_team_id,
        teams__away__team__name                 as away_team_name,
        teams__away__score                      as away_score,
        teams__away__is_winner                  as away_is_winner,
        is_tie,
        games_in_series,
        series_game_number
    from {{ source('dw', 'games') }}
    where status__abstract_game_state = 'Final'  -- confirmed literal
      and game_type = 'R'                        -- confirmed literal (regular season)
	  and status__detailed_state NOT IN ('Postponed','Cancelled')
	  and is_tie <> 1
      {% if is_incremental() %}
      and try_convert(date, official_date) >= (
          select dateadd(day, -14, max(game_date)) from {{ this }}
      )
      {% endif %}

),

game_details as (

    select
        game_pk,
        first_pitch,             -- datetimeoffset; legacy/fallback -- see FIRST_PITCH SCHEMA-VARIANT NOTE above
        first_pitch__v_text      -- nvarchar "HH:MM" local time; preferred -- see FIRST_PITCH SCHEMA-VARIANT NOTE above
    from {{ source('dw', 'game_details') }}

),

-- team_league maps team_id + season -> league_id, used to flag interleague games.
-- dw.teams uses `id` (not team_id) and `league__id` (not league_id) --
-- season here is bigint, cast to match games.season above.
team_league as (

    select
        id          as team_id,
        season,
        league__id  as league_id
    from {{ source('dw', 'teams') }}

),

games_enriched as (

    select
        g.*,
        d.first_pitch,
        d.first_pitch__v_text,
        home_lg.league_id as home_league_id,
        away_lg.league_id as away_league_id
    from games g
    left join game_details d
        on g.game_pk = d.game_pk
    left join team_league home_lg
        on g.home_team_id = home_lg.team_id
       and g.season       = home_lg.season
    left join team_league away_lg
        on g.away_team_id = away_lg.team_id
       and g.season       = away_lg.season

),

{% if is_incremental() %}
-- Most recently assigned series_id per home/away pairing, read from
-- {{ this }} as it stood before this run. Needed because the 14-day
-- lookback window in the `games` CTE above is NOT on its own enough to
-- get series_id right: two teams can easily go >14 days without meeting
-- again (e.g. one series in April, next not until August), so naively
-- recomputing the running count over just the reduced window would
-- silently restart that pairing's counter at 1 instead of continuing its
-- true cumulative count -- corrupting series_id (and everything keyed off
-- it downstream) without erroring. Mirrors the prior_totals pattern used
-- in int_team_game_results_to_date.
prior_series as (

    select
        team_id           as home_team_id,
        opponent_team_id  as away_team_id,
        max(series_id)    as prior_max_series_id
    from {{ this }}
    where is_home = 1
    group by team_id, opponent_team_id

),
{% endif %}

-- Derive a series_id: a new series starts every time series_game_number
-- resets to 1, within a given home/away team pairing. On incremental runs
-- this is prior_max_series_id (the true cumulative count as of the last
-- run) PLUS a running count of series starts within this run's (reduced,
-- 14-day) batch -- NOT a from-scratch recount, since the batch alone
-- doesn't have full history. See prior_series note above.
games_with_series as (

    select
        e.*,
        {% if is_incremental() %}coalesce(ps.prior_max_series_id, 0) + {% endif %}
        sum(case when e.series_game_number = 1 then 1 else 0 end) over (
            partition by e.home_team_id, e.away_team_id
            order by e.game_date, e.game_datetime, e.game_pk
            rows unbounded preceding
        ) as series_id
    from games_enriched e
    {% if is_incremental() %}
    left join prior_series ps
        on ps.home_team_id = e.home_team_id
       and ps.away_team_id = e.away_team_id
    {% endif %}

),

-- Unpivot to one row per team per game: home perspective + away perspective
team_perspective as (

    select
        game_pk, season, game_type_desc, game_date, game_datetime,
        double_header, day_night, venue_id, first_pitch, first_pitch__v_text,
        games_in_series, series_game_number, series_id,
        home_team_id     as team_id,
        home_team_name   as team_name,
        away_team_id     as opponent_team_id,
        away_team_name   as opponent_team_name,
        home_score       as team_score,
        away_score       as opponent_score,
        1                as is_home,
        case
            when is_tie = 1 then 'T'
            when home_is_winner = 1 then 'W'
            else 'L'
        end as result,
        case
            when home_league_id is not null
             and away_league_id is not null
             and home_league_id <> away_league_id then 1
            else 0
        end as is_interleague
    from games_with_series

    union all

    select
        game_pk, season, game_type_desc, game_date, game_datetime,
        double_header, day_night, venue_id, first_pitch, first_pitch__v_text,
        games_in_series, series_game_number, series_id,
        away_team_id     as team_id,
        away_team_name   as team_name,
        home_team_id     as opponent_team_id,
        home_team_name   as opponent_team_name,
        away_score       as team_score,
        home_score       as opponent_score,
        0                as is_home,
        case
            when is_tie = 1 then 'T'
            when away_is_winner = 1 then 'W'
            else 'L'
        end as result,
        case
            when home_league_id is not null
             and away_league_id is not null
             and home_league_id <> away_league_id then 1
            else 0
        end as is_interleague
    from games_with_series

)

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
    result,                              -- 'W' / 'L' / 'T'
    double_header,
    day_night,
    -- Prefer first_pitch__v_text (the correct post-fix local "HH:MM"
    -- string); fall back to the legacy first_pitch datetimeoffset column
    -- for games loaded before the mlb_load.py fix -- see FIRST_PITCH
    -- SCHEMA-VARIANT NOTE at the top of this file. try_convert/cast return
    -- NULL instead of erroring on any stray malformed values rather than
    -- failing the whole model.
    datepart(
        hour,
        coalesce(
            try_convert(time, first_pitch__v_text),
            cast(first_pitch as time)
        )
    )                                                      as start_hour,
    is_interleague,
    series_id,
    games_in_series,
    series_game_number
from team_perspective