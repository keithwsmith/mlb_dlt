-- models/intermediate/int_player_game_stats.sql
--
-- One row per PLAYER per COMPLETED GAME (batting/pitching/fielding box
-- score line), with season/date/team context already joined in -- mirrors
-- the shape of int_team_game_results.sql for the team-level models.
--
-- ASSUMPTIONS TO VERIFY:
--   1. Filters to game_type = 'R' (regular season) and dim_games.is_final = 1,
--      matching the filter used in int_team_game_results.sql.
--   2. pitching_innings_pitched is stored as MLB's fractional-outs notation
--      (e.g. '6.1' = 6 innings + 1 out, NOT 6.1 decimal innings). This model
--      converts it to pitching_outs_recorded (whole_innings*3 + partial_outs)
--      so it can be safely SUMmed downstream -- summing the raw string/decimal
--      directly would silently produce wrong innings totals (e.g. '0.2' + '0.2'
--      should be '1.1', not '0.4').
--   3. opponent_team_id/name derived from is_home vs. dim_games' home/away
--      team ids for that game_pk.
--   4. game_details is joined for start_hour context, for parity with
--      int_team_game_results.sql, even though no stat column depends on it.
--
-- INCREMENTAL STRATEGY:
--   Unlike int_team_game_results.sql, this model has no running counter
--   (like series_id) that needs prior-batch state to compute correctly --
--   every row here is self-contained (one player's stat line for one
--   game), so there's no equivalent of that model's prior_series CTE
--   needed. The only reason to scan a reduced window on incremental runs
--   at all is to pick up late scorer corrections to recently-played
--   games (box scores are occasionally amended a day or two after a
--   game is marked Final) -- LOOKBACK_DAYS below controls how far back
--   that re-scan goes. 3 days is a starting guess for how long box-score
--   corrections realistically trickle in; widen it if you find corrected
--   stats aren't being picked up in practice.
--
--   incremental_strategy='merge' on unique_key=(game_pk, player_id) means
--   a re-scanned game's rows simply overwrite what's already there --
--   safe to run with a generous lookback with no risk of duplicating rows.

{{
    config(
        materialized='incremental',
        unique_key=['game_pk', 'player_id'],
        incremental_strategy='merge'
    )
}}

with player_stats as (

    select
        game_pk,
        player_id,
        player_name,
        team_id,
        team_name,
        is_home,
		COALESCE(position_abbreviation, 'Unknown') AS position_abbreviation,
		COALESCE(position_code, 'Unknown')         AS position_code,
		COALESCE(position_name, 'Unknown')          AS position_name,
        pitching_innings_pitched,
        pitching_hits_allowed,
        pitching_runs_allowed,
        pitching_earned_runs,
        pitching_walks,
        pitching_strikeouts,
        pitching_home_runs_allowed,
        fielding_putouts,
        fielding_assists,
        fielding_errors,
        fielding_chances,
        batting_at_bats,
        batting_runs,
        batting_hits,
        batting_doubles,
        batting_triples,
        batting_home_runs,
        batting_rbi,
        batting_walks,
        batting_strikeouts,
        batting_stolen_bases,
        batting_left_on_base,
        batting_avg   as batting_avg_raw,    -- per-game rate, not summed -- see dim_player_stats_by_date
        pitching_era  as pitching_era_raw    -- per-game rate, not summed -- see dim_player_stats_by_date
    from {{ source('dw', 'player_game_stats') }}

),

games as (

    select
        game_pk,
        season,
        game_type,
        game_type_desc,
        game_date,
        game_datetime,
        home_team_id,
        home_team_name,
        away_team_id,
        away_team_name,
        is_final
    from {{ ref('dim_games') }}
    where game_type = 'R'     -- regular season, matching int_team_game_results.sql
      and is_final = 1
      {% if is_incremental() %}
      -- Reduced re-scan window on incremental runs -- see INCREMENTAL
      -- STRATEGY note above. Every column here is self-contained per
      -- row, so (unlike int_team_game_results.sql's series_id) there's
      -- no correctness requirement forcing a wider lookback -- this is
      -- purely about catching late box-score corrections.
      and game_date >= (
          select dateadd(day, -3, max(game_date)) from {{ this }}
      )
      {% endif %}

),

game_details as (

    select
        game_pk,
        first_pitch,
        first_pitch__v_text
    from {{ source('dw', 'game_details') }}

),

-- Convert pitching_innings_pitched's fractional-outs notation into a raw
-- outs count so it can be summed safely. See assumption #2 above.
innings_parsed as (

    select
        ps.*,
        case
            when ps.pitching_innings_pitched is null
              or ltrim(rtrim(ps.pitching_innings_pitched)) = ''
            then null
            else
                try_cast(
                    case
                        when charindex('.', ps.pitching_innings_pitched) > 0
                        then left(ps.pitching_innings_pitched, charindex('.', ps.pitching_innings_pitched) - 1)
                        else ps.pitching_innings_pitched
                    end as int
                ) * 3
                +
                case
                    when charindex('.', ps.pitching_innings_pitched) > 0
                    then try_cast(substring(
                            ps.pitching_innings_pitched,
                            charindex('.', ps.pitching_innings_pitched) + 1,
                            1
                         ) as int)
                    else 0
                end
        end as pitching_outs_recorded
    from player_stats ps

    -- On incremental runs, only bother parsing stat rows for games that
    -- actually fell inside this batch's reduced window -- avoids parsing
    -- the whole raw source table every run.
    {% if is_incremental() %}
    where ps.game_pk in (select game_pk from games)
    {% endif %}

)

select
    g.game_pk,
    g.season,
    g.game_type_desc,
    g.game_date,
    g.game_datetime,
    ip.player_id,
    ip.player_name,
    ip.team_id,
    ip.team_name,
    case when ip.is_home = 1 then g.away_team_id   else g.home_team_id   end as opponent_team_id,
    case when ip.is_home = 1 then g.away_team_name else g.home_team_name end as opponent_team_name,
    ip.is_home,
    ip.position_code,
    ip.position_name,
    ip.position_abbreviation,

    -- Pitching (counting stats)
    ip.pitching_outs_recorded,
    ip.pitching_hits_allowed,
    ip.pitching_runs_allowed,
    ip.pitching_earned_runs,
    ip.pitching_walks,
    ip.pitching_strikeouts,
    ip.pitching_home_runs_allowed,

    -- Fielding
    ip.fielding_putouts,
    ip.fielding_assists,
    ip.fielding_errors,
    ip.fielding_chances,

    -- Batting
    ip.batting_at_bats,
    ip.batting_runs,
    ip.batting_hits,
    ip.batting_doubles,
    ip.batting_triples,
    ip.batting_home_runs,
    ip.batting_rbi,
    ip.batting_walks,
    ip.batting_strikeouts,
    ip.batting_stolen_bases,
    ip.batting_left_on_base,

    -- Per-game rate stats, kept for reference only -- NOT summed downstream
    ip.batting_avg_raw,
    ip.pitching_era_raw,

    -- Game-time context (parity with int_team_game_results.sql)
    gd.first_pitch,
    gd.first_pitch__v_text,
    datepart(
        hour,
        coalesce(
            try_convert(time, gd.first_pitch__v_text),
            cast(gd.first_pitch as time)
        )
    ) as start_hour

from innings_parsed ip
inner join games g
    on ip.game_pk = g.game_pk
left join game_details gd
    on ip.game_pk = gd.game_pk