-- models/intermediate/int_team_game_results_to_date.sql
--
-- One row per TEAM per COMPLETED game (same grain as int_team_game_results),
-- with running "as of this game" win/loss totals split by venue, opponent
-- (further split by home/away), double_header, day_night, start_hour,
-- interleague, this specific series, games_in_series, and
-- series_game_number.
--
-- DAY_NIGHT AND DOUBLE_HEADER ARE PIVOTED, NOT "MATCHES THIS ROW": unlike
-- the other splits (which show the running total for whatever category
-- *this* row happens to be in), day_night and double_header each expose
-- all their category columns on every row, so you can see the full
-- breakdown at a glance regardless of what type the current game is:
--   - day_night: wins/losses_to_date_day_night_day,
--     wins/losses_to_date_day_night_night
--   - double_header: wins/losses_to_date_doubleheader_n,
--     wins/losses_to_date_doubleheader_y, wins/losses_to_date_doubleheader_s
--     (MLB Stats API doubleHeader codes: N = not a doubleheader, Y = game
--     of a single-admission doubleheader, S = game of a split-admission
--     doubleheader)
-- All other splits keep the original "matches this row's own value"
-- pattern.
--
-- SEASON RESET: every split EXCEPT the series one is scoped to `season` in
-- its partition -- i.e. these are "wins at this venue THIS SEASON", not a
-- career total. Without season in the partition, a team's wins_to_date_venue
-- would silently accumulate across every season in the warehouse (back to
-- 2000) instead of resetting each year -- that produced badly inflated,
-- nonsensical numbers in an earlier version of this model. series_id is the
-- one exception: it's derived as a running counter that never repeats
-- across seasons (see int_team_game_results), so wins_to_date_series is
-- already inherently season-safe without adding season to its partition.
--
-- INCREMENTAL STRATEGY (prior totals + batch delta):
--   A naive date-lookback (like int_team_game_results uses for series_id)
--   does NOT work here. series_id is safe with a 14-day lookback because a
--   series repeats within days. Most of these splits don't have that
--   locality -- a team may not revisit the same venue, face the same
--   opponent, or start at the same hour again for weeks -- so a running
--   total computed only over a recent date window would be missing most of
--   its true history and would understate/reset instead of continuing
--   correctly.
--
--   Instead, on an incremental run:
--     1. source_rows = only genuinely NEW (game_pk, team_id) rows from
--        int_team_game_results -- i.e. rows not already in this table.
--     2. batch_running = a running sum WITHIN that new batch only (window
--        functions, same as before).
--     3. prior_totals_* = the wins/losses already accumulated per
--        dimension group (now including season), read directly from THIS
--        table as it stood before this run (already-correct historical
--        totals -- no need to recompute them).
--     4. final wins/losses_to_date = prior_totals + batch_running.
--   This gives correct full-history running totals while only ever
--   scanning new rows plus one aggregate pass over the existing table --
--   no reprocessing of unchanged historical rows.
--
-- ASSUMPTION TO VERIFY: this pattern assumes new rows are always
-- chronologically at-or-after everything already in the table for the same
-- dimension group (true for normal day-to-day incremental loads of newly
-- completed games). If you ever backfill older, previously-missed games
-- out of order, the ENDING totals will still be correct (addition doesn't
-- care about order), but the per-row running values for rows in between
-- could be wrong for that stretch. Run `dbt run --full-refresh` on this
-- model after any out-of-order historical backfill to be safe.
--
-- to_date columns are inclusive of the current game.
--
-- NOTE on opponent split: (team_id, season, opponent_team_id, is_home) --
-- combine both is_home values downstream for the overall (non-split)
-- head-to-head within a season.
--
-- NOTE on series_id: scoped to (team_id, opponent_team_id, is_home,
-- series_id) -- deliberately NO season here, see SEASON RESET note above.
--
-- Only wins/losses to date are included (ties excluded), per what was
-- asked -- say the word if you'd also like ties_to_date_* columns.

{{
    config(
        materialized='incremental',
        unique_key=['game_pk', 'team_id'],
        incremental_strategy='merge'
    )
}}

with source_rows as (

    select
        s.*,
        case when s.result = 'W' then 1 else 0 end as is_win,
        case when s.result = 'L' then 1 else 0 end as is_loss
    from {{ ref('int_team_game_results') }} s
    {% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} t
        where t.game_pk = s.game_pk
          and t.team_id = s.team_id
    )
    {% endif %}

),

batch_running as (

    select
        *,
        sum(is_win)  over (partition by team_id, season, venue_id
                            order by game_date, game_datetime
                            rows unbounded preceding)          as batch_wins_venue,
        sum(is_loss) over (partition by team_id, season, venue_id
                            order by game_date, game_datetime
                            rows unbounded preceding)          as batch_losses_venue,

        sum(is_win)  over (partition by team_id, season, opponent_team_id, is_home
                            order by game_date, game_datetime
                            rows unbounded preceding)          as batch_wins_opponent,
        sum(is_loss) over (partition by team_id, season, opponent_team_id, is_home
                            order by game_date, game_datetime
                            rows unbounded preceding)          as batch_losses_opponent,

        -- double_header is pivoted into explicit N/Y/S columns (MLB Stats
        -- API doubleHeader codes: 'N' = not a doubleheader, 'Y' = game of a
        -- single-admission doubleheader, 'S' = game of a split-admission
        -- doubleheader) for the same reason as the day_night pivot above --
        -- "matches this row's own value" made a non-doubleheader game's
        -- wins_to_date_doubleheader silently track the team's cumulative
        -- NON-doubleheader record instead of reading as 0 on non-DH games.
        sum(case when double_header = 'N' and is_win  = 1 then 1 else 0 end)
            over (partition by team_id, season
                  order by game_date, game_datetime
                  rows unbounded preceding)                    as batch_wins_doubleheader_n,
        sum(case when double_header = 'N' and is_loss = 1 then 1 else 0 end)
            over (partition by team_id, season
                  order by game_date, game_datetime
                  rows unbounded preceding)                    as batch_losses_doubleheader_n,
        sum(case when double_header = 'Y' and is_win  = 1 then 1 else 0 end)
            over (partition by team_id, season
                  order by game_date, game_datetime
                  rows unbounded preceding)                    as batch_wins_doubleheader_y,
        sum(case when double_header = 'Y' and is_loss = 1 then 1 else 0 end)
            over (partition by team_id, season
                  order by game_date, game_datetime
                  rows unbounded preceding)                    as batch_losses_doubleheader_y,
        sum(case when double_header = 'S' and is_win  = 1 then 1 else 0 end)
            over (partition by team_id, season
                  order by game_date, game_datetime
                  rows unbounded preceding)                    as batch_wins_doubleheader_s,
        sum(case when double_header = 'S' and is_loss = 1 then 1 else 0 end)
            over (partition by team_id, season
                  order by game_date, game_datetime
                  rows unbounded preceding)                    as batch_losses_doubleheader_s,

        -- day_night is pivoted into explicit day/night columns (not just
        -- "matches this row's own day_night value") so every row shows the
        -- full day-vs-night breakdown at a glance, computed unconditionally
        -- over all of team_id+season (not partitioned by day_night).
        sum(case when day_night = 'day'   and is_win  = 1 then 1 else 0 end)
            over (partition by team_id, season
                  order by game_date, game_datetime
                  rows unbounded preceding)                    as batch_wins_day_night_day,
        sum(case when day_night = 'day'   and is_loss = 1 then 1 else 0 end)
            over (partition by team_id, season
                  order by game_date, game_datetime
                  rows unbounded preceding)                    as batch_losses_day_night_day,
        sum(case when day_night = 'night' and is_win  = 1 then 1 else 0 end)
            over (partition by team_id, season
                  order by game_date, game_datetime
                  rows unbounded preceding)                    as batch_wins_day_night_night,
        sum(case when day_night = 'night' and is_loss = 1 then 1 else 0 end)
            over (partition by team_id, season
                  order by game_date, game_datetime
                  rows unbounded preceding)                    as batch_losses_day_night_night,

        sum(is_win)  over (partition by team_id, season, start_hour
                            order by game_date, game_datetime
                            rows unbounded preceding)          as batch_wins_start_hour,
        sum(is_loss) over (partition by team_id, season, start_hour
                            order by game_date, game_datetime
                            rows unbounded preceding)          as batch_losses_start_hour,

        sum(is_win)  over (partition by team_id, season, is_interleague
                            order by game_date, game_datetime
                            rows unbounded preceding)          as batch_wins_interleague,
        sum(is_loss) over (partition by team_id, season, is_interleague
                            order by game_date, game_datetime
                            rows unbounded preceding)          as batch_losses_interleague,

        -- deliberately NO season in this partition -- series_id already
        -- never repeats across seasons, see SEASON RESET note above.
        sum(is_win)  over (partition by team_id, opponent_team_id, is_home, series_id
                            order by game_date, game_datetime
                            rows unbounded preceding)          as batch_wins_series,
        sum(is_loss) over (partition by team_id, opponent_team_id, is_home, series_id
                            order by game_date, game_datetime
                            rows unbounded preceding)          as batch_losses_series,

        sum(is_win)  over (partition by team_id, season, games_in_series
                            order by game_date, game_datetime
                            rows unbounded preceding)          as batch_wins_games_in_series,
        sum(is_loss) over (partition by team_id, season, games_in_series
                            order by game_date, game_datetime
                            rows unbounded preceding)          as batch_losses_games_in_series,

        sum(is_win)  over (partition by team_id, season, series_game_number
                            order by game_date, game_datetime
                            rows unbounded preceding)          as batch_wins_series_game_number,
        sum(is_loss) over (partition by team_id, season, series_game_number
                            order by game_date, game_datetime
                            rows unbounded preceding)          as batch_losses_series_game_number
    from source_rows

)

{% if is_incremental() %}
,
prior_totals_venue as (
    select team_id, season, venue_id,
           sum(case when result = 'W' then 1 else 0 end) as prior_wins,
           sum(case when result = 'L' then 1 else 0 end) as prior_losses
    from {{ this }}
    group by team_id, season, venue_id
),
prior_totals_opponent as (
    select team_id, season, opponent_team_id, is_home,
           sum(case when result = 'W' then 1 else 0 end) as prior_wins,
           sum(case when result = 'L' then 1 else 0 end) as prior_losses
    from {{ this }}
    group by team_id, season, opponent_team_id, is_home
),
prior_totals_doubleheader as (
    select team_id, season,
           sum(case when double_header = 'N' and result = 'W' then 1 else 0 end) as prior_wins_n,
           sum(case when double_header = 'N' and result = 'L' then 1 else 0 end) as prior_losses_n,
           sum(case when double_header = 'Y' and result = 'W' then 1 else 0 end) as prior_wins_y,
           sum(case when double_header = 'Y' and result = 'L' then 1 else 0 end) as prior_losses_y,
           sum(case when double_header = 'S' and result = 'W' then 1 else 0 end) as prior_wins_s,
           sum(case when double_header = 'S' and result = 'L' then 1 else 0 end) as prior_losses_s
    from {{ this }}
    group by team_id, season
),
prior_totals_day_night as (
    select team_id, season,
           sum(case when day_night = 'day'   and result = 'W' then 1 else 0 end) as prior_wins_day,
           sum(case when day_night = 'day'   and result = 'L' then 1 else 0 end) as prior_losses_day,
           sum(case when day_night = 'night' and result = 'W' then 1 else 0 end) as prior_wins_night,
           sum(case when day_night = 'night' and result = 'L' then 1 else 0 end) as prior_losses_night
    from {{ this }}
    group by team_id, season
),
prior_totals_start_hour as (
    select team_id, season, start_hour,
           sum(case when result = 'W' then 1 else 0 end) as prior_wins,
           sum(case when result = 'L' then 1 else 0 end) as prior_losses
    from {{ this }}
    group by team_id, season, start_hour
),
prior_totals_interleague as (
    select team_id, season, is_interleague,
           sum(case when result = 'W' then 1 else 0 end) as prior_wins,
           sum(case when result = 'L' then 1 else 0 end) as prior_losses
    from {{ this }}
    group by team_id, season, is_interleague
),
-- deliberately NO season here -- see SEASON RESET note above.
prior_totals_series as (
    select team_id, opponent_team_id, is_home, series_id,
           sum(case when result = 'W' then 1 else 0 end) as prior_wins,
           sum(case when result = 'L' then 1 else 0 end) as prior_losses
    from {{ this }}
    group by team_id, opponent_team_id, is_home, series_id
),
prior_totals_games_in_series as (
    select team_id, season, games_in_series,
           sum(case when result = 'W' then 1 else 0 end) as prior_wins,
           sum(case when result = 'L' then 1 else 0 end) as prior_losses
    from {{ this }}
    group by team_id, season, games_in_series
),
prior_totals_series_game_number as (
    select team_id, season, series_game_number,
           sum(case when result = 'W' then 1 else 0 end) as prior_wins,
           sum(case when result = 'L' then 1 else 0 end) as prior_losses
    from {{ this }}
    group by team_id, season, series_game_number
)
{% endif %}

select
    b.game_pk,
    b.season,
    b.game_type_desc,
    b.game_date,
    b.game_datetime,
    b.team_id,
    b.team_name,
    b.opponent_team_id,
    b.opponent_team_name,
    b.is_home,
    b.venue_id,
    b.team_score,
    b.opponent_score,
    b.result,
    b.double_header,
    b.day_night,
    b.start_hour,
    b.is_interleague,
    b.series_id,
    b.games_in_series,
    b.series_game_number,

    {% if is_incremental() %}coalesce(pv.prior_wins, 0)   + {% endif %}b.batch_wins_venue    as wins_to_date_venue,
    {% if is_incremental() %}coalesce(pv.prior_losses, 0) + {% endif %}b.batch_losses_venue  as losses_to_date_venue,

    {% if is_incremental() %}coalesce(po.prior_wins, 0)   + {% endif %}b.batch_wins_opponent    as wins_to_date_opponent,
    {% if is_incremental() %}coalesce(po.prior_losses, 0) + {% endif %}b.batch_losses_opponent  as losses_to_date_opponent,

    {% if is_incremental() %}coalesce(pd.prior_wins_n, 0)   + {% endif %}b.batch_wins_doubleheader_n    as wins_to_date_doubleheader_n,
    {% if is_incremental() %}coalesce(pd.prior_losses_n, 0) + {% endif %}b.batch_losses_doubleheader_n  as losses_to_date_doubleheader_n,
    {% if is_incremental() %}coalesce(pd.prior_wins_y, 0)   + {% endif %}b.batch_wins_doubleheader_y    as wins_to_date_doubleheader_y,
    {% if is_incremental() %}coalesce(pd.prior_losses_y, 0) + {% endif %}b.batch_losses_doubleheader_y  as losses_to_date_doubleheader_y,
    {% if is_incremental() %}coalesce(pd.prior_wins_s, 0)   + {% endif %}b.batch_wins_doubleheader_s    as wins_to_date_doubleheader_s,
    {% if is_incremental() %}coalesce(pd.prior_losses_s, 0) + {% endif %}b.batch_losses_doubleheader_s  as losses_to_date_doubleheader_s,

    {% if is_incremental() %}coalesce(pdn.prior_wins_day, 0)     + {% endif %}b.batch_wins_day_night_day       as wins_to_date_day_night_day,
    {% if is_incremental() %}coalesce(pdn.prior_losses_day, 0)   + {% endif %}b.batch_losses_day_night_day     as losses_to_date_day_night_day,
    {% if is_incremental() %}coalesce(pdn.prior_wins_night, 0)   + {% endif %}b.batch_wins_day_night_night     as wins_to_date_day_night_night,
    {% if is_incremental() %}coalesce(pdn.prior_losses_night, 0) + {% endif %}b.batch_losses_day_night_night   as losses_to_date_day_night_night,

    {% if is_incremental() %}coalesce(psh.prior_wins, 0)   + {% endif %}b.batch_wins_start_hour    as wins_to_date_start_hour,
    {% if is_incremental() %}coalesce(psh.prior_losses, 0) + {% endif %}b.batch_losses_start_hour  as losses_to_date_start_hour,

    {% if is_incremental() %}coalesce(pil.prior_wins, 0)   + {% endif %}b.batch_wins_interleague    as wins_to_date_interleague,
    {% if is_incremental() %}coalesce(pil.prior_losses, 0) + {% endif %}b.batch_losses_interleague  as losses_to_date_interleague,

    {% if is_incremental() %}coalesce(ps.prior_wins, 0)   + {% endif %}b.batch_wins_series    as wins_to_date_series,
    {% if is_incremental() %}coalesce(ps.prior_losses, 0) + {% endif %}b.batch_losses_series  as losses_to_date_series,

    {% if is_incremental() %}coalesce(pgis.prior_wins, 0)   + {% endif %}b.batch_wins_games_in_series    as wins_to_date_games_in_series,
    {% if is_incremental() %}coalesce(pgis.prior_losses, 0) + {% endif %}b.batch_losses_games_in_series  as losses_to_date_games_in_series,

    {% if is_incremental() %}coalesce(psgn.prior_wins, 0)   + {% endif %}b.batch_wins_series_game_number    as wins_to_date_series_game_number,
    {% if is_incremental() %}coalesce(psgn.prior_losses, 0) + {% endif %}b.batch_losses_series_game_number  as losses_to_date_series_game_number

from batch_running b
{% if is_incremental() %}
left join prior_totals_venue pv
    on pv.team_id = b.team_id and pv.season = b.season and pv.venue_id = b.venue_id
left join prior_totals_opponent po
    on po.team_id = b.team_id and po.season = b.season
   and po.opponent_team_id = b.opponent_team_id and po.is_home = b.is_home
left join prior_totals_doubleheader pd
    on pd.team_id = b.team_id and pd.season = b.season
left join prior_totals_day_night pdn
    on pdn.team_id = b.team_id and pdn.season = b.season
left join prior_totals_start_hour psh
    on psh.team_id = b.team_id and psh.season = b.season
   and (psh.start_hour = b.start_hour or (psh.start_hour is null and b.start_hour is null))
left join prior_totals_interleague pil
    on pil.team_id = b.team_id and pil.season = b.season and pil.is_interleague = b.is_interleague
-- deliberately NO season join here -- see SEASON RESET note above.
left join prior_totals_series ps
    on ps.team_id = b.team_id and ps.opponent_team_id = b.opponent_team_id
   and ps.is_home = b.is_home and ps.series_id = b.series_id
left join prior_totals_games_in_series pgis
    on pgis.team_id = b.team_id and pgis.season = b.season and pgis.games_in_series = b.games_in_series
left join prior_totals_series_game_number psgn
    on psgn.team_id = b.team_id and psgn.season = b.season and psgn.series_game_number = b.series_game_number
{% endif %}