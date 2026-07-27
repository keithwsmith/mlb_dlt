"""
All SQL for the app lives here, in one place, on purpose.

IMPORTANT — READ THIS FIRST
============================
I (Claude) do not have access to your live SQL Server, so I don't know the
real column names in your `silver` star schema. mlb_load.py only shows me
the *raw* `dw` tables (populated straight from the MLB Stats API via dlt),
not the dbt models that presumably build `silver.dim_*` / `silver.fact_*`
on top of them. The column names below are my best, informed guess based on:
  - the raw field names dlt would produce from mlb_load.py's resources
  - standard star-schema naming conventions (dim_x has an x_id surrogate key)

Every query below is annotated with [ASSUMED] where I'm guessing. Run
`python inspect_schema.py` (in this folder) against your DB first — it
prints the actual columns for every table this app touches — then fix
any mismatches here. This file is the *only* place you should need to
edit column names; nothing else in the backend references raw SQL.
"""

# ---------------------------------------------------------------------------
# Games
# ---------------------------------------------------------------------------

SEASONS = """
    SELECT DISTINCT season
    FROM silver.dim_season
    ORDER BY season DESC
"""  # [ASSUMED] dim_season has a plain int `season` column (e.g. 2024)

TEAMS = """
    SELECT team_id, team_name, abbreviation
    FROM silver.dim_team
    ORDER BY team_name
"""  # [ASSUMED] dim_team(team_id, team_name, team_abbreviation)

GAMES_BY_SEASON = """
    SELECT
        g.game_pk,
        g.game_date,
        g.season,
        g.home_team_id,
        g.away_team_id,
        ht.abbreviation AS home_team_abbr,
        at.abbreviation AS away_team_abbr
    FROM silver.dim_games g
    JOIN silver.dim_team ht ON ht.team_id = g.home_team_id
    JOIN silver.dim_team at ON at.team_id = g.away_team_id
    WHERE g.season = ?
    ORDER BY g.game_date desc
"""

GAMES_BY_TEAM = """
    SELECT
        g.game_pk,
        g.game_date,
        g.season,
        g.home_team_id,
        g.away_team_id,
        ht.abbreviation AS home_team_abbr,
        at.abbreviation AS away_team_abbr
    FROM silver.dim_games g
    JOIN silver.dim_team ht ON ht.team_id = g.home_team_id
    JOIN silver.dim_team at ON at.team_id = g.away_team_id
    WHERE g.home_team_id = ? OR g.away_team_id = ?
    ORDER BY g.game_date
"""

GAMES_BY_TEAM_AND_OPPONENT = """
    SELECT
        g.game_pk,
        g.game_date,
        g.season,
        g.home_team_id,
        g.away_team_id,
        ht.abbreviation AS home_team_abbr,
        at.abbreviation AS away_team_abbr
    FROM silver.dim_games g
    JOIN silver.dim_team ht ON ht.team_id = g.home_team_id
    JOIN silver.dim_team at ON at.team_id = g.away_team_id
    WHERE (g.home_team_id = ? AND g.away_team_id = ?)
       OR (g.home_team_id = ? AND g.away_team_id = ?)
    ORDER BY g.game_date
"""

GAME_DETAILS = """
    SELECT *
    FROM silver.dim_game_details
    WHERE game_pk = ?
"""  # column list left as SELECT * — see fetch_game_details() in mlb_load.py
     # for the ~40 field names (weather_*, attendance, home_*/away_* box
     # score totals, winning/losing/save pitcher, etc.) if you want to
     # be explicit instead.

GAME_SEASON = """
    SELECT season
    FROM silver.dim_games
    WHERE game_pk = ?
"""  # reuses the same season column already selected in GAMES_BY_SEASON —
     # not a new assumption, just isolated into its own lookup so the YTD
     # endpoint can resolve game_pk -> season before querying the dw views.

GAME_UMPIRES = """
    SELECT game_pk, official_type, official_id, full_name
    FROM silver.dim_game_umpires
    WHERE game_pk = ?
"""  # matches umpires_resource's yield shape exactly (not a guess)

GAME_UMPIRE_PERFORMANCE = """
    SELECT up.*, u.full_name
    FROM silver.fact_umpire_performance up
    JOIN silver.dim_game_umpires u
      ON u.game_pk = up.game_pk AND u.official_id = up.official_id
    WHERE up.game_pk = ?
"""  # [ASSUMED — HEAVILY] there's no resource in mlb_load.py that produces
     # this table, so I have no field names to go on at all. SELECT * so
     # the frontend just renders whatever columns come back; rename/trim
     # once you know the real schema.
     #
     # Joined on official_id as well as game_pk — a game has multiple
     # umpires (home/first/second/third base) but presumably only one
     # performance row (ball/strike calls are a home-plate-umpire stat).
     # Joining on game_pk alone would fan that row out across every
     # umpire and pair it with the wrong name for 3 of them.

# ---------------------------------------------------------------------------
# Players & Venue (for a selected game, and standalone)
# ---------------------------------------------------------------------------

GAME_VENUE = """
    SELECT v.*
    FROM silver.dim_games g
    JOIN silver.dim_venue v ON v.venue_id = g.venue_id
    WHERE g.game_pk = ?
"""  # [ASSUMED] dim_games has venue_id; dim_venue has venue_id PK

GAME_PLAYERS = """
    SELECT DISTINCT p.player_id, p.full_name, p.primary_position
    FROM silver.fact_at_bats ab
    JOIN silver.dim_players p
      ON p.player_id IN (ab.batter_id, ab.pitcher_id)
    WHERE ab.game_pk = ?
    ORDER BY p.full_name
"""  # [ASSUMED] fact_at_bats(game_pk, batter_id, pitcher_id);
     # dim_players(player_id, full_name, primary_position)
	 
GAME_PLAYERS_STATS = """
   WITH player_activity AS (
    SELECT
        -- game_pk,
        player_id,
        player_name,
        --team_id,
        team_name,
        is_home,
        --position_code,
        position_name,
       -- position_abbreviation,
           CASE
            WHEN (
                    TRY_CAST(pitching_innings_pitched AS DECIMAL(5,1)) > 0
                 OR pitching_hits_allowed > 0
                 OR pitching_runs_allowed > 0
                 OR pitching_earned_runs > 0
                 OR pitching_walks > 0
                 OR pitching_strikeouts > 0
                 OR pitching_home_runs_allowed > 0
                 )
             AND (
                 batting_at_bats > 0
                 OR batting_runs > 0
                 OR batting_hits > 0
                 OR batting_doubles > 0
                 OR batting_triples > 0
                 OR batting_home_runs > 0
                 OR batting_rbi > 0
                 OR batting_walks > 0
                 OR batting_strikeouts > 0
                 OR batting_stolen_bases > 0
                 OR batting_left_on_base > 0
                 )
            THEN 'Both'
            WHEN (
                    TRY_CAST(pitching_innings_pitched AS DECIMAL(5,1)) > 0
                 OR pitching_hits_allowed > 0
                 OR pitching_runs_allowed > 0
                 OR pitching_earned_runs > 0
                 OR pitching_walks > 0
                 OR pitching_strikeouts > 0
                 OR pitching_home_runs_allowed > 0
                 )
            THEN 'Pitcher'
            WHEN (
                    fielding_putouts > 0
                 OR fielding_assists > 0
                 OR fielding_errors > 0
                 OR fielding_chances > 0
                 OR batting_at_bats > 0
                 OR batting_runs > 0
                 OR batting_hits > 0
                 OR batting_doubles > 0
                 OR batting_triples > 0
                 OR batting_home_runs > 0
                 OR batting_rbi > 0
                 OR batting_walks > 0
                 OR batting_strikeouts > 0
                 OR batting_stolen_bases > 0
                 OR batting_left_on_base > 0
                 )
            THEN 'Batter/Fielder'
            ELSE 'Did Not Play'
        END AS player_role,
        pitching_innings_pitched,
        pitching_hits_allowed,
        pitching_runs_allowed,
        pitching_earned_runs,
        pitching_walks,
        pitching_strikeouts,
        pitching_home_runs_allowed,
        pitching_era,
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
        batting_avg
    FROM [dw].[player_game_stats]
    WHERE game_pk = ?
)
SELECT *
FROM player_activity
WHERE player_role <> 'Did Not Play'
ORDER BY is_home DESC, position_name;
"""  # [ASSUMED] fact_at_bats(game_pk, batter_id, pitcher_id);
     # dim_players(player_id, full_name, primary_position)

ALL_PLAYERS = """
    SELECT player_id, full_name, position__name
    FROM silver.dim_players
    ORDER BY full_name
"""

PLAYER_SEASON_STATS = """
    SELECT
        s.season,
        s.age,
        t.abbreviation AS team_abbr,
        s.games_played,
        s.games_started,
        s.at_bats,
        s.runs,
        s.hits,
        s.doubles,
        s.triples,
        s.home_runs,
        s.rbi,
        s.stolen_bases,
        s.caught_stealing,
        s.walks,
        s.strikeouts,
        s.batting_average,
        s.on_base_percentage,
        s.slugging_percentage,
        s.ops,
        s.wins,
        s.losses,
        s.era,
        s.games_finished,
        s.saves,
        s.save_opportunities,
        s.holds,
        s.innings_pitched,
        s.hits_per9_inn,
        s.hits_allowed,
        s.runs_allowed,
        s.earned_runs,
        s.home_runs_allowed,
        s.walks_allowed,
        s.strikeouts_pitched,
        s.whip,
        s.position_code,
        s.games_at_position,
        s.putouts,
        s.assists,
        s.errors,
        s.double_plays,
        s.fielding_percentage,
        s.war
    FROM silver.fact_player_stats s
    LEFT JOIN silver.dim_team t ON t.team_id = s.team_key
    WHERE s.player_key = ?
    ORDER BY s.season DESC
"""  # [ASSUMED] player_key on fact_player_stats is the same id as player_id
     # used everywhere else in this app (dim_players.player_id / the id the
     # frontend already has from GAME_PLAYERS_STATS). If fact_player_stats
     # uses a different surrogate key than the natural MLB person id, change
     # the WHERE clause to whatever column actually matches.
     # [ASSUMED] team_key -> silver.dim_team.team_id, same FK pattern used
     # elsewhere in this file. LEFT JOIN (not JOIN) so a row still shows up
     # even if team_key doesn't resolve to a known team.

ALL_VENUES = """
    SELECT venue_id, venue_name, location_name
    FROM silver.dim_venue
    ORDER BY venue_name
"""  # [ASSUMED] no venue-producing resource exists in mlb_load.py at all —
     # confirm this table's real columns

PLAYER_DETAIL = """
    SELECT *
    FROM dw.mlbplayers
    WHERE id = ?
"""  # [ASSUMED] players_resource() yields the raw MLB `person` object,
     # whose primary key field is `id` (not `player_id`) — dlt would keep
     # that name as-is since it's not camelCase. If dbt renamed it on the
     # way into dw.mlbplayers, change `id` to whatever it's called.

PLAYER_BATTING_AT_BATS = """
    SELECT *
    FROM silver.fact_at_bats
    WHERE batter_id = ?
    ORDER BY game_pk
"""

PLAYER_PITCHING_AT_BATS = """
    SELECT *
    FROM silver.fact_at_bats
    WHERE pitcher_id = ?
    ORDER BY game_pk
"""

PLAYER_DRAFT = """
    SELECT TOP 1
        person__draft_year                                                       AS draft_year,
        round_pick_number                                                        AS [round],
        round_pick_number                                                        AS pick,
        person_primary_position_name,
        CONCAT_WS(', ', person_birth_city, person_birth_state_province, person_birth_country) AS birth_location,
        CONCAT_WS(', ', home_city, home_state, home_country)                                  AS home_location,
        CONCAT_WS(', ', school_name, school_city, school_state, school_country)                AS school_location,
        draft_type_description
    FROM silver.dim_draft
    WHERE person_id = ?
    ORDER BY draft_year DESC
"""  # TOP 1 + ORDER BY draft_year DESC -> a player drafted more than once
     # (e.g. out of high school, then again out of college) returns only
     # their most recent record. run_query_one() is used for this one in
     # main.py, not run_query — it's a single popup, not a list.

PLAYER_AWARDS = """
    SELECT
        a.award_name,
        ar.award_id,
        ar.player_id,
        ar.season,
        ar.award_date,
        ISNULL(t.abbreviation, 'Unknown') AS team
    FROM [dlt].[silver].[fact_award_recipient] ar
    JOIN [dlt].[silver].[dim_award] a ON a.award_id = ar.award_id
    LEFT OUTER JOIN [dlt].[silver].[dim_team] t on t.team_id = ar.team_id
    WHERE ar.player_id = ?
    order by  ar.season desc
"""  # matches the spec exactly (not a guess) — replaces the old
     # dim_award_recipient guess with the real fact/dim join

PLAYER_YTD_PITCHING = """
    SELECT
        age, games_played, games_started, games_finished, complete_games, shutouts,
        wins, losses, win_pct, saves, save_opportunities, blown_saves, holds,
        home_runs_allowed, walks, opp_avg, k_per9, strike_pct,
        inherited_runners, inherited_runners_scored
    FROM dw.vw_pitching_stats
    WHERE player_id = ?
    AND season = ?
"""  # matches the spec's pitching query, with one change: added
     # "AND season = ?". The spec's pitching version didn't filter by
     # season (only the batting version did), but since the whole point
     # of this endpoint is "get the season from the game_pk" first, an
     # unfiltered pitching query would return every season's row instead
     # of just the YTD one. Drop the season filter here if that was
     # intentional and you actually want the player's full history.

PLAYER_YTD_BATTING = """
    SELECT
        games_played, at_bats, plate_appearances, runs, hits,
        doubles, triples, home_runs, rbi, total_bases, stolen_bases, walks,
        strikeouts, gidp, avg, obp, slg, ops
    FROM [dw].[vw_batting_stats]
    WHERE player__id = ?
    AND season = ?
"""  # matches the spec exactly

# ---------------------------------------------------------------------------
# Teams
# ---------------------------------------------------------------------------

TEAM_DETAIL = """
    SELECT *
    FROM silver.dim_team
    WHERE team_id = ?
"""

TEAM_VENUE = """
    SELECT v.*
    FROM silver.dim_team t
    JOIN silver.dim_venue v ON v.venue_id = t.venue_id
    WHERE t.team_id = ?
"""  # [ASSUMED] dim_team has a venue_id FK

TEAM_DRAFT = """
    SELECT *
    FROM silver.dim_draft
    WHERE team_id = ?
    ORDER BY year DESC, round_number, pick_number
"""

# ---------------------------------------------------------------------------
# Standalone menu list pages
# ---------------------------------------------------------------------------

ALL_AWARDS = """
    SELECT *
    FROM silver.dim_award_recipient
    ORDER BY season DESC, award_name
"""

ALL_DRAFT_PICKS = """
    SELECT *
    FROM silver.dim_draft
    ORDER BY year DESC, round_number, pick_number
"""

ALL_UMPIRES = """
    SELECT DISTINCT official_id, full_name
    FROM silver.dim_game_umpires
    ORDER BY full_name
"""

# ---------------------------------------------------------------------------
# Matchups & Pitcher — NOT SPECIFIED
# ---------------------------------------------------------------------------
# The original request didn't describe what these two main-menu items
# should show. Stubbed as empty placeholders below (see main.py) so the
# nav is complete and the app runs; tell me what each should display and
# I'll fill these in for real.
