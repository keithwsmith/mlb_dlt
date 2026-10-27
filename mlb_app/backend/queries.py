"""
All SQL for the app lives here, in one place, on purpose.


"""

# ---------------------------------------------------------------------------
# Games
# ---------------------------------------------------------------------------

SEASONS = """
    SELECT DISTINCT season
    FROM silver.dim_season
    where season < 2027
    ORDER BY season DESC
"""  # [ASSUMED] dim_season has a plain int `season` column (e.g. 2024)

TEAMS = """
   SELECT
    team_id,
    team_name,
    abbreviation,
    season
FROM (
    SELECT
        team_id,
        team_name,
        abbreviation,
        season,
        ROW_NUMBER() OVER (
            PARTITION BY team_id
            ORDER BY season DESC
        ) AS rn
    FROM silver.dim_teams
) t
WHERE rn = 1
ORDER BY team_name;
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
    JOIN silver.dim_teams ht ON ht.team_id = g.home_team_id and ht.season = g.season
    JOIN silver.dim_teams at ON at.team_id = g.away_team_id and at.season = g.season
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
        ht.team_name AS home_team_name,
        at.abbreviation AS away_team_abbr,
        at.team_name AS away_team_name
    FROM silver.dim_games g
    LEFT JOIN silver.dim_teams ht ON ht.team_id = g.home_team_id and ht.season = g.season
    LEFT JOIN silver.dim_teams at ON at.team_id = g.away_team_id and at.season = g.season
    WHERE g.home_team_id = ? OR g.away_team_id = ?
    ORDER BY g.game_date DESC
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
    LEFT JOIN silver.dim_teams ht ON ht.team_id = g.home_team_id and ht.season = g.season
    LEFT JOIN silver.dim_teams at ON at.team_id = g.away_team_id and at.season = g.season
    WHERE (g.home_team_id = ? AND g.away_team_id = ?)
       OR (g.home_team_id = ? AND g.away_team_id = ?)
    ORDER BY g.game_date DESC
"""

GAME_DETAILS = """
    SELECT g.venue_name, gd.*
    FROM silver.dim_game_details gd
    JOIN silver.dim_games g ON g.game_pk = gd.game_pk
    WHERE gd.game_pk = ?
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

GAME_MATCHUP = """
    SELECT
        g.game_pk,
        g.game_date,
        g.home_team_id,
        g.away_team_id,
        ht.team_name AS home_team_name,
        at.team_name AS away_team_name
    FROM silver.dim_games g
    LEFT JOIN silver.dim_teams ht ON ht.team_id = g.home_team_id and ht.season = g.season
    LEFT JOIN silver.dim_teams at ON at.team_id = g.away_team_id and at.season = g.season
    WHERE g.game_pk = ?
"""  # dim_game_details (see GAME_DETAILS below) doesn't carry team names or
     # game_date -- it's weather/attendance/box-score totals. This is the
     # same join pattern as GAMES_BY_TEAM, just filtered to one game_pk
     # instead of a team, specifically for the page header's away @ home +
     # date display.

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
"""  

# ---------------------------------------------------------------------------
# Players & Venue (for a selected game, and standalone)
# ---------------------------------------------------------------------------

GAME_VENUE = """
    SELECT v.venue_name,v.location_name,v.first_year_of_play
    FROM silver.dim_games g
    JOIN silver.dim_venue v ON v.venue_id = g.venue_id
    WHERE g.game_pk = ?
"""  # [ASSUMED] dim_games has venue_id; dim_venue has venue_id PK

GAME_PLAYERS = """
   SELECT DISTINCT p.player_id, p.full_name, p.position__name
FROM silver.fact_at_bats ab
JOIN silver.dim_player p
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
                  TRY_CAST(pitching_outs_recorded AS DECIMAL(5,1)) > 0
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
                  TRY_CAST(pitching_outs_recorded AS DECIMAL(5,1)) > 0
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
      pitching_outs_recorded,
      CASE
          WHEN TRY_CAST(pitching_outs_recorded AS INT) IS NULL THEN NULL
          ELSE CAST(TRY_CAST(pitching_outs_recorded AS INT) / 3 AS VARCHAR)
               + '.' +
               CAST(TRY_CAST(pitching_outs_recorded AS INT) % 3 AS VARCHAR)
      END AS innings_pitched,
      pitching_hits_allowed,
      pitching_runs_allowed,
      pitching_earned_runs,
      pitching_walks,
      pitching_strikeouts,
      pitching_home_runs_allowed,
      pitching_era_raw,
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
      batting_avg_raw
  FROM silver.dim_player_stats
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
    FROM silver.dim_player
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
       LEFT JOIN silver.dim_teams t ON t.team_key = s.team_key 
    WHERE s.player_key = ?
    ORDER BY s.season DESC
"""  

ALL_VENUES = """
    SELECT venue_name,location_name,first_year_of_play
    FROM silver.dim_venue
    ORDER BY venue_name
"""  # [ASSUMED] no venue-producing resource exists in mlb_load.py at all —
     # confirm this table's real columns

PLAYER_DETAIL = """
    SELECT  id,full_name,birth_date,current_age,
	CONCAT_WS(', ', birth_city, birth_state_province, birth_country) AS birth_place,
	concat(height,' ',weight,' lbs.') as 'height/weight',
	primary_position__name as position,
	mlb_debut_date as debut,
	CONCAT('Bats',' ',bat_side__description,' ','Throws',' ',pitch_hand__description) as 'Bats/Throws',
	CONCAT_WS(', ',birth_city,birth_state_province,birth_country) as birthplace
    FROM dw.mlbplayers
    WHERE id = ?
""" 
PLAYER_TRANSACTIONS = """
    SELECT 
      date,
      description
  FROM dw.player_transactions
  where player_id = ?
  order by date desc
""" 
 
# Shared bio columns for every player list on the Players tab. `p` is
# dw.mlbplayers; `ps` is a per-player team lookup from dim_player_stats
# (so each list can show which team the player was with).
_PLAYER_LIST_COLUMNS = """
        p.id,
        p.full_name,
        ps.team_name,
        p.birth_date,
        p.current_age,
        CONCAT(p.height, ' ', p.weight, ' lbs.')                                    AS [height/weight],
        p.primary_position__name                                                    AS position,
        p.mlb_debut_date                                                            AS debut,
        CONCAT('Bats ', p.bat_side__description, ' Throws ', p.pitch_hand__description) AS [Bats/Throws],
        CONCAT_WS(', ', p.birth_city, p.birth_state_province, p.birth_country)      AS birth_place
"""

GAME_PLAYER_DETAIL = f"""
    SELECT {_PLAYER_LIST_COLUMNS}
    FROM dw.mlbplayers p
    JOIN (
        SELECT DISTINCT player_id, team_id, team_name
        FROM silver.dim_player_stats
        WHERE game_pk = ?
    ) ps ON ps.player_id = p.id
    ORDER BY ps.team_name, p.full_name
"""

# Everyone who appeared for a team in one season. Params: (team_id, season).
# MAX(team_name) collapses the per-game rows to one per player.
TEAM_PLAYERS = f"""
    SELECT {_PLAYER_LIST_COLUMNS}
    FROM dw.mlbplayers p
    JOIN (
        SELECT player_id, MAX(team_name) AS team_name
        FROM silver.dim_player_stats
        WHERE team_id = ?
          AND season  = ?
        GROUP BY player_id
    ) ps ON ps.player_id = p.id
    ORDER BY p.full_name
"""

# Most recent season a team has player stats for (used when no game is
# selected, so a team's roster defaults to its latest season).
TEAM_LATEST_SEASON = """
    SELECT MAX(season) AS season
    FROM silver.dim_player_stats
    WHERE team_id = ?
"""  

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
        person_draft_year                                                       AS draft_year,
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
"""  

PLAYER_AWARDS = """
    SELECT
        a.award_name,
        ar.award_id,
        ar.player_id,
        ar.season,
        ar.award_date,
        ISNULL(t.abbreviation, 'Unknown') AS team
    FROM [dlt].[silver].[dim_award_recipient] ar
    JOIN [dlt].[silver].[dim_award] a ON a.award_id = ar.award_id
     LEFT OUTER JOIN [dlt].[silver].[dim_teams] t on t.team_id = ar.team_id and ar.season = t.season
    WHERE ar.player_id = ?
    order by  ar.season desc
"""  # matches the spec exactly (not a guess) — replaces the old
     # dim_award_recipient guess with the real fact/dim join

PLAYER_YTD_PITCHING = """
    SELECT
        COUNT(DISTINCT s.game_pk)                                            AS games_played,
        SUM(CASE WHEN f.winning_pitcher_id = s.player_id THEN 1 ELSE 0 END)  AS wins,
        SUM(CASE WHEN f.losing_pitcher_id  = s.player_id THEN 1 ELSE 0 END)  AS losses,
        SUM(CASE WHEN f.save_pitcher_id    = s.player_id THEN 1 ELSE 0 END)  AS saves,
        CASE WHEN SUM(s.pitching_outs_recorded) IS NULL THEN NULL
             ELSE CONCAT(SUM(s.pitching_outs_recorded) / 3, '.',
                         SUM(s.pitching_outs_recorded) % 3)
        END                                                                  AS innings_pitched,
        SUM(s.pitching_hits_allowed)                                         AS hits_allowed,
        SUM(s.pitching_runs_allowed)                                         AS runs_allowed,
        SUM(s.pitching_earned_runs)                                          AS earned_runs,
        SUM(s.pitching_walks)                                                AS walks,
        SUM(s.pitching_strikeouts)                                           AS strikeouts,
        SUM(s.pitching_home_runs_allowed)                                    AS home_runs_allowed,
        -- Rate stats recomputed from season totals (27 outs = 9 innings),
        -- never averaged from per-game values.
        CAST(27.0 * SUM(s.pitching_earned_runs)
             / NULLIF(SUM(s.pitching_outs_recorded), 0) AS DECIMAL(6,2))     AS era,
        CAST(3.0 * (SUM(s.pitching_hits_allowed) + SUM(s.pitching_walks))
             / NULLIF(SUM(s.pitching_outs_recorded), 0) AS DECIMAL(5,2))     AS whip,
        CAST(27.0 * SUM(s.pitching_strikeouts)
             / NULLIF(SUM(s.pitching_outs_recorded), 0) AS DECIMAL(5,2))     AS k_per9
    FROM silver.int_player_game_stats s
    JOIN silver.dim_games g        ON g.game_pk = s.game_pk
    JOIN silver.dim_games cur      ON cur.game_pk = ?          -- param 1: game_pk
    LEFT JOIN silver.fact_games f  ON f.game_pk = s.game_pk    -- W / L / SV decisions
    WHERE s.player_id = ?                                      -- param 2: player_id
      AND g.season        = cur.season
      AND g.game_type     = cur.game_type
      AND g.game_datetime <= cur.game_datetime
      -- only games he actually pitched in (a two-way player's batting-only
      -- games would otherwise count as pitching appearances)
      AND (   s.pitching_outs_recorded     > 0
           OR s.pitching_hits_allowed      > 0
           OR s.pitching_runs_allowed      > 0
           OR s.pitching_earned_runs       > 0
           OR s.pitching_walks             > 0
           OR s.pitching_strikeouts        > 0
           OR s.pitching_home_runs_allowed > 0)
"""  

PLAYER_YTD_BATTING = """
   select COUNT(DISTINCT s.game_pk)   AS games_played,
        SUM(s.batting_at_bats)      AS at_bats,
        SUM(s.batting_runs)         AS runs,
        SUM(s.batting_hits)         AS hits,
        SUM(s.batting_doubles)      AS doubles,
        SUM(s.batting_triples)      AS triples,
        SUM(s.batting_home_runs)    AS home_runs,
		FORMAT(1.0 * SUM(s.batting_hits) / NULLIF(SUM(s.batting_at_bats), 0),
               '#.000', 'en-US')                AS avg
    FROM silver.int_player_game_stats s
    JOIN silver.dim_games g   ON g.game_pk = s.game_pk
     JOIN silver.dim_games cur ON cur.game_pk = ?
    WHERE s.player_id = ?
      AND g.season    = cur.season
      AND g.game_type = cur.game_type
      AND g.game_datetime <= cur.game_datetime   -- datetime, not date: handles doubleheaders
"""  # matches the spec exactly

# ---------------------------------------------------------------------------
# Teams
# ---------------------------------------------------------------------------

TEAM_DETAIL = """
    SELECT *
    FROM silver.dim_teams
    WHERE team_id = ?
"""

TEAM_VENUE = """
    SELECT v.venue_name,v.location_name,v.first_year_of_play
    FROM silver.dim_teams t
    JOIN silver.dim_venue v ON v.venue_id = t.venue_id
    WHERE t.team_id = ?
"""  # [ASSUMED] dim_team has a venue_id FK

TEAM_DRAFT = """
    SELECT *
    FROM silver.dim_draft
    WHERE team_id = ?
      ORDER BY person_draft_year DESC, round_pick_number, pick_number
"""

# ---------------------------------------------------------------------------
# Standalone menu list pages
# ---------------------------------------------------------------------------

ALL_AWARDS = """
    SELECT
       a.award_name,
       ar.award_id,
       ar.player_id,
       ar.season,
       ar.award_date,
       ISNULL(t.abbreviation, 'Unknown') AS team
   FROM [dlt].[silver].[dim_award_recipient] ar
   JOIN [dlt].[silver].[dim_award] a ON a.award_id = ar.award_id
    LEFT OUTER JOIN [dlt].[silver].[dim_teams] t on t.team_id = ar.team_id and ar.season = t.season
    ORDER BY season DESC, ar.award_date desc,award_name
"""

ALL_DRAFT_PICKS = """
    SELECT *
    FROM silver.dim_draft
    ORDER BY person_draft_year DESC, round_pick_number, pick_number
"""

ALL_UMPIRES = """
    SELECT DISTINCT official_id, full_name
    FROM silver.dim_game_umpires
    ORDER BY full_name
"""

# ---------------------------------------------------------------------------
# Team Records / Splits (for game detail panel)
# ---------------------------------------------------------------------------

GAME_TEAM_RECORDS = """
    SELECT
        r.team_id, r.team_name, r.is_home, r.opponent_team_name, r.result,
        r.venue_id, r.double_header, r.day_night, r.start_hour, r.is_interleague,
        r.games_in_series, r.series_game_number,
        r.wins_to_date_venue,            r.losses_to_date_venue,
        r.wins_to_date_opponent,         r.losses_to_date_opponent,
        r.wins_to_date_doubleheader_n,   r.losses_to_date_doubleheader_n,
        r.wins_to_date_doubleheader_y,   r.losses_to_date_doubleheader_y,
        r.wins_to_date_doubleheader_s,   r.losses_to_date_doubleheader_s,
        r.wins_to_date_day_night_day,    r.losses_to_date_day_night_day,
        r.wins_to_date_day_night_night,  r.losses_to_date_day_night_night,
        r.wins_to_date_start_hour,       r.losses_to_date_start_hour,
        r.wins_to_date_interleague,      r.losses_to_date_interleague,
        r.wins_to_date_series,           r.losses_to_date_series,
        r.wins_to_date_games_in_series,  r.losses_to_date_games_in_series,
        r.wins_to_date_series_game_number, r.losses_to_date_series_game_number
    FROM silver.int_team_game_results_to_date r
    WHERE r.game_pk = ?
    ORDER BY r.is_home DESC
"""

# ---------------------------------------------------------------------------
# Matchups & Pitcher — NOT SPECIFIED
# ---------------------------------------------------------------------------
# The original request didn't describe what these two main-menu items
# should show. Stubbed as empty placeholders below (see main.py) so the
# nav is complete and the app runs; tell me what each should display and
# I'll fill these in for real.