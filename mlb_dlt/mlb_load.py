import dlt
import requests
import pyodbc
from datetime import datetime, timedelta
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ----------------------------
# Destination DB connection
# ----------------------------
DW_CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=10.0.0.54;"
    "DATABASE=dlt;"
    "username = hlabs;"
    "password = 1234pass;"
    "Trusted_Connection=yes;"
    "trust_server_certificate = true;"
)
BASE_URL = "https://statsapi.mlb.com/api/v1/"


def build_session():
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = build_session()


# ----------------------------
# DB helpers
# ----------------------------
def get_max_transaction_date():
    try:
        conn = pyodbc.connect(DW_CONNECTION_STRING, timeout=10)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT MAX(CAST(date AS DATE))
            FROM dw.player_transactions
        """)
        row = cursor.fetchone()
        conn.close()
        if row and row[0]:
            return row[0]
        return None
    except pyodbc.Error as e:
        print(f"Could not query max transaction date: {e}")
        print("   Falling back to full load.")
        return None


def get_max_game_date():
    try:
        conn = pyodbc.connect(DW_CONNECTION_STRING, timeout=10)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT MAX(CAST(game_date AS DATETIME))
            FROM dw.games
        """)
        row = cursor.fetchone()
        conn.close()
        if row and row[0]:
            return row[0]
        return None
    except pyodbc.Error as e:
        print(f"Could not query max game_date: {e}")
        print("   Falling back to full load.")
        return None


def get_existing_game_pks():
    try:
        conn = pyodbc.connect(DW_CONNECTION_STRING, timeout=10)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT distinct game_pk
            FROM dw.play_events
        """)
        rows = cursor.fetchall()
        conn.close()
        return set(r[0] for r in rows) if rows else set()
    except pyodbc.Error as e:
        print(f"Could not query existing game_pks: {e}")
        print("   Falling back to full load.")
        return set()


# ----------------------------
# HTTP helper
# ----------------------------
def get_json(url, params=None):
    resp = SESSION.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


# ----------------------------
# Transactions
# ----------------------------
@dlt.resource(
    name="player_transactions",
    write_disposition="append",primary_key=("transaction_id")
)
def transactions_resource(start_year: int = None, end_year: int = None):
    user_provided_range = start_year is not None or end_year is not None
    current_year = datetime.now().year
    max_date = get_max_transaction_date()

    if start_year is None:
        start_year = max_date.year if max_date else 1960
    if end_year is None:
        end_year = min(current_year, 2026)

    if max_date and not user_provided_range:
        incremental_start = (max_date + timedelta(days=1)).strftime("%m/%d/%Y")
        print(f"Incremental transactions load from {incremental_start}")
    else:
        incremental_start = None
        print(f"Full transactions load: {start_year}–{end_year}")

    for year in range(start_year, end_year + 1):
        if incremental_start and year < max_date.year:
            continue

        sd = incremental_start if (incremental_start and year == max_date.year) \
            else f"01/01/{year}"
        ed = f"12/31/{year}"

        try:
            data = get_json(
                f"{BASE_URL}/transactions",
                {"startDate": sd, "endDate": ed}
            )
        except Exception as e:
            print(f"Transaction fetch failed for {year}: {e}")
            continue

        for txn in data.get("transactions", []):
            # `or {}` guards against explicit None from the API
            player    = txn.get("person") or {}
            from_team = txn.get("fromTeam") or {}
            to_team   = txn.get("toTeam") or {}

            player_id   = player.get("id")
            player_name = player.get("fullName")
            description = txn.get("description") or ""
            txn_category = "player" if player_id else "team"

            # Warn on genuinely sparse records (no player AND no description)
            if not player_id and not description:
                print(
                    f"Sparse txn: id={txn.get('id')} "
                    f"type={txn.get('typeCode')} date={txn.get('date')}"
                )

            # BUG FIX: yield is outside the if block — all records are yielded
            yield {
                "transaction_id":       txn.get("id"),
                "date":                 txn.get("date"),
                "effective_date":       txn.get("effectiveDate"),
                "resolution_date":      txn.get("resolutionDate"),
                "type_code":            txn.get("typeCode"),
                "type_desc":            txn.get("typeDesc"),
                "description":          description,
                "transaction_category": txn_category,
                "has_player":           player_id is not None,
                "player_id":            player_id,
                "player_name":          player_name,
                "from_team_id":         from_team.get("id"),
                "from_team_name":       from_team.get("name"),
                "to_team_id":           to_team.get("id"),
                "to_team_name":         to_team.get("name"),
            }


# ----------------------------
# Seasons
# ----------------------------
@dlt.resource(name="seasons", write_disposition="replace")
def seasons_resource():
    for year in range(1990, 2026):
        data = get_json(f"{BASE_URL}/seasons/{year}", {"sportId": 1})
        seasons = data.get("seasons")
        yield seasons


# ----------------------------
# Teams
# ----------------------------
@dlt.resource(name="teams", write_disposition="merge", primary_key=["id"])
def teams_resource(start_year: int = None, end_year: int = None):
    current_year = datetime.now().year
    start_year = start_year or 1960
    end_year   = end_year   or current_year

    seen_ids = set()
    for year in range(start_year, end_year + 1):
        data = get_json(f"{BASE_URL}/teams", {"sportId": 1, "season": year})
        for team in data["teams"]:
            team_id = team.get("id")
            if team_id and team_id not in seen_ids:
                seen_ids.add(team_id)
                yield team


# ----------------------------
# Draft Picks
# ----------------------------
@dlt.resource(name="draft", write_disposition="replace")
def draft_resource():
    try:
        for year in range(1960, 2026):
            data = get_json(f"{BASE_URL}/draft/{year}")
            drafts = data.get("drafts")
            if not drafts:
                continue
            rounds = drafts.get("rounds")
            if not isinstance(rounds, list):
                continue
            for round_obj in rounds:
                picks = round_obj.get("picks", [])
                for pick in picks:
                    yield pick
    except Exception as ex:
        print('(draft_resource)Error {}'.format(ex.args))


# ----------------------------
# Play-by-play fetch helper
# ----------------------------
def fetch_game_pbp(year, game_pk):
    try:
        pbp = get_json(f"{BASE_URL}/game/{game_pk}/playByPlay")
        events = []

        for play in pbp.get("allPlays", []):
            at_bat_index = play.get("atBatIndex")
            matchup      = play.get("matchup", {})
            batter       = matchup.get("batter", {})
            pitcher      = matchup.get("pitcher", {})
            batter_hand  = matchup.get("batSide", {})
            pitcher_hand = matchup.get("pitchHand", {})

            for event in play.get("playEvents", []):
                if not event.get("isPitch"):
                    continue

                details    = event.get("details", {})
                is_in_play = details.get("isInPlay", False)

                event["season"]       = year
                event["gamePk"]       = game_pk
                event["atBatIndex"]   = at_bat_index
                event["isInPlay"]     = is_in_play
                event["batter_id"]    = batter.get("id")
                event["batter_name"]  = batter.get("fullName")
                event["pitcher_id"]   = pitcher.get("id")
                event["pitcher_name"] = pitcher.get("fullName")
                event["batter_side"]  = batter_hand.get("code")
                event["pitcher_hand"] = pitcher_hand.get("code")

                events.append(event)

        return events
    except Exception:
        return []


# ----------------------------
# Players (40-man rosters)
# ----------------------------
@dlt.resource(name="mlbplayers", write_disposition="replace")
def players_resource():
    seen_ids = set()
    for year in range(1960, 2026):
        play = get_json(
            f"{BASE_URL}/sports/1/players",
            {"season": year}
        )
        for player in play.get("people", []):
            playid = player.get("id")
            this_player = get_json(f"{BASE_URL}/people/{playid}")
            for play in this_player.get("people", []):
                yield play


# ----------------------------
# Award Recipients
# ----------------------------
@dlt.resource(
    name="award_recipients",
    write_disposition="merge",
    primary_key=("award_id", "season", "player_id")
)
def award_recipients_resource():
    awards_data = get_json(f"{BASE_URL}/awards")
    awards = awards_data.get("awards", [])

    for award_meta in awards:
        award_id = award_meta.get("id")
        if not award_id or award_id.upper().startswith("RETIRE"):
            continue

        for year in range(2023, 2024):
            try:
                data = get_json(
                    f"{BASE_URL}/awards/{award_id}/recipients",
                    {"season": year}
                )
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 404:
                    continue
                raise
            except requests.RequestException:
                continue

            for award in data.get("awards", []):
                player   = award.get("player", {})
                position = player.get("primaryPosition", {})
                team     = award.get("team", {})

                yield {
                    "award_id":              award.get("id"),
                    "award_name":            award.get("name"),
                    "award_date":            award.get("date"),
                    "season":                award.get("season") or year,
                    "team_id":               team.get("id"),
                    "player_id":             player.get("id"),
                    "player_name":           player.get("nameFirstLast"),
                    "position_code":         position.get("code"),
                    "position_name":         position.get("name"),
                    "position_type":         position.get("type"),
                    "position_abbreviation": position.get("abbreviation"),
                    "notes":                 award.get("notes"),
                }


# ----------------------------
# Rosters
# ----------------------------
@dlt.resource(name="rosters", write_disposition="append", primary_key=("person__id", "season", "status__code"))
def rosters_resource(start_year: int = None, end_year: int = None):
    current_year = datetime.now().year

    if start_year is None:
        start_year = current_year
    if end_year is None:
        end_year = current_year

    teams = get_json(f"{BASE_URL}/teams", {"sportId": 1})["teams"]
    print('teams found:' + str(len(teams)))

    for year in range(start_year, end_year + 1):
        for team in teams:
            roster = get_json(
                f"{BASE_URL}/teams/{team['id']}/roster/40Man",
                {"season": year}
            )
            for player in roster.get("roster", []):
                player["season"] = year
                player["parent_team_id"] = team["id"]
                yield player


# ----------------------------
# Games
# ----------------------------
@dlt.resource(name="games", write_disposition="merge", primary_key=["game_pk"])
def games_resource(start_year: int = None, end_year: int = None):
    user_provided_range = start_year is not None or end_year is not None
    current_year = datetime.now().year
    max_date = get_max_game_date()
    seen_pks = set()

    if start_year is None:
        start_year = max_date.year if max_date else 1960
    if end_year is None:
        end_year = current_year

    if max_date and not user_provided_range:
        start_date_str = max_date.strftime("%Y-%m-%d")
        print(f"Incremental games load: fetching from {start_date_str}")
    else:
        start_date_str = None
        print(f"Full games load: {start_year} to {end_year}")

    for year in range(start_year, end_year + 1):
        params = {"sportId": 1, "season": year}
        if start_date_str and year == start_year:
            params["startDate"] = start_date_str
            params["endDate"]   = f"{year}-12-31"

        schedule = get_json(f"{BASE_URL}/schedule", params)

        for date in schedule.get("dates", []):
            for game in date.get("games", []):
                game_pk = game.get("gamePk")
                if game_pk in seen_pks:
                    continue
                seen_pks.add(game_pk)
                game_type = game.get("gameType", "")
                if game_type in ("E", "A"):
                    continue
                detailed_state = game.get("status", {}).get("detailedState", "")
                if detailed_state in ('Cancelled', 'Postponed'):
                    continue
                status = game.get("status", {}).get("abstractGameState", "")
                if status == "Final":
                    yield game


def get_games_missing_umpires(start_year: int = 2023):
    conn = pyodbc.connect(DW_CONNECTION_STRING, timeout=10)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT game_pk
        FROM dw.games
        WHERE season >= ?
          AND game_pk NOT IN (SELECT game_pk FROM dw.umpires)
          AND game_type = 'R'   -- Regular season only
          AND status__detailed_state NOT IN ('Cancelled','Postponed')
    """, (start_year,))
    rows = cursor.fetchall()
    conn.close()
    return [r[0] for r in rows]


@dlt.resource(
    name="umpires",
    write_disposition="merge",   # changed from append
    primary_key=["game_pk", "official_type"]
)
def umpires_resource(start_year: int = None, end_year: int = None):
    effective_start = start_year if start_year is not None else 2023
    missing_pks = get_games_missing_umpires(effective_start)
    print(f"Fetching umpires for {len(missing_pks)} games missing records...")

    no_officials = []
    loaded = 0

    for game_pk in missing_pks:
        try:
            boxscore = get_json(f"{BASE_URL}/game/{game_pk}/boxscore")
        except Exception as e:
            print(f"Failed to fetch boxscore for game {game_pk}: {e}")
            continue

        officials = boxscore.get("officials", [])
        if not officials:
            no_officials.append(game_pk)
            continue

        for umpire in officials:
            official = umpire.get("official", {})
            loaded += 1
            yield {
                "game_pk":       game_pk,
                "official_type": umpire.get("officialType"),
                "official_id":   official.get("id"),
                "full_name":     official.get("fullName"),
            }

    print(f"Yielded {loaded} umpire records.")
    print(f"{len(no_officials)} games had no officials in API response: {no_officials}")


# ----------------------------
# Game details helper
# ----------------------------
def fetch_game_details(game):
    game_pk = game.get("gamePk")
    result = {"game_pk": game_pk}

    # ── 1. Live feed (v1.1) ─────────────────────────────────────────
    try:
        feed       = get_json(f"{BASE_URL.replace('/v1/', '/v1.1/')}/game/{game_pk}/feed/live")
        game_data  = feed.get("gameData", {})
        live_data  = feed.get("liveData", {})
        weather    = game_data.get("weather", {})
        game_info  = game_data.get("gameInfo", {})
        decisions  = live_data.get("decisions", {})
        winner     = decisions.get("winner", {})
        loser      = decisions.get("loser", {})
        save       = decisions.get("save", {})
        boxscore   = live_data.get("boxscore", {})
        teams_box  = boxscore.get("teams", {})

        home_stats    = teams_box.get("home", {}).get("teamStats", {})
        away_stats    = teams_box.get("away", {}).get("teamStats", {})
        home_batting  = home_stats.get("batting", {})
        away_batting  = away_stats.get("batting", {})
        home_pitching = home_stats.get("pitching", {})
        away_pitching = away_stats.get("pitching", {})

        result.update({
            "weather_condition":               weather.get("condition"),
            "weather_temp":                    weather.get("temp"),
            "weather_wind":                    weather.get("wind"),
            "attendance":                      game_info.get("attendance"),
            "first_pitch":                     game_info.get("firstPitch"),
            "game_duration_minutes":           game_info.get("gameDurationMinutes"),
            "winning_pitcher_id":              winner.get("id"),
            "winning_pitcher_name":            winner.get("fullName"),
            "losing_pitcher_id":               loser.get("id"),
            "losing_pitcher_name":             loser.get("fullName"),
            "save_pitcher_id":                 save.get("id"),
            "save_pitcher_name":               save.get("fullName"),
            "home_runs":                       home_batting.get("runs"),
            "home_hits":                       home_batting.get("hits"),
            "home_doubles":                    home_batting.get("doubles"),
            "home_triples":                    home_batting.get("triples"),
            "home_home_runs":                  home_batting.get("homeRuns"),
            "home_rbi":                        home_batting.get("rbi"),
            "home_stolen_bases":               home_batting.get("stolenBases"),
            "home_strikeouts":                 home_batting.get("strikeOuts"),
            "home_walks":                      home_batting.get("baseOnBalls"),
            "home_left_on_base":               home_batting.get("leftOnBase"),
            "away_runs":                       away_batting.get("runs"),
            "away_hits":                       away_batting.get("hits"),
            "away_doubles":                    away_batting.get("doubles"),
            "away_triples":                    away_batting.get("triples"),
            "away_home_runs":                  away_batting.get("homeRuns"),
            "away_rbi":                        away_batting.get("rbi"),
            "away_stolen_bases":               away_batting.get("stolenBases"),
            "away_strikeouts":                 away_batting.get("strikeOuts"),
            "away_walks":                      away_batting.get("baseOnBalls"),
            "away_left_on_base":               away_batting.get("leftOnBase"),
            "home_pitching_strikeouts":        home_pitching.get("strikeOuts"),
            "home_pitching_walks":             home_pitching.get("baseOnBalls"),
            "home_pitching_hits_allowed":      home_pitching.get("hits"),
            "home_pitching_runs_allowed":      home_pitching.get("runs"),
            "home_pitching_earned_runs":       home_pitching.get("earnedRuns"),
            "home_pitching_home_runs_allowed": home_pitching.get("homeRuns"),
            "home_errors":                     home_batting.get("errors") or home_pitching.get("errors"),
            "away_pitching_strikeouts":        away_pitching.get("strikeOuts"),
            "away_pitching_walks":             away_pitching.get("baseOnBalls"),
            "away_pitching_hits_allowed":      away_pitching.get("hits"),
            "away_pitching_runs_allowed":      away_pitching.get("runs"),
            "away_pitching_earned_runs":       away_pitching.get("earnedRuns"),
            "away_pitching_home_runs_allowed": away_pitching.get("homeRuns"),
            "away_errors":                     away_batting.get("errors") or away_pitching.get("errors"),
        })
    except Exception as e:
        print(f"Live feed failed for {game_pk}: {e}")
        return None

    # ── 2. Boxscore endpoint — fills attendance/first_pitch gaps ────
    # The /boxscore endpoint sometimes has attendance when live feed doesn't
    if not result.get("attendance") or not result.get("first_pitch"):
        try:
            bs = get_json(f"{BASE_URL}/game/{game_pk}/boxscore")
            info = bs.get("info", [])
            # info is a list of {label, value} dicts
            info_map = {
                item.get("label", "").strip().lower(): item.get("value", "").strip()
                for item in info
                if isinstance(item, dict) and item.get("label") and item.get("value")
            }
            if not result.get("attendance"):
                att_raw = info_map.get("att") or info_map.get("attendance")
                if att_raw:
                    # Strip commas: "44,392" → 44392
                    result["attendance"] = int(att_raw.replace(",", "").strip()) \
                        if att_raw.replace(",", "").strip().isdigit() else None
            if not result.get("first_pitch"):
                result["first_pitch"] = info_map.get("first pitch") or info_map.get("t")
            if not result.get("game_duration_minutes"):
                duration_raw = info_map.get("t")   # "3:22" format
                if duration_raw and ":" in duration_raw:
                    h, m = duration_raw.split(":")
                    result["game_duration_minutes"] = int(h) * 60 + int(m)
        except Exception as e:
            print(f"Boxscore fallback failed for {game_pk}: {e}")

    # ── 3. Linescore — fills decisions for edge cases ────────────────
    if not result.get("winning_pitcher_id"):
        try:
            ls = get_json(f"{BASE_URL}/game/{game_pk}/linescore")
            decisions = ls.get("decisions", {})
            if decisions:
                w = decisions.get("winner", {})
                l = decisions.get("loser", {})
                s = decisions.get("save", {})
                result["winning_pitcher_id"]   = result.get("winning_pitcher_id")   or w.get("id")
                result["winning_pitcher_name"] = result.get("winning_pitcher_name") or w.get("fullName")
                result["losing_pitcher_id"]    = result.get("losing_pitcher_id")    or l.get("id")
                result["losing_pitcher_name"]  = result.get("losing_pitcher_name")  or l.get("fullName")
                result["save_pitcher_id"]      = result.get("save_pitcher_id")      or s.get("id")
                result["save_pitcher_name"]    = result.get("save_pitcher_name")    or s.get("fullName")
        except Exception as e:
            print(f"Linescore fallback failed for {game_pk}: {e}")

    return result
	

def get_game_pks_from_dw(start_year, end_year):
    current_year = datetime.now().year
    # Default to full range if no years provided
    start_year = start_year or 1960
    end_year   = end_year   or current_year

    conn = pyodbc.connect(DW_CONNECTION_STRING, timeout=10)
    cursor = conn.cursor()
    # Only fetch game_pks that exist in dw.games (enforces referential integrity
    # at load time — prevents game_details orphans from the start).
    # games have to be Final and Not game_type E,A
    cursor.execute("""
    SELECT game_pk FROM dw.games
    WHERE season BETWEEN ? AND ?
      AND status__abstract_game_state = 'Final'
      AND game_type NOT IN ('E', 'A')
""", (start_year, end_year))
    rows = cursor.fetchall()
    conn.close()
    print(f"[game_details] Found {len(rows)} eligible games in dw.games "
          f"for seasons {start_year}–{end_year}")
    return [{"gamePk": r[0]} for r in rows]
	

def get_all_game_pks():
    conn = pyodbc.connect(DW_CONNECTION_STRING, timeout=10)
    cursor = conn.cursor()
    # Only fetch game_pks that exist in dw.games (enforces referential integrity
    # at load time — prevents game_details orphans from the start).
    # games have to be Final and Not game_type E,A
    cursor.execute("""
    SELECT game_pk FROM dw.games
""")
    rows = cursor.fetchall()
    conn.close()
    return [{"gamePk": r[0]} for r in rows]
	

# ----------------------------
# Game details resource
# ----------------------------
@dlt.resource(name="game_details", write_disposition="merge", primary_key=["game_pk"])
def game_details_resource(start_year: int = None, end_year: int = None):
    # ── Orphan cleanup ───────────────────────────────────────────────
    # Remove any game_details rows whose game_pk no longer exists in
    # dw.games. These arise when a prior pipeline run loaded details
    # for a game that was later removed/rolled back from dw.games.
    try:
        conn = pyodbc.connect(DW_CONNECTION_STRING, timeout=10)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*)
            FROM dw.game_details gd
            WHERE NOT EXISTS (
                SELECT 1 FROM dw.games g WHERE g.game_pk = gd.game_pk
            )
        """)
        orphan_count = cursor.fetchone()[0]
        if orphan_count:
            print(f"[game_details] Removing {orphan_count} orphaned rows "
                  f"(game_pk not in dw.games)...")
            cursor.execute("""
                DELETE FROM dw.game_details
                WHERE game_pk NOT IN (SELECT game_pk FROM dw.games)
            """)
            conn.commit()
            print(f"[game_details] Orphan cleanup complete.")
        else:
            print("[game_details] No orphaned rows found.")
        conn.close()
    except Exception as e:
        print(f"[game_details] Orphan cleanup failed (non-fatal): {e}")
    # ────────────────────────────────────────────────────────────────

    games = get_game_pks_from_dw(start_year, end_year)
    print(f"Fetching details for {len(games)} games...")

    BATCH_SIZE = 200
    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = {
            executor.submit(fetch_game_details, game): game.get("gamePk")
            for game in games
        }
        batch = []
        done  = 0
        total = len(futures)

        for future in as_completed(futures):
            done += 1
            try:
                result = future.result()
                if result:
                    batch.append(result)
            except Exception as e:
                print(f"Detail fetch failed for {futures[future]}: {e}")

            if len(batch) >= BATCH_SIZE:
                yield batch
                batch = []

            if done % 500 == 0:
                print(f"Progress: {done}/{total}")

        if batch:
            yield batch


# ----------------------------
# Stats helpers
# ----------------------------
def get_all_stats(year: int, group: str) -> list:
    all_splits = []
    limit  = 500
    offset = 0

    while True:
        data = get_json(
            f"{BASE_URL}/stats",
            {
                "stats":      "season",
                "group":      group,
                "season":     year,
                "sportIds":   1,
                "playerPool": "All",
                "limit":      limit,
                "offset":     offset,
            }
        )
        stat_groups = data.get("stats", [])
        if not stat_groups:
            break
        splits = stat_groups[0].get("splits", [])
        if not splits:
            break
        all_splits.extend(splits)
        total  = stat_groups[0].get("totalSplits", 0)
        offset += limit
        if offset >= total:
            break

    return all_splits


@dlt.resource(
    name="player_stats",
    write_disposition="append",
    primary_key=["season", "player__id", "group", "position__code"]
)
def stats_resource(start_year: int = None, end_year: int = None):
    current_year = datetime.now().year
    start_year = start_year or current_year
    end_year   = end_year   or current_year

    for year in range(start_year, end_year + 1):
        for group in ["hitting", "pitching", "fielding", "running"]:
            for split in get_all_stats(year, group):
                split["season"] = year
                split["group"]  = group
                yield split


# ----------------------------
# Play Events (incremental)
# ----------------------------
@dlt.resource(name="play_events", write_disposition="append")
def play_events_resource(start_year: int = None, end_year: int = None):
    user_provided_range = start_year is not None or end_year is not None
    current_year = datetime.now().year
    max_date = get_max_game_date()

    if start_year is None:
        start_year = max_date.year if max_date else 2020
    if end_year is None:
        end_year = current_year

    if max_date and not user_provided_range:
        start_date_str = max_date.strftime("%Y-%m-%d")
        print(f"Incremental load from {start_date_str}")
    else:
        start_date_str = None
        print(f"Full/backfill load from {start_year} to {end_year}")

    all_games = []
    schedule_params = []
    for year in range(start_year, end_year + 1):
        params = {"sportId": 1, "season": year}
        if start_date_str and year == start_year:
            params["startDate"] = start_date_str
            params["endDate"]   = f"{year}-12-31"
        schedule_params.append((year, params))

    with ThreadPoolExecutor(max_workers=10) as executor:
        schedule_futures = {
            executor.submit(get_json, f"{BASE_URL}/schedule", params): year
            for year, params in schedule_params
        }
        for future in as_completed(schedule_futures):
            year = schedule_futures[future]
            try:
                schedule = future.result()
            except Exception as e:
                print(f"Schedule fetch failed for {year}: {e}")
                continue
            for date_entry in schedule.get("dates", []):
                for game in date_entry.get("games", []):
                    game_pk = game.get("gamePk")
                    status  = game.get("status", {}).get("abstractGameState")
                    game_type = game.get("gameType", "")
                    if game_type in ("E", "A"):
                        continue
                    if game_pk and status == "Final":
                        all_games.append((year, game_pk))

    existing_pks = get_existing_game_pks()
    before = len(all_games)
    all_games = [(y, pk) for y, pk in all_games if pk not in existing_pks]
    print(f"{before} total games, {before - len(all_games)} already loaded, "
          f"fetching {len(all_games)} remaining...")

    if not all_games:
        print("Nothing new to load.")
        return

    BATCH_SIZE = 200
    with ThreadPoolExecutor(max_workers=75) as executor:
        futures = {
            executor.submit(fetch_game_pbp, year, game_pk): game_pk
            for year, game_pk in all_games
        }
        batch = []
        done  = 0
        total = len(futures)

        for future in as_completed(futures):
            done += 1
            try:
                events = future.result()
                batch.extend(events)
            except Exception as e:
                game_pk = futures[future]
                print(f"PBP failed for game {game_pk}: {e}")
                continue

            if len(batch) >= BATCH_SIZE:
                yield batch
                batch = []

            if done % 500 == 0:
                print(f"Progress: {done}/{total} games processed")

        if batch:
            yield batch


# ----------------------------
# Pipeline entry point
# ----------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("resource", help="Resource to run (e.g. play_events, games, seasons)")
    parser.add_argument("--start-year", type=int, default=None)
    parser.add_argument("--end-year",   type=int, default=None)
    args = parser.parse_args()

    pipeline = dlt.pipeline(
        pipeline_name="mlb_full_stats",
        destination="mssql",
        dataset_name="dw"
    )
    
    resources = {
        "play_events":         lambda: play_events_resource(args.start_year, args.end_year),
        "games":               lambda: games_resource(args.start_year, args.end_year),
        "seasons":             seasons_resource,
        "teams":               lambda: teams_resource(args.start_year, args.end_year),
        "draft":               draft_resource,
        "players":             players_resource,
        "player_stats":        lambda: stats_resource(args.start_year, args.end_year),
        "rosters":             lambda: rosters_resource(args.start_year, args.end_year),
        "awards":              award_recipients_resource,
        "player_transactions": lambda: transactions_resource(args.start_year, args.end_year),
        "umpires":             lambda: umpires_resource(args.start_year, args.end_year),
        "game_details":        lambda: game_details_resource(args.start_year, args.end_year),
    }

    if args.resource not in resources:
        print(f"Unknown resource '{args.resource}'. Choose from: {', '.join(resources)}")
    else:
        load_info = pipeline.run([resources[args.resource]()])
        print("Load complete")
        print(load_info)