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
    "SERVER=KEITH-PERSONAL;"
    "DATABASE=dlt;"
    "username = sa;"
    "password = pass0123;"
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


def get_valid_game_pks_from_dw() -> set:
    """
    Single source of truth for valid game_pks.
    Always queries dw.games so every downstream resource
    (play_events, umpires, game_details) is guaranteed to reference
    only game_pks that exist in the parent table — ensuring dbt
    relationship tests never fail due to orphaned foreign keys.
    Returns a set of integers.
    """
    try:
        conn = pyodbc.connect(DW_CONNECTION_STRING, timeout=10)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT game_pk
            FROM dw.games
            WHERE status__abstract_game_state = 'Final'
              AND game_type NOT IN ('E', 'A')
        """)
        rows = cursor.fetchall()
        conn.close()
        result = set(r[0] for r in rows) if rows else set()
        print(f"[dw.games] {len(result)} valid game_pks available.")
        return result
    except pyodbc.Error as e:
        print(f"Could not query valid game_pks from dw.games: {e}")
        print("   Falling back to empty set — nothing will be loaded.")
        return set()


def get_loaded_pitch_game_pks() -> set:
    """
    Games that already have play_events (pitch-level) data loaded.
    The previous version of get_valid_game_pks_from_dw()'s docstring claimed
    this filtering already happened — it didn't; that function only checks
    dw.games for schedule/status validity, never whether pitches for that
    game were already loaded. This is the actual "skip already-loaded
    games" check. Now that play_events uses write_disposition="merge"
    (see play_events_resource), re-processing an already-loaded game is
    no longer incorrect — just wasted API calls — so this is an
    efficiency filter, not a correctness one.
    """
    try:
        conn = pyodbc.connect(DW_CONNECTION_STRING, timeout=10)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT game_pk FROM dw.play_events")
        rows = cursor.fetchall()
        conn.close()
        result = set(r[0] for r in rows) if rows else set()
        print(f"[dw.play_events] {len(result)} games already have pitch data loaded.")
        return result
    except pyodbc.Error as e:
        print(f"Could not query loaded game_pks from dw.play_events: {e}")
        print("   Falling back to empty set — no games will be skipped.")
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
# ----------------------------
# Transactions
# ----------------------------
@dlt.resource(
    name="player_transactions",
    write_disposition="merge", primary_key=("transaction_id")
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

    # DEDUP FIX: the MLB Stats API can return the same transaction in more
    # than one yearly window (its date, effectiveDate, and resolutionDate
    # can straddle a year boundary), so the same transaction_id can come
    # back from two different `year` iterations of this loop within a
    # single run. dlt's merge write disposition is expected to dedupe by
    # primary_key, but rows sharing a load id have been observed landing
    # in dw.player_transactions as duplicates — so we dedupe explicitly
    # here rather than relying on that behavior.
    seen_ids = set()

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
            txn_id = txn.get("id")

            # Skip transactions we've already yielded this run (see DEDUP FIX above)
            if txn_id in seen_ids:
                continue
            seen_ids.add(txn_id)

            # `or {}` guards against explicit None from the API
            player = txn.get("person") or {}
            from_team = txn.get("fromTeam") or {}
            to_team = txn.get("toTeam") or {}
            player_id = player.get("id")
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
                "transaction_id": txn_id,
                "date": txn.get("date"),
                "effective_date": txn.get("effectiveDate"),
                "resolution_date": txn.get("resolutionDate"),
                "type_code": txn.get("typeCode"),
                "type_desc": txn.get("typeDesc"),
                "description": description,
                "transaction_category": txn_category,
                "has_player": player_id is not None,
                "player_id": player_id,
                "player_name": player_name,
                "from_team_id": from_team.get("id"),
                "from_team_name": from_team.get("name"),
                "to_team_id": to_team.get("id"),
                "to_team_name": to_team.get("name"),
            }


def get_all_player_transactions() -> list:
    """
    Reads all player-attributed transactions back from dw.player_transactions.
    Source of truth for player_season_transactions_resource below, which
    reshapes/enriches already-loaded transaction data (adding a derived
    `season`) rather than re-fetching from the API and duplicating
    transactions_resource's fetch logic.
    """
    try:
        conn = pyodbc.connect(DW_CONNECTION_STRING, timeout=10)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                transaction_id,
                player_id,
                player_name,
                from_team_id,
                from_team_name,
                to_team_id,
                to_team_name,
                type_code,
                type_desc,
                description,
                date,
                effective_date,
                resolution_date
            FROM dw.player_transactions
            WHERE player_id IS NOT NULL
        """)
        columns = [c[0] for c in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        result = [dict(zip(columns, row)) for row in rows]
        print(f"[dw.player_transactions] {len(result)} player-attributed transactions available.")
        return result
    except pyodbc.Error as e:
        print(f"Could not query dw.player_transactions: {e}")
        print("   Falling back to empty set — nothing will be loaded.")
        return []


# ----------------------------
# Player-Season Transactions (one row per player+season+transaction)
# ----------------------------
@dlt.resource(
    name="player_season_transactions",
    write_disposition="merge",
    primary_key=("player_id", "season", "transaction_id"),
)
def player_season_transactions_resource():
    """
    One row per (player, season, transaction) -- every player-attributed
    transaction event, fully expanded (no grouping/aggregation/collapsing).
    Each transaction already belongs to exactly one player and one date,
    so this is transactions_resource's data with a derived `season`
    attached, filtered to player-only transactions.

    NOTE on `season`: derived as the calendar year of effective_date
    (falling back to date) -- i.e. an offseason move is attributed to the
    year it actually happened in, not necessarily the upcoming season it
    might be "for". If that doesn't match how season boundaries are
    defined elsewhere (e.g. dim_player.sql), this is the place to adjust.
    """
    for txn in get_all_player_transactions():
        player_id = txn.get("player_id")
        if player_id is None:
            continue

        txn_date = txn.get("effective_date") or txn.get("date")
        if txn_date is None:
            print(f"Skipping txn {txn.get('transaction_id')} for player {player_id}: no date to derive season from")
            continue

        try:
            season = txn_date.year if hasattr(txn_date, "year") else int(str(txn_date)[:4])
        except (ValueError, TypeError):
            print(f"Skipping txn {txn.get('transaction_id')} for player {player_id}: unparseable date {txn_date!r}")
            continue

        yield {
            "player_id": player_id,
            "season": season,
            "transaction_id": txn.get("transaction_id"),
            "player_name": txn.get("player_name"),
            "from_team_id": txn.get("from_team_id"),
            "from_team_name": txn.get("from_team_name"),
            "to_team_id": txn.get("to_team_id"),
            "to_team_name": txn.get("to_team_name"),
            "type_code": txn.get("type_code"),
            "type_desc": txn.get("type_desc"),
            "description": txn.get("description"),
            "date": txn.get("date"),
            "effective_date": txn.get("effective_date"),
            "resolution_date": txn.get("resolution_date"),
        }


# ----------------------------
# Seasons
# ----------------------------
@dlt.resource(name="seasons", write_disposition="replace")
def seasons_resource(start_year: int = None, end_year: int = None):
    start_year = start_year or 1960
    end_year = end_year or 2026
    for year in range(start_year, end_year + 1):
        try:
            data = get_json(f"{BASE_URL}/seasons/{year}", {"sportId": 1})
        except Exception as e:
            print(f"Season fetch failed for {year}: {e}")
            continue
        seasons = data.get("seasons")
        if seasons:
            yield seasons
        else:
            print(f"No season data returned for {year}: {data}")

# ----------------------------
# Teams
# ----------------------------
@dlt.resource(name="teams", write_disposition="merge", primary_key=["id", "season"])
def teams_resource(start_year: int = None, end_year: int = None):
    """
    One row per team PER SEASON, not just one row per team ever --
    league_id, division, venue, etc. can all change season to season
    (franchise moves between leagues, expansion teams, relocations), so
    collapsing to a single row per team_id would silently freeze every
    team at whatever season it happened to get de-duped on.

    BUG FIX: the previous version deduped by team_id with a single
    `seen_ids` set shared across the ENTIRE start_year..end_year loop, so
    only the very FIRST season a given team_id was ever seen in (1960,
    since the loop starts there and virtually every currently-active
    team_id already existed by then) ever got yielded -- every later
    season for that team_id was silently skipped. Combined with the old
    primary_key=["id"] (which would have collapsed even multiple yielded
    seasons down to one row per id on merge anyway), this left dw.teams
    with exactly one stale row per team, frozen at season=1960 -- any
    downstream join that filters on `season` (e.g. resolving a team's
    league_id for the CURRENT year, to flag interleague games) can never
    match a present-day season and silently falls through to a default/
    null value instead of erroring.
    """
    current_year = datetime.now().year
    start_year = start_year or 1960
    end_year = end_year or current_year

    for year in range(start_year, end_year + 1):
        try:
            data = get_json(f"{BASE_URL}/teams", {"sportId": 1, "season": year})
        except Exception as e:
            print(f"[teams] fetch failed for season {year}: {e}")
            continue

        # Dedup is scoped to THIS year's response only (defends against the
        # API returning a team twice within one call) -- NOT across years,
        # since we deliberately want one row per team per season now.
        seen_ids_this_year = set()
        for team in data.get("teams", []):
            team_id = team.get("id")
            if team_id and team_id not in seen_ids_this_year:
                seen_ids_this_year.add(team_id)
                yield team


# ----------------------------
# Draft Picks
# ----------------------------
@dlt.resource(
    name="draft",
    write_disposition="append",
    primary_key=("year", "pick_number"),
)
def draft_resource():
    import traceback
    try:
        for year in range(2026, 2027):
            # BASE_URL already ends in "/", so no leading "/" here —
            # otherwise this becomes ".../api/v1//draft/2026" (double
            # slash), which is why this endpoint was returning nothing.
            url = f"{BASE_URL}draft/{year}"
            print(f"[draft] fetching {url}")
            data = get_json(url)
            drafts = data.get("drafts")
            if not drafts:
                print(f"[draft] no 'drafts' key in response for {year}: keys={list(data.keys())}")
                continue
            rounds = drafts.get("rounds")
            if not isinstance(rounds, list):
                print(f"[draft] 'rounds' missing or not a list for {year}: {type(rounds)}")
                continue
            pick_count = 0
            for round_obj in rounds:
                picks = round_obj.get("picks", [])
                for pick in picks:
                    pick_count += 1
                    yield pick
            print(f"[draft] yielded {pick_count} picks for {year}")
    except Exception:
        print("(draft_resource) Error:")
        traceback.print_exc()


# ----------------------------
# Play-by-play fetch helper
# ----------------------------
def fetch_game_pbp(year, game_pk):
    try:
        pbp = get_json(f"{BASE_URL}/game/{game_pk}/playByPlay")
        events = []

        for play in pbp.get("allPlays", []):
            at_bat_index = play.get("atBatIndex")
            matchup = play.get("matchup", {})
            batter = matchup.get("batter", {})
            pitcher = matchup.get("pitcher", {})
            batter_hand = matchup.get("batSide", {})
            pitcher_hand = matchup.get("pitchHand", {})

            for event in play.get("playEvents", []):
                if not event.get("isPitch"):
                    continue

                details = event.get("details", {})
                is_in_play = details.get("isInPlay", False)

                event["season"] = year
                event["gamePk"] = game_pk
                event["atBatIndex"] = at_bat_index
                event["isInPlay"] = is_in_play
                event["batter_id"] = batter.get("id")
                event["batter_name"] = batter.get("fullName")
                event["pitcher_id"] = pitcher.get("id")
                event["pitcher_name"] = pitcher.get("fullName")
                event["batter_side"] = batter_hand.get("code")
                event["pitcher_hand"] = pitcher_hand.get("code")

                events.append(event)

        return events
    except Exception:
        return []


# ----------------------------
# Players (40-man rosters)
# ----------------------------
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
                player = award.get("player", {})
                position = player.get("primaryPosition", {})
                team = award.get("team", {})

                yield {
                    "award_id": award.get("id"),
                    "award_name": award.get("name"),
                    "award_date": award.get("date"),
                    "season": award.get("season") or year,
                    "team_id": team.get("id"),
                    "player_id": player.get("id"),
                    "player_name": player.get("nameFirstLast"),
                    "position_code": position.get("code"),
                    "position_name": position.get("name"),
                    "position_type": position.get("type"),
                    "position_abbreviation": position.get("abbreviation"),
                    "notes": award.get("notes"),
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
            params["endDate"] = f"{year}-12-31"

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
    """
    Returns game_pks from dw.games that are:
      - In a valid season (>= start_year)
      - Final, regular-season, non-exhibition games (consistent with
        get_valid_game_pks_from_dw so the umpires FK is always valid)
      - Not yet present in dw.umpires
    Sourcing exclusively from dw.games guarantees dbt relationship
    tests on umpires.game_pk -> games.game_pk always pass.
    """
    conn = pyodbc.connect(DW_CONNECTION_STRING, timeout=10)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT game_pk
        FROM dw.games
        WHERE season >= ?
          AND status__abstract_game_state = 'Final'
          AND game_type NOT IN ('E', 'A')
          AND status__detailed_state NOT IN ('Cancelled', 'Postponed')
          AND game_pk NOT IN (SELECT game_pk FROM dw.umpires)
    """, (start_year,))
    rows = cursor.fetchall()
    conn.close()
    print(f"[umpires] {len(rows)} games in dw.games are missing umpire records.")
    return [r[0] for r in rows]


@dlt.resource(
    name="umpires",
    write_disposition="merge",  # changed from append
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
                "game_pk": game_pk,
                "official_type": umpire.get("officialType"),
                "official_id": official.get("id"),
                "full_name": official.get("fullName"),
            }

    print(f"Yielded {loaded} umpire records.")
    print(f"{len(no_officials)} games had no officials in API response: {no_officials}")


# ----------------------------
# Game details helper
# ----------------------------
def _to_24_hour(time_str, ampm=None):
    """
    Normalize a 12-hour local time to 24-hour "HH:MM" format.

    Accepts either:
      - a bare hour:minute string plus a separate ampm ("7:05", "PM")
        (gameData.datetime.time / .ampm from the live feed), or
      - a single combined string ("7:05 PM")
        (the boxscore's "first pitch" info label).

    Returns None if the input is missing or doesn't parse cleanly, rather
    than raising -- a malformed value here should fall through to NULL,
    not blow up the whole game_details fetch.
    """
    if not time_str:
        return None
    raw = time_str.strip()
    if ampm:
        raw = f"{raw} {ampm.strip()}"
    try:
        return datetime.strptime(raw, "%I:%M %p").strftime("%H:%M")
    except ValueError:
        return None


def fetch_game_details(game):
    game_pk = game.get("gamePk")
    result = {"game_pk": game_pk}

    # ── 1. Live feed (v1.1) ─────────────────────────────────────────
    try:
        feed = get_json(f"{BASE_URL.replace('/v1/', '/v1.1/')}/game/{game_pk}/feed/live")
        game_data = feed.get("gameData", {})
        live_data = feed.get("liveData", {})
        weather = game_data.get("weather", {})
        game_info = game_data.get("gameInfo", {})
        game_datetime = game_data.get("datetime", {})
        decisions = live_data.get("decisions", {})
        winner = decisions.get("winner", {})
        loser = decisions.get("loser", {})
        save = decisions.get("save", {})
        boxscore = live_data.get("boxscore", {})
        teams_box = boxscore.get("teams", {})

        home_stats = teams_box.get("home", {}).get("teamStats", {})
        away_stats = teams_box.get("away", {}).get("teamStats", {})
        home_batting = home_stats.get("batting", {})
        away_batting = away_stats.get("batting", {})
        home_pitching = home_stats.get("pitching", {})
        away_pitching = away_stats.get("pitching", {})

        result.update({
            "weather_condition": weather.get("condition"),
            "weather_temp": weather.get("temp"),
            "weather_wind": weather.get("wind"),
            "attendance": game_info.get("attendance"),
            # gameData.datetime.time / .ampm are the API's own LOCAL
            # ballpark wall-clock fields (e.g. "7:05" + "PM") -- no
            # timezone conversion needed. Normalized to 24-hour "HH:MM"
            # (e.g. "19:05") so first_pitch sorts/parses/compares cleanly
            # downstream without AM/PM string handling. gameInfo.firstPitch
            # is a UTC ISO timestamp instead; kept separately below in case
            # it's useful, but NOT used as first_pitch since mixing UTC
            # and local values in one column was the root cause of the
            # bogus hour-23 values (e.g. a 7:05 PM ET game is 23:05 UTC).
            "first_pitch": _to_24_hour(
                game_datetime.get("time"), game_datetime.get("ampm")
            ),
            "first_pitch_utc_raw": game_info.get("firstPitch"),
            "game_duration_minutes": game_info.get("gameDurationMinutes"),
            "winning_pitcher_id": winner.get("id"),
            "winning_pitcher_name": winner.get("fullName"),
            "losing_pitcher_id": loser.get("id"),
            "losing_pitcher_name": loser.get("fullName"),
            "save_pitcher_id": save.get("id"),
            "save_pitcher_name": save.get("fullName"),
            "home_runs": home_batting.get("runs"),
            "home_hits": home_batting.get("hits"),
            "home_doubles": home_batting.get("doubles"),
            "home_triples": home_batting.get("triples"),
            "home_home_runs": home_batting.get("homeRuns"),
            "home_rbi": home_batting.get("rbi"),
            "home_stolen_bases": home_batting.get("stolenBases"),
            "home_strikeouts": home_batting.get("strikeOuts"),
            "home_walks": home_batting.get("baseOnBalls"),
            "home_left_on_base": home_batting.get("leftOnBase"),
            "away_runs": away_batting.get("runs"),
            "away_hits": away_batting.get("hits"),
            "away_doubles": away_batting.get("doubles"),
            "away_triples": away_batting.get("triples"),
            "away_home_runs": away_batting.get("homeRuns"),
            "away_rbi": away_batting.get("rbi"),
            "away_stolen_bases": away_batting.get("stolenBases"),
            "away_strikeouts": away_batting.get("strikeOuts"),
            "away_walks": away_batting.get("baseOnBalls"),
            "away_left_on_base": away_batting.get("leftOnBase"),
            "home_pitching_strikeouts": home_pitching.get("strikeOuts"),
            "home_pitching_walks": home_pitching.get("baseOnBalls"),
            "home_pitching_hits_allowed": home_pitching.get("hits"),
            "home_pitching_runs_allowed": home_pitching.get("runs"),
            "home_pitching_earned_runs": home_pitching.get("earnedRuns"),
            "home_pitching_home_runs_allowed": home_pitching.get("homeRuns"),
            "home_errors": home_batting.get("errors") or home_pitching.get("errors"),
            "away_pitching_strikeouts": away_pitching.get("strikeOuts"),
            "away_pitching_walks": away_pitching.get("baseOnBalls"),
            "away_pitching_hits_allowed": away_pitching.get("hits"),
            "away_pitching_runs_allowed": away_pitching.get("runs"),
            "away_pitching_earned_runs": away_pitching.get("earnedRuns"),
            "away_pitching_home_runs_allowed": away_pitching.get("homeRuns"),
            "away_errors": away_batting.get("errors") or away_pitching.get("errors"),
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
                # NOTE: previously this also fell back to info_map.get("t"),
                # but "t" is the game DURATION label (e.g. "3:22"), parsed
                # a few lines below -- not a time-of-day. That fallback
                # was silently writing duration strings into first_pitch
                # whenever a boxscore lacked a "first pitch" label. Fixed
                # to only use the actual "first pitch" label, and run it
                # through the same 24-hour normalizer as the live-feed
                # branch so first_pitch is always "HH:MM" either way.
                result["first_pitch"] = _to_24_hour(info_map.get("first pitch"))
            if not result.get("game_duration_minutes"):
                duration_raw = info_map.get("t")  # "3:22" format
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
                result["winning_pitcher_id"] = result.get("winning_pitcher_id") or w.get("id")
                result["winning_pitcher_name"] = result.get("winning_pitcher_name") or w.get("fullName")
                result["losing_pitcher_id"] = result.get("losing_pitcher_id") or l.get("id")
                result["losing_pitcher_name"] = result.get("losing_pitcher_name") or l.get("fullName")
                result["save_pitcher_id"] = result.get("save_pitcher_id") or s.get("id")
                result["save_pitcher_name"] = result.get("save_pitcher_name") or s.get("fullName")
        except Exception as e:
            print(f"Linescore fallback failed for {game_pk}: {e}")

    return result


def get_game_pks_from_dw(start_year, end_year):
    """
    Returns game_pks from dw.games scoped to a year range for game_details,
    EXCLUDING game_pks that already have a row in dw.game_details.

    Previously this had no "already loaded" filter at all, unlike
    get_loaded_pitch_game_pks (play_events) and get_games_missing_umpires
    (umpires). With no --start-year passed, start_year defaulted to 1960,
    so every single run re-fetched and re-merged full game history —
    tens of thousands of games, each costing up to 3 API calls in
    fetch_game_details (live feed + boxscore/linescore fallbacks) — instead
    of only the handful of new games since the last run. That's both the
    slowness and (from MERGE-ing the entire history back into SQL Server
    every run) the source of the database errors. The NOT EXISTS below
    makes this incremental like its sibling resources: after the first
    backfill, only genuinely new game_pks are fetched.

    If you ever need to force-reprocess games that already have details
    (e.g. to pick up corrected data), delete those specific game_pks from
    dw.game_details first, or add a force_reload flag — don't remove this
    filter, or you're back to a full reload every run.
    """
    current_year = datetime.now().year
    start_year = start_year or 1960
    end_year = end_year or current_year

    conn = pyodbc.connect(DW_CONNECTION_STRING, timeout=10)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT g.game_pk
        FROM dw.games g
        WHERE g.season BETWEEN ? AND ?
          AND g.status__abstract_game_state = 'Final'
          AND g.game_type NOT IN ('E', 'A')
          AND NOT EXISTS (
              SELECT 1 FROM dw.game_details gd WHERE gd.game_pk = g.game_pk
          )
    """, (start_year, end_year))
    rows = cursor.fetchall()
    conn.close()
    print(f"[game_details] {len(rows)} games in seasons {start_year}–{end_year} "
          f"are missing from dw.game_details and will be fetched.")
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
        done = 0
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
# Player game stats (per-player, per-game boxscore)
# ----------------------------
def get_game_pks_missing_player_stats(start_year: int = None, end_year: int = None) -> list:
    """
    Same incremental pattern as get_game_pks_from_dw() / get_games_missing_umpires():
    scope to dw.games (Final, non-exhibition) within the year range, and skip any
    game_pk that already has rows in dw.player_game_stats.
    """
    current_year = datetime.now().year
    start_year = start_year or 1960
    end_year = end_year or current_year

    conn = pyodbc.connect(DW_CONNECTION_STRING, timeout=10)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT g.game_pk
            FROM dw.games g
            WHERE g.season BETWEEN ? AND ?
              AND g.status__abstract_game_state = 'Final'
              AND g.game_type NOT IN ('E', 'A')
              AND NOT EXISTS (
                  SELECT 1 FROM dw.player_game_stats pgs WHERE pgs.game_pk = g.game_pk
              )
        """, (start_year, end_year))
        rows = cursor.fetchall()
    except pyodbc.Error as e:
        # First-ever run: dw.player_game_stats doesn't exist yet because dlt
        # only creates a destination table once it has received data to write
        # (it can't create the table ahead of the first load). Fall back to an
        # unfiltered query for this range -- there's nothing to exclude yet
        # anyway. Once this resource has run once, the table exists and the
        # NOT EXISTS branch above takes over on every subsequent run.
        print(f"[player_game_stats] dw.player_game_stats not found yet ({e}); "
              f"treating this as a first-time full load for {start_year}–{end_year}.")
        cursor.execute("""
            SELECT g.game_pk
            FROM dw.games g
            WHERE g.season BETWEEN ? AND ?
              AND g.status__abstract_game_state = 'Final'
              AND g.game_type NOT IN ('E', 'A')
        """, (start_year, end_year))
        rows = cursor.fetchall()
    conn.close()
    print(f"[player_game_stats] {len(rows)} games in seasons {start_year}–{end_year} "
          f"are missing from dw.player_game_stats and will be fetched.")
    return [r[0] for r in rows]


def fetch_boxscore_player_stats(game_pk: int) -> list:
    """
    One row per player per game, for both the away and home side, pulled from
    the same /game/{pk}/boxscore endpoint umpires_resource already calls
    (that resource only reads `officials` off this payload — everything
    under teams.{home,away}.players is the per-player batting/pitching/
    fielding line for this specific game).
    """
    try:
        bs = get_json(f"{BASE_URL}/game/{game_pk}/boxscore")
    except Exception as e:
        print(f"Boxscore fetch failed for game {game_pk}: {e}")
        return []

    rows = []
    teams = bs.get("teams", {})
    for side in ("home", "away"):
        side_data = teams.get(side, {})
        team = side_data.get("team", {})
        team_id = team.get("id")
        team_name = team.get("name")

        for player in side_data.get("players", {}).values():
            person = player.get("person") or {}
            position = player.get("position") or {}
            stats = player.get("stats") or {}
            batting = stats.get("batting") or {}
            pitching = stats.get("pitching") or {}
            fielding = stats.get("fielding") or {}

            rows.append({
                "game_pk": game_pk,
                "player_id": person.get("id"),
                "player_name": person.get("fullName"),
                "team_id": team_id,
                "team_name": team_name,
                "is_home": side == "home",
                "position_code": position.get("code"),
                "position_name": position.get("name"),
                "position_abbreviation": position.get("abbreviation"),
                # Batting
                "batting_at_bats": batting.get("atBats"),
                "batting_runs": batting.get("runs"),
                "batting_hits": batting.get("hits"),
                "batting_doubles": batting.get("doubles"),
                "batting_triples": batting.get("triples"),
                "batting_home_runs": batting.get("homeRuns"),
                "batting_rbi": batting.get("rbi"),
                "batting_walks": batting.get("baseOnBalls"),
                "batting_strikeouts": batting.get("strikeOuts"),
                "batting_stolen_bases": batting.get("stolenBases"),
                "batting_left_on_base": batting.get("leftOnBase"),
                "batting_avg": batting.get("avg"),
                # Pitching
                "pitching_innings_pitched": pitching.get("inningsPitched"),
                "pitching_hits_allowed": pitching.get("hits"),
                "pitching_runs_allowed": pitching.get("runs"),
                "pitching_earned_runs": pitching.get("earnedRuns"),
                "pitching_walks": pitching.get("baseOnBalls"),
                "pitching_strikeouts": pitching.get("strikeOuts"),
                "pitching_home_runs_allowed": pitching.get("homeRuns"),
                "pitching_era": pitching.get("era"),
                # Fielding
                "fielding_putouts": fielding.get("putOuts"),
                "fielding_assists": fielding.get("assists"),
                "fielding_errors": fielding.get("errors"),
                "fielding_chances": fielding.get("chances"),
            })

    return rows


@dlt.resource(
    name="player_game_stats",
    write_disposition="merge",
    primary_key=("game_pk", "player_id"),
)
def player_game_stats_resource(start_year: int = None, end_year: int = None):
    game_pks = get_game_pks_missing_player_stats(start_year, end_year)
    print(f"Fetching player boxscore stats for {len(game_pks)} games...")

    if not game_pks:
        print("Nothing new to load.")
        return

    BATCH_SIZE = 200
    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = {
            executor.submit(fetch_boxscore_player_stats, game_pk): game_pk
            for game_pk in game_pks
        }
        batch = []
        done = 0
        total = len(futures)

        for future in as_completed(futures):
            done += 1
            try:
                rows = future.result()
                batch.extend(rows)
            except Exception as e:
                print(f"Player stats fetch failed for game {futures[future]}: {e}")
                continue

            if len(batch) >= BATCH_SIZE:
                yield batch
                batch = []

            if done % 500 == 0:
                print(f"Progress: {done}/{total} games processed")

        if batch:
            yield batch


# ----------------------------
# Stats helpers
# ----------------------------
def get_all_stats(year: int, group: str) -> list:
    all_splits = []
    limit = 500
    offset = 0

    while True:
        data = get_json(
            f"{BASE_URL}/stats",
            {
                "stats": "season",
                "group": group,
                "season": year,
                "sportIds": 1,
                "playerPool": "All",
                "limit": limit,
                "offset": offset,
            }
        )
        stat_groups = data.get("stats", [])
        if not stat_groups:
            break
        splits = stat_groups[0].get("splits", [])
        if not splits:
            break
        all_splits.extend(splits)
        total = stat_groups[0].get("totalSplits", 0)
        offset += limit
        if offset >= total:
            break

    return all_splits


@dlt.resource(
    name="player_stats",
    write_disposition="merge",
    primary_key=["season", "player__id"]
)
def stats_resource(start_year: int = None, end_year: int = None):
    current_year = datetime.now().year
    start_year = start_year or current_year
    end_year = end_year or current_year

    # Prefix each group's stat fields so they don't collide when merged
    # onto the same row (e.g. hitting has "avg", pitching doesn't, but
    # both have "gamesPlayed" -- prefixing avoids one group silently
    # clobbering another's value for a same-named field).
    GROUP_PREFIX = {"hitting": "batting", "pitching": "pitching",
                     "fielding": "fielding", "running": "running"}

    for year in range(start_year, end_year + 1):
        # player__id -> merged record for this season
        players_this_year: dict[int, dict] = {}

        for group in ["hitting", "pitching", "fielding", "running"]:
            prefix = GROUP_PREFIX[group]
            for split in get_all_stats(year, group):
                player_id = split["player"]["id"]
                record = players_this_year.setdefault(player_id, {
                    "season": year,
                    "player": split.get("player"),
                    "team": split.get("team"),
                })

                # Fielding is the only group with a position sub-object.
                # We no longer need the NULL-in-merge-key sentinel (group
                # isn't part of the key anymore), but position is still
                # worth keeping -- tag it so you know which group it came
                # from, since a player can field under both "fielding"
                # and incidentally have a pitching position too.
                if split.get("position"):
                    record[f"{prefix}_position"] = split["position"]

                for key, value in split.items():
                    if key in ("player", "team", "position"):
                        continue
                    record[f"{prefix}_{key}"] = value

        yield from players_this_year.values()

# ----------------------------
# Play Events (incremental)
# ----------------------------
@dlt.resource(
    name="play_events",
    write_disposition="merge",
    primary_key=("gamePk", "playId"),
)
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
            params["endDate"] = f"{year}-12-31"
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
                    status = game.get("status", {}).get("abstractGameState")
                    game_type = game.get("gameType", "")
                    if game_type in ("E", "A"):
                        continue
                    if game_pk and status == "Final":
                        all_games.append((year, game_pk))

    # Filter to only game_pks that exist in dw.games — guarantees dbt
    # relationship tests pass.
    valid_pks = get_valid_game_pks_from_dw()
    before = len(all_games)
    all_games = [(y, pk) for y, pk in all_games if pk in valid_pks]
    print(f"{before} total Final games from schedule, "
          f"{len(all_games)} exist in dw.games...")

    # Skip games whose pitches are already loaded — an efficiency filter,
    # not a correctness one, now that play_events uses merge disposition
    # (see get_loaded_pitch_game_pks docstring for why this is separate
    # from the valid_pks filter above). Explicit --start-year/--end-year
    # backfills still go through this same filter; pass force_reload=True
    # below (or just query/delete the specific game_pks first) if you
    # ever need to deliberately reprocess an already-loaded game.
    loaded_pks = get_loaded_pitch_game_pks()
    before = len(all_games)
    all_games = [(y, pk) for y, pk in all_games if pk not in loaded_pks]
    print(f"{before} candidate games, {len(all_games)} not yet loaded and will be fetched...")

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
        done = 0
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
    parser.add_argument("--end-year", type=int, default=None)
    args = parser.parse_args()

    pipeline = dlt.pipeline(
        pipeline_name="mlb_full_stats",
        destination="mssql",
        dataset_name="dw"
    )

    resources = {
        "play_events": lambda: play_events_resource(args.start_year, args.end_year),
        "games": lambda: games_resource(args.start_year, args.end_year),
        "seasons": lambda: seasons_resource(args.start_year, args.end_year),
        "teams": lambda: teams_resource(args.start_year, args.end_year),
        "draft": draft_resource,
        "players": players_resource,
        "player_stats": lambda: stats_resource(args.start_year, args.end_year),
        "player_game_stats": lambda: player_game_stats_resource(args.start_year, args.end_year),
        "rosters": lambda: rosters_resource(args.start_year, args.end_year),
        "awards": award_recipients_resource,
        "player_transactions": lambda: transactions_resource(args.start_year, args.end_year),
        "player_season_transactions": player_season_transactions_resource,
        "umpires": lambda: umpires_resource(args.start_year, args.end_year),
        "game_details": lambda: game_details_resource(args.start_year, args.end_year),
    }

    if args.resource not in resources:
        print(f"Unknown resource '{args.resource}'. Choose from: {', '.join(resources)}")
    else:
        load_info = pipeline.run([resources[args.resource]()])
        print("Load complete")
        print(load_info)