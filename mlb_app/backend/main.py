from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

import queries
from db import run_query, run_query_one

app = FastAPI(title="MLB Explorer API")

# Local dev SPA on a different port than the API — wide open CORS is fine
# for a single-user local tool. Tighten this before deploying anywhere else.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# An *unhandled* exception (e.g. a raw pyodbc/SQL error) normally gets
# turned into a 500 by Starlette's outermost error middleware, which sits
# outside CORSMiddleware — so the response has no CORS headers, and the
# browser can't read it. fetch() then reports a generic network failure
# ("Failed to fetch") instead of the real error. Catching it here, inside
# the app, means CORSMiddleware still gets to stamp the response on its
# way out, so the frontend actually sees what went wrong.
@app.exception_handler(Exception)
async def unhandled_exception_handler(request, exc):
    return JSONResponse(status_code=500, content={"detail": str(exc)})


def not_found(what: str):
    raise HTTPException(status_code=404, detail=f"{what} not found")


# ---------------------------------------------------------------------------
# Games
# ---------------------------------------------------------------------------

@app.get("/api/seasons")
def get_seasons():
    return run_query(queries.SEASONS)


@app.get("/api/teams")
def get_teams():
    return run_query(queries.TEAMS)


@app.get("/api/games")
def get_games(season: int | None = None, team_id: int | None = None, opponent_id: int | None = None):
    """
    Supports the three dropdown flows from the spec:
      - ?season=2024                          -> every game that season
      - ?team_id=147                          -> every game for that team
      - ?team_id=147&opponent_id=111          -> that head-to-head matchup
    """
    if team_id and opponent_id:
        return run_query(queries.GAMES_BY_TEAM_AND_OPPONENT, (team_id, opponent_id, opponent_id, team_id))
    if team_id:
        return run_query(queries.GAMES_BY_TEAM, (team_id, team_id))
    if season:
        return run_query(queries.GAMES_BY_SEASON, (season,))
    raise HTTPException(status_code=400, detail="Provide season or team_id")


@app.get("/api/games/{game_pk}/details")
def get_game_details(game_pk: int):
    row = run_query_one(queries.GAME_DETAILS, (game_pk,))
    if not row:
        not_found("Game details")
    return row


@app.get("/api/games/{game_pk}/umpires")
def get_game_umpires(game_pk: int):
    return run_query(queries.GAME_UMPIRES, (game_pk,))


@app.get("/api/games/{game_pk}/umpire-performance")
def get_game_umpire_performance(game_pk: int):
    return run_query(queries.GAME_UMPIRE_PERFORMANCE, (game_pk,))


@app.get("/api/games/{game_pk}/venue")
def get_game_venue(game_pk: int):
    row = run_query_one(queries.GAME_VENUE, (game_pk,))
    if not row:
        not_found("Venue")
    return row


@app.get("/api/games/{game_pk}/players")
def get_game_players(game_pk: int):
    return run_query(queries.GAME_PLAYERS, (game_pk,))


@app.get("/api/games/{game_pk}/players-stats")
def get_game_players_stats(game_pk: int):
    return run_query(queries.GAME_PLAYERS_STATS, (game_pk,))


# ---------------------------------------------------------------------------
# Players
# ---------------------------------------------------------------------------

@app.get("/api/players")
def get_players():
    return run_query(queries.ALL_PLAYERS)


@app.get("/api/venues")
def get_venues():
    return run_query(queries.ALL_VENUES)


@app.get("/api/players/{player_id}")
def get_player_detail(player_id: int):
    row = run_query_one(queries.PLAYER_DETAIL, (player_id,))
    if not row:
        not_found("Player")
    return row


@app.get("/api/players/{player_id}/at-bats")
def get_player_at_bats(player_id: int, role: str = "batter"):
    """role=batter (default) or role=pitcher, per the spec's branching logic."""
    if role == "pitcher":
        return run_query(queries.PLAYER_PITCHING_AT_BATS, (player_id,))
    return run_query(queries.PLAYER_BATTING_AT_BATS, (player_id,))


@app.get("/api/players/{player_id}/draft")
def get_player_draft(player_id: int):
    """Returns the player's single most recent draft record (not a list) —
    PLAYER_DRAFT already orders by draft_year DESC and takes TOP 1, since a
    player can be drafted more than once (e.g. high school, then college)."""
    return run_query_one(queries.PLAYER_DRAFT, (player_id,)) or {}


@app.get("/api/players/{player_id}/awards")
def get_player_awards(player_id: int):
    return run_query(queries.PLAYER_AWARDS, (player_id,))


@app.get("/api/players/{player_id}/season-stats")
def get_player_season_stats(player_id: int):
    return run_query(queries.PLAYER_SEASON_STATS, (player_id,))


@app.get("/api/players/{player_id}/ytd")
def get_player_ytd(player_id: int, game_pk: int, role: str = "batter"):
    """YTD popup: resolve the season from game_pk, then pull from whichever
    dw view matches the player's role in that game (role is passed by the
    frontend — it already knows this per-row, from GAME_PLAYERS_STATS'
    player_role classification — rather than re-deriving it here)."""
    game = run_query_one(queries.GAME_SEASON, (game_pk,))
    if not game:
        not_found("Game")
    season = game["season"]

    if role == "pitcher":
        row = run_query_one(queries.PLAYER_YTD_PITCHING, (player_id, season))
    else:
        row = run_query_one(queries.PLAYER_YTD_BATTING, (player_id, season))
    return row or {}


# ---------------------------------------------------------------------------
# Teams
# ---------------------------------------------------------------------------

@app.get("/api/teams/{team_id}")
def get_team_detail(team_id: int):
    row = run_query_one(queries.TEAM_DETAIL, (team_id,))
    if not row:
        not_found("Team")
    return row


@app.get("/api/teams/{team_id}/venue")
def get_team_venue(team_id: int):
    row = run_query_one(queries.TEAM_VENUE, (team_id,))
    if not row:
        not_found("Venue")
    return row


@app.get("/api/teams/{team_id}/draft")
def get_team_draft(team_id: int):
    return run_query(queries.TEAM_DRAFT, (team_id,))


# ---------------------------------------------------------------------------
# Standalone menu items
# ---------------------------------------------------------------------------

@app.get("/api/awards")
def get_awards():
    return run_query(queries.ALL_AWARDS)


@app.get("/api/draft")
def get_draft():
    return run_query(queries.ALL_DRAFT_PICKS)


@app.get("/api/umpires")
def get_umpires():
    return run_query(queries.ALL_UMPIRES)


# ---------------------------------------------------------------------------
# Matchups & Pitcher — placeholders, spec didn't define these
# ---------------------------------------------------------------------------

@app.get("/api/matchups")
def get_matchups():
    return {"status": "not_specified", "message": "Tell me what a Matchup should show and I'll build it."}


@app.get("/api/pitchers")
def get_pitchers():
    return {"status": "not_specified", "message": "Tell me what the Pitcher menu should show and I'll build it."}
