# mlb-semantic-layer

A [Cube](https://cube.dev) semantic layer over the MLB data warehouse built by
`mlb_load.py` (dlt) and `dbt_baseball` (dbt), plus `cube_test_runner.py`, which
tests every cube.

## What is Cube?

Cube (Cube Core) is an open-source **semantic layer**. It sits between the
database and the tools that query it, and defines business terms in one place:

- **Cubes** map to tables or views (here, the `silver` star schema and two `dw` views).
- **Measures** are the numbers you aggregate: game counts, total attendance,
  win %, whiff rate.
- **Dimensions** are what you group and filter by: season, team, pitch type,
  game date.
- **Joins** say how cubes relate, so a query can mix fields from several cubes
  without anyone writing the SQL join.

You ask Cube for *"total wins by team for 2024"*. Cube writes the SQL Server
query, runs it, and returns the result. A measure such as `win_pct` is defined
once, so a dashboard, a notebook and an AI agent all get the same number.

Cube offers:

- **REST (JSON) and GraphQL APIs** for apps and scripts
- **A SQL API** (Postgres-compatible, port 15432), so tools like Power BI,
  Tableau, DBeaver or `psql` can query cubes like tables
- **Playground**, a web UI for building and testing queries
- **Caching and pre-aggregations** for speed

## How this folder is organized

```
mlb-semantic-layer/
├── model/
│   └── cubes/              # One YAML file per cube (see below)
├── cube_test_runner.py     # Tests every cube's measures, dimensions and joins
├── requirements.txt        # Python packages for cube_test_runner.py
└── .env                    # Cube settings: NOT in Git (see Install, step 3)
```

The cubes in `model/cubes/`:

| Kind | Cubes | Examples of what they answer |
|------|-------|------------------------------|
| Facts | `fact_games`, `fact_at_bats`, `fact_batted_balls`, `fact_player_stats`, `fact_draft`, `fact_umpire_performance` | Attendance, run totals, exit velocity, hard-hit rate, umpire overturn rate |
| Dimensions | `dim_games`, `dim_teams`, `dim_player`, `dim_venue`, `dim_season`, `dim_date`, `dim_pitch_type`, `dim_zone`, `dim_award`, `dim_draft`, `dim_school`, ... | Who, where, when, which pitch |
| Standings and stats | `dim_team_standings`, `dim_team_standings_by_date`, `dim_player_stats`, `dim_player_stats_by_date`, `vw_batting_stats`, `vw_pitching_stats` | Wins, win %, season-to-date stats, OPS, ERA |
| Pitch level | `int_pitches_enriched` | Swing, whiff, chase and zone rates by pitch type |
| Role-playing cubes | `home_teams`, `away_teams`, `opponent_teams`, `winning_pitchers`, `losing_pitchers`, `save_pitchers`, `credited_pitchers`, `first_pitchers`, `at_bat_batters`, `official_dates` | The same team, player or date table used in two roles in one query |

### Role-playing cubes

A game has a home team *and* an away team, and both come from `dim_teams`. Cube
needs a separate cube name for each role, so these cubes **extend** a base cube
under a new name:

```yaml
# model/cubes/home_teams.yml
cubes:
  - name: home_teams
    extends: dim_teams
```

`dim_games` then joins to both roles:

```yaml
# model/cubes/dim_games.yml (joins excerpt)
      - name: home_teams
        sql: "{CUBE}.home_team_id = {home_teams.team_id} AND {CUBE}.season = {home_teams.season}"
        relationship: many_to_one
```

`dim_teams` has **one row per team per season**, so team joins always match on
`team_id` **and** `season`. Joining on `team_id` alone multiplies rows.

## Install Cube

Cube runs in Docker. The steps are for Windows; macOS and Linux are the same
except for the shell.

### 1. Install Docker

Install [Docker Desktop](https://www.docker.com/products/docker-desktop/) and
start it.

### 2. Add `docker-compose.yml` to this folder

Skip this if the folder already has one.

```yaml
services:
  cube:
    image: cubejs/cube:latest
    ports:
      - 4000:4000     # Playground, REST and GraphQL APIs
      - 15432:15432   # SQL API
    env_file: .env
    volumes:
      - .:/cube/conf  # Cube reads model/ from this folder
```

### 3. Create `.env` in this folder

Copy the `CUBEJS_*` lines from the repo's [`.env.template`](../.env.template)
and fill them in:

```ini
CUBEJS_DB_TYPE=mssql
CUBEJS_DB_HOST=host.docker.internal   # the PC running Docker; or your SQL Server's name
CUBEJS_DB_PORT=1433
CUBEJS_DB_NAME=YOUR_DATABASE_NAME
CUBEJS_DB_USER=YOUR_DB_USERNAME
CUBEJS_DB_PASS=YOUR_DB_PASSWORD
CUBEJS_API_SECRET=any-long-random-string
CUBEJS_DEV_MODE=true
```

- `host.docker.internal` lets the container reach SQL Server on your own PC.
  For a server elsewhere, use its name or IP address.
- SQL Server must accept **SQL logins** and **TCP/IP on port 1433**. Check both
  in SQL Server Configuration Manager.
- `.env` holds real passwords. It is in `.gitignore`, and the release script
  never copies it.

### 4. Start Cube

```bash
docker compose up -d
```

Open <http://localhost:4000> to see **Playground**. If a cube shows an error,
check its YAML file. `docker compose logs -f cube` shows details.

> **Development mode is for your own PC only.** `CUBEJS_DEV_MODE=true` turns
> off API authentication and turns on Playground. Never use it on a shared
> server or one reachable from the internet. For a shared setup, set it to
> `false` and use a strong `CUBEJS_API_SECRET`.

## Examples

Run each example as a query in Playground (paste the JSON into the JSON query
editor), from Python against the REST API, or as SQL through the SQL API.

### 1. Team standings: top 10 teams by wins in 2024

```json
{
  "measures": ["dim_team_standings.total_wins", "dim_team_standings.total_losses", "dim_team_standings.win_pct"],
  "dimensions": ["dim_team_standings.team_name"],
  "filters": [{ "member": "dim_team_standings.season", "operator": "equals", "values": ["2024"] }],
  "order": { "dim_team_standings.total_wins": "desc" },
  "limit": 10
}
```

`total_wins` counts every game type in the season (spring training, regular
season, postseason). To narrow it, add a filter on
`dim_team_standings.game_type_desc`.

### 2. Attendance and game length by season

```json
{
  "measures": ["fact_games.count", "fact_games.avg_attendance", "fact_games.avg_game_duration_minutes"],
  "dimensions": ["fact_games.season"],
  "order": { "fact_games.season": "asc" }
}
```

### 3. A join through a role-playing cube: games by home team's league

No SQL join is written: Cube follows the `dim_games` → `home_teams` join.

```json
{
  "measures": ["dim_games.count", "dim_games.avg_run_diff"],
  "dimensions": ["home_teams.league_name"],
  "filters": [{ "member": "dim_games.season", "operator": "equals", "values": ["2024"] }]
}
```

### 4. A time dimension: a team's win % through the season, week by week

```json
{
  "measures": ["dim_team_standings_by_date.win_pct_season_to_date"],
  "dimensions": ["dim_team_standings_by_date.team_name"],
  "timeDimensions": [{
    "dimension": "dim_team_standings_by_date.game_date",
    "granularity": "week",
    "dateRange": ["2024-03-20", "2024-09-30"]
  }],
  "filters": [{ "member": "dim_team_standings_by_date.team_name", "operator": "contains", "values": ["Dodgers"] }]
}
```

### 5. Pitch level: whiff and chase rate by pitch type for one pitcher

```json
{
  "measures": ["int_pitches_enriched.count", "int_pitches_enriched.whiff_rate", "int_pitches_enriched.chase_rate", "int_pitches_enriched.avg_release_speed"],
  "dimensions": ["int_pitches_enriched.pitch_type_description"],
  "filters": [{ "member": "int_pitches_enriched.pitcher_name", "operator": "equals", "values": ["Tarik Skubal"] }],
  "order": { "int_pitches_enriched.count": "desc" }
}
```

### 6. Replay challenges: how often each type is overturned

```json
{
  "measures": ["dim_challenges.count", "dim_challenges.overturned_count", "dim_challenges.overturn_rate"],
  "dimensions": ["dim_challenges.review_type"],
  "order": { "dim_challenges.count": "desc" }
}
```

### Calling the REST API from Python

```python
import json
import requests

query = {
    "measures": ["dim_team_standings.total_wins", "dim_team_standings.win_pct"],
    "dimensions": ["dim_team_standings.team_name"],
    "filters": [{"member": "dim_team_standings.season", "operator": "equals", "values": ["2024"]}],
    "order": {"dim_team_standings.total_wins": "desc"},
    "limit": 10,
}

# In development mode no token is needed. Otherwise send a JWT signed with
# CUBEJS_API_SECRET in the Authorization header.
resp = requests.get(
    "http://localhost:4000/cubejs-api/v1/load",
    params={"query": json.dumps(query)},
    timeout=60,
)
resp.raise_for_status()
for row in resp.json()["data"]:
    print(row)
```

### The same question through the SQL API

Connect any Postgres client to `localhost`, port `15432`. Cubes look like
tables. Measures are wrapped in `MEASURE()`:

```sql
SELECT team_name,
       MEASURE(total_wins) AS wins,
       MEASURE(win_pct)    AS win_pct
FROM dim_team_standings
WHERE season = 2024
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;
```

## Testing the cubes

`cube_test_runner.py` sends test queries for every cube's measures, dimensions
and joins (including the role-playing cubes) to a running Cube, and reports any
that fail.

```bash
pip install -r requirements.txt
python cube_test_runner.py --cubes-dir model\cubes --env-file .env
```

Run it after changing a cube, or after a dbt change that renames or retypes a
column in the `silver` schema.

## Tips

- **Count vs. distinct:** some cubes have more than one row per thing.
  `dim_teams.count` counts team-*seasons*; use `dim_teams.distinct_teams` for
  "how many teams".
- **Averages of averages:** `vw_batting_stats.avg_ops` and
  `vw_pitching_stats.avg_era` are plain averages across player-seasons, not
  weighted by plate appearances or innings.
- **Hidden fields:** keys and raw columns are marked `public: false`. They
  still work in joins but don't appear in Playground.

## Learn more

- Cube docs: <https://cube.dev/docs>
- Data modeling reference: <https://cube.dev/docs/product/data-modeling/reference/cube>
- REST API: <https://cube.dev/docs/product/apis-integrations/rest-api>
- SQL API: <https://cube.dev/docs/product/apis-integrations/sql-api>

## Part of

[mlb_dlt](https://github.com/keithwsmith/mlb_dlt.git) — MLB Stats API → SQL Server data warehouse

---
*Generated 2026-11-20*
