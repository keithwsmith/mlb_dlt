# mlb_app

FastAPI + React (in-browser Babel, no build step) web app for exploring the MLB data warehouse — browse games, players, teams, awards, draft picks, and umpires, backed by the `silver` star schema and `dw.player_game_stats`.

## Structure

```
mlb_app/
├── main.py          # FastAPI app — route definitions, CORS, error handling
├── db.py             # pyodbc connection helper (DATETIMEOFFSET converter, run_query/run_query_one)
├── queries.py        # All SQL for the app, in one place
├── index.html        # SPA shell — loads React/Babel from a CDN, no build step
├── app.js             # React app (JSX, transpiled in-browser by Babel)
└── styles.css         # App styling
```

## Usage

```bash
# 1. Start the API (from this folder)
uvicorn main:app --reload --port 8000

# 2. Serve the frontend (from this folder, in a second terminal)
python -m http.server 5500
# then open http://localhost:5500 in a browser
```

`app.js` calls the API at `API_BASE = "http://localhost:8000/api"` — update
that constant if you run the backend on a different host or port.

## Setup

```bash
# 1. Create and activate a virtual environment
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate    # macOS / Linux

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment variables
copy ..\.env.template .env    # Windows
# cp ../.env.template .env    # macOS / Linux
# Then edit .env with your SQL Server credentials and settings
```

> **Note:** `db.py`'s `CONNECTION_STRING` is currently hardcoded rather than
> read from `.env` — consider wiring it up to the `MSSQL_*` variables below
> before committing, especially since it contains a plaintext password.

## Environment Variables

This project reads configuration from a `.env` file in the repo root.
See [`.env.template`](../.env.template) for all available variables.

Key variables for this project:

| Variable | Purpose |
|----------|---------|
| `MSSQL_SERVER` | SQL Server hostname or IP |
| `MSSQL_DATABASE` | Target database name |
| `MSSQL_USERNAME` | SQL Server login (leave blank if using Windows auth) |
| `MSSQL_PASSWORD` | SQL Server password |
| `MSSQL_TRUSTED_CONNECTION` | Set `yes` for Windows Authentication |

## API Endpoints

Games, players, teams, awards, draft, and umpires — see `main.py` for the
full route list. All routes are read-only (`GET`) and return JSON built
from the queries in `queries.py`.

## Part of

[mlb_dlt](https://github.com/keithwsmith/mlb_dlt.git) — MLB Stats API → SQL Server data warehouse

---
*Generated 2026-08-03*
