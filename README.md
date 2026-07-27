        # mlb_dlt — MLB Stats API Data Warehouse

        A collection of Python tools that load, transform, monitor, and analyse MLB data
        from the [MLB Stats API](https://statsapi.mlb.com) into a **SQL Server** data
        warehouse using [dlt](https://dlthub.com) and [dbt](https://www.getdbt.com).

        ## Projects

        ### Python Pipeline Tools

        | Folder | Script | Description |
        |--------|--------|-------------|
        | [`mlb_orchestration`](mlb_orchestration/) | pipeline.py | Orchestration pipeline that coordinates and sequences all MLB dlt loads |
| [`mlb_lineage`](mlb_lineage/) | lineage_builder.py | Builds and exports dbt lineage metadata for the MLB data warehouse |
| [`mlb_dlt`](mlb_dlt/) | mlb_load.py | Core dlt-based ELT loader that pulls data from the MLB Stats API and loads it into a SQL Server data warehouse |
| [`ETLMonitor`](ETLMonitor/) | ETLMonitor.py | Monitors ETL pipeline runs and reports on load status, row counts, and errors |
| [`mlb_test_output`](mlb_test_output/) | mlb_test_output.py | Renders dbt test results into a structured report for review |
| [`mlb_latestgame_agent`](mlb_latestgame_agent/) | check_latest_game.py | Agent that checks for the latest completed MLB game and triggers an incremental load if new games are available |

        ### dbt Transformation Layer

        | Folder | Description |
        |--------|-------------|
        | [`dbt_baseball`](dbt_baseball/) | Full dbt project — dimensions, facts, staging models, custom tests, macros, seeds, and sources for the MLB data warehouse |

        The `dbt_baseball` project contains:

        | Subfolder / File | Purpose |
        |------------------|---------|
        | `models/dimensions/` | Dimension tables — games, players, teams, venues, umpires, seasons, etc. |
        | `models/facts/` | Fact tables — pitch-level play events, box scores, stats |
        | `models/staging/` | Lightweight staging models that clean and cast raw source data |
        | `tests/` | Custom singular and generic data tests beyond dbt built-ins |
        | `macros/` | Reusable Jinja macros shared across models |
        | `seeds/` | Static CSV lookups (zones, pitch types, school classifications, etc.) |
        | `snapshots/` | SCD Type-2 snapshots for slowly changing dimensions |
        | `analyses/` | Ad-hoc analytical SQL kept under version control |
        | `dbt_project.yml` | Project configuration — model paths, vars, materialisation defaults |
        | `packages.yml` | dbt package dependencies (e.g. dbt-utils) |
        | `profiles.yml` | SQL Server connection profiles — update for your environment |
        | `sources.yml` | Source definitions and freshness checks for all raw `dw.*` tables |
        | `dimensions_schema.yml` | Column descriptions and data tests for all dimension models |

        ### Web Application

        | Folder | Description |
        |--------|-------------|
        | [`mlb_app`](mlb_app/) | FastAPI + React (in-browser Babel, no build step) web app for exploring the MLB data warehouse — browse games, players, teams, awards, draft picks, and umpires, backed by the `silver` star schema and `dw |

        The `mlb_app` project contains:

        | File | Purpose |
        |------|---------|
        | `main.py` | FastAPI app — route definitions, CORS, centralised error handling |
        | `db.py` | pyodbc connection helper, incl. a `DATETIMEOFFSET` output converter |
        | `queries.py` | All SQL for the app, kept in one place |
        | `index.html` | SPA shell — loads React + Babel from a CDN, no build step |
        | `app.js` | React app (JSX, transpiled in-browser) |
        | `styles.css` | App styling |

        Run the API with `uvicorn main:app --reload --port 8000`, then serve the
        frontend folder (e.g. `python -m http.server 5500`) and open it in a browser.
        See [`mlb_app/README.md`](mlb_app/README.md) for details.

        ## Architecture

        ```
        MLB Stats API
              │
              ▼
        mlb_load.py  (dlt)
              │  Loads raw data into SQL Server dw schema:
              │  games, teams, players, rosters, play_events,
              │  game_details, umpires, awards, drafts, transactions, ...
              ▼
        SQL Server — dw schema (raw tables)
              │
              ▼
        dbt_baseball/models/staging/     ← cast + clean raw columns
              │
              ▼
        dbt_baseball/models/dimensions/  ← dim_game, dim_player, dim_team,
              │                             dim_venue, dim_umpire, dim_season, ...
              ▼
        dbt_baseball/models/facts/       ← fact_play_events, fact_box_score, ...
              │
              ├── dbt test                ← schema tests, custom tests, dbt-utils
              │
              ▼
        mlb_test_output.py               ← renders dbt test results into reports
        ETLMonitor.py                    ← pipeline health monitoring + row counts
        lineage_builder.py               ← exports dbt lineage metadata
              │
              ▼
        mlb_app  (FastAPI + React)       ← browses the silver schema in a web UI
        ```

        ## Quick Start

        ```bash
        git clone https://github.com/keithwsmith/mlb_dlt.git
        cd mlb_dlt

        # ── 1. Configure environment ─────────────────────────────────
        copy .env.template .env        # Windows
        # cp .env.template .env        # macOS / Linux
        # Edit .env with your SQL Server credentials

        # ── 2. Load raw data (dlt) ───────────────────────────────────
        cd mlb_dlt
        pip install -r requirements.txt

        python mlb_load.py teams
        python mlb_load.py games --start-year 2024 --end-year 2024
        python mlb_load.py play_events --start-year 2024 --end-year 2024
        python mlb_load.py game_details --start-year 2024 --end-year 2024

        # ── 3. Transform with dbt ────────────────────────────────────
        cd ../dbt_baseball
        pip install dbt-sqlserver
        dbt deps                       # install dbt packages
        dbt seed                       # load static lookup CSVs
        dbt run                        # build all models
        dbt test                       # run all data quality tests

        # ── 4. Review results ────────────────────────────────────────
        cd ../mlb_test_output
        python mlb_test_output.py      # render dbt test report

        # ── 5. Browse the data (web app) ─────────────────────────────
        cd ../mlb_app
        pip install -r requirements.txt
        uvicorn main:app --reload --port 8000
        # in a second terminal, from the same folder:
        python -m http.server 5500
        # open http://localhost:5500
        ```

        ## Requirements

        - Python 3.10+
        - SQL Server with ODBC Driver 17 (or 18)
        - `pyodbc` + `sqlalchemy`
        - dlt — `pip install dlt[mssql]`
        - dbt-sqlserver — `pip install dbt-sqlserver`
        - dbt-utils — installed automatically via `dbt deps` from `packages.yml`

        ## Configuration

        All connection strings and runtime settings are controlled via environment variables.
        Copy `.env.template` to `.env` and fill in your values. **Never commit `.env` to Git.**

        See [`.env.template`](.env.template) for the full list of variables.

        For dbt specifically, update `dbt_baseball/profiles.yml` with your SQL Server host,
        database, and authentication method, or set `DBT_PROFILES_DIR` in your `.env` to
        point at the repo root.

        ## Repository Structure

        ```
        mlb_dlt/
        ├── mlb_dlt/                      # Core dlt loader (mlb_load.py)
        ├── mlb_orchestration/            # Pipeline orchestration (pipeline.py)
        ├── mlb_lineage/                  # dbt lineage metadata builder
        ├── mlb_latestgame_agent/         # Latest game check + incremental trigger
        ├── mlb_test_output/              # dbt test result reporter
        ├── ETLMonitor/                   # ETL health monitor
        ├── mlb_app/                      # FastAPI + React web app for browsing the warehouse
        ├── dbt_baseball/                 # dbt project
        │   ├── models/
        │   │   ├── staging/              # Staging models
        │   │   ├── dimensions/           # Dimension models + schema tests
        │   │   └── facts/                # Fact models
        │   ├── tests/                    # Custom data tests
        │   ├── macros/                   # Jinja macros
        │   ├── seeds/                    # Static CSV lookups
        │   ├── snapshots/                # SCD Type-2 snapshots
        │   ├── analyses/                 # Ad-hoc analytical SQL
        │   ├── dbt_project.yml           # Project config
        │   ├── packages.yml              # dbt package dependencies
        │   ├── profiles.yml              # SQL Server connection profiles
        │   ├── sources.yml               # Raw source definitions + freshness
        │   └── dimensions_schema.yml     # Dimension column docs + tests
        ├── .env.template                 # Environment variable template
        ├── .gitignore
        └── README.md
        ```

        ---
        *Generated 2026-07-26*
