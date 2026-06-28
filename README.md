        # mlb_dlt — MLB Stats API Data Warehouse

        A collection of Python tools that load, monitor, and analyse MLB data from the
        [MLB Stats API](https://statsapi.mlb.com) into a **SQL Server** data warehouse using
        [dlt](https://dlthub.com) and [dbt](https://www.getdbt.com).

        ## Projects

        | Folder | Script | Description |
        |--------|--------|-------------|
        | [`mlb_orchestration`](mlb_orchestration/) | pipeline.py | Orchestration pipeline that coordinates and sequences all MLB dlt loads |
| [`mlb_lineage`](mlb_lineage/) | lineage_builder.py | Builds and exports dbt lineage metadata for the MLB data warehouse |
| [`mlb_dlt`](mlb_dlt/) | mlb_load.py | Core dlt-based ELT loader that pulls data from the MLB Stats API and loads it into a SQL Server data warehouse |
| [`ETLMonitor`](ETLMonitor/) | ETLMonitor.py | Monitors ETL pipeline runs and reports on load status, row counts, and errors |
| [`mlb_test_output`](mlb_test_output/) | mlb_test_output.py | Renders dbt test results into a structured report for review |
| [`mlb_latestgame_agent`](mlb_latestgame_agent/) | check_latest_game.py | Agent that checks for the latest completed MLB game and triggers an incremental load if new games are available |

        ## Architecture

        ```
        MLB Stats API
              │
              ▼
        mlb_load.py  (dlt)          ← loads raw data into SQL Server
              │
              ▼
        SQL Server (dw schema)      ← raw tables: games, teams, players, ...
              │
              ▼
        dbt models                  ← transforms + tests (sources.yml)
              │
              ▼
        mlb_test_output.py          ← test result reports
        ETLMonitor.py               ← pipeline health monitoring
        lineage_builder.py          ← lineage metadata export
        ```

        ## Quick Start

        ```bash
        git clone https://github.com/keithwsmith/mlb_dlt.git
        cd mlb_dlt

        # Copy and configure environment variables
        copy .env.template .env
        # Edit .env with your SQL Server credentials

        # Install dependencies for the loader
        cd mlb_dlt
        pip install -r requirements.txt

        # Load all teams (historical, 1960–present)
        python mlb_load.py teams

        # Load games for a specific year
        python mlb_load.py games --start-year 2024 --end-year 2024
        ```

        ## Requirements

        - Python 3.10+
        - SQL Server with ODBC Driver 17 (or 18)
        - `pyodbc` + `sqlalchemy`
        - dlt (`pip install dlt[mssql]`)
        - dbt-sqlserver (`pip install dbt-sqlserver`)

        ## Configuration

        All connection strings and runtime settings are controlled via environment variables.
        Copy `.env.template` to `.env` and fill in your values. **Never commit `.env` to Git.**

        See [`.env.template`](.env.template) for the full list of variables.

        ## Repository Structure

        ```
        mlb_dlt/
        ├── mlb_dlt/                  # Core dlt loader
        ├── mlb_orchestration/        # Pipeline orchestration
        ├── mlb_lineage/              # dbt lineage builder
        ├── mlb_latestgame_agent/     # Latest game check agent
        ├── mlb_test_output/          # dbt test reporter
        ├── ETLMonitor/               # ETL health monitor
        ├── dbt_baseball/             # dbt project (models, tests, macros, seeds)
        ├── .env.template             # Environment variable template (edit → .env)
        ├── .gitignore
        └── README.md
        ```

        ---
        *Generated 2026-06-27*
