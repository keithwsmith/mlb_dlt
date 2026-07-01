# mlb_orchestration

Orchestration pipeline that coordinates and sequences all MLB dlt loads.

## File

`pipeline.py`

## Usage

```bash
python pipeline.py [--start-year YYYY] [--end-year YYYY] [--resource RESOURCE]
```

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
| `DLT_PIPELINE_NAME` | dlt pipeline name |
| `DLT_DATASET_NAME` | Target schema name |

## Part of

[mlb_dlt](https://github.com/keithwsmith/mlb_dlt.git) — MLB Stats API → SQL Server data warehouse

---
*Generated 2026-06-30*
