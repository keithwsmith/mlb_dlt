# dbt_baseball

dbt project that transforms raw MLB Stats API data loaded by mlb_load.py into dimension and fact tables in SQL Server. Contains models, tests, sources, seeds, macros, and snapshots for the full MLB data warehouse.

## Structure

```
dbt_baseball/
├── models/          # SQL models (dimensions, facts, staging)
├── tests/           # Custom data tests
├── macros/          # Reusable Jinja macros
├── seeds/           # Static CSV lookup data
├── snapshots/       # SCD type-2 snapshots (if any)
├── dbt_project.yml  # Project configuration
└── profiles.yml     # Connection profiles (edit for your environment)
```

## Usage

```bash
# Install dbt
pip install dbt-sqlserver

# Run all models
dbt run

# Run tests
dbt test

# Generate + serve docs
dbt docs generate && dbt docs serve
```

## Setup

1. Copy `profiles.yml` and update the SQL Server connection details, or
   point `DBT_PROFILES_DIR` at the repo root in your `.env`.
2. Run `dbt deps` to install any packages listed in `packages.yml`.

## Part of

[mlb_dlt](https://github.com/keithwsmith/mlb_dlt.git) — MLB Stats API → SQL Server data warehouse

---
*Generated 2026-07-21*
