# SQL_ENV

`env.example` is an environment file used by many of the Python programs, so
that there are no hard-coded values. This file can be adapted to run in your
environment.

## Switching to another database

To switch your `.env` file from SQL Server to another database, update the
connection driver, the default port, and the connection string format, and make
sure the database driver is installed where the programs run. The Python
packages for that database also need to be installed.

| Update | SQL Server (now) | PostgreSQL (example) |
|--------|------------------|----------------------|
| Connection driver | `ODBC Driver 17 for SQL Server` | `psycopg2` |
| Default port | `1433` | `5432` |
| Connection string format | `DRIVER={...};SERVER=...;DATABASE=...` | `host=... port=5432 dbname=... user=...` |
| Python packages | `pip install pyodbc` | `pip install psycopg2-binary` |

> Keep your real `.env` out of Git. Only `env.example`, with placeholder values,
> belongs in the repository.

## Part of

[mlb_dlt](https://github.com/keithwsmith/mlb_dlt.git) — MLB Stats API → SQL Server data warehouse

---
*Generated 2026-11-24*
