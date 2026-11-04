# mlb-semantic-layer

Cube (cube.dev) semantic layer over the MLB data warehouse, plus cube_test_runner.py, which exercises every cube's measures, dimensions, and joins (including extends/role-playing cubes) against a running Cube instance.

## Running the tests

```bash
pip install -r requirements.txt
python cube_test_runner.py --cubes-dir model\cubes --env-file .env
```

Cube reads its settings (database connection, API secret) from a `.env`
file in this folder. It is not committed; see the `CUBEJS_*` entries in
[`.env.template`](../.env.template).

## Part of

[mlb_dlt](https://github.com/keithwsmith/mlb_dlt.git) — MLB Stats API → SQL Server data warehouse

---
*Generated 2026-11-03*
