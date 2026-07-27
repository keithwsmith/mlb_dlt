"""
Run this first: `python inspect_schema.py`

Prints the real columns for every table this app touches, so you can
compare against the [ASSUMED] guesses in queries.py and fix mismatches
in one pass instead of hitting them one 500 error at a time.
"""
from db import run_query

TABLES = [
    "silver.dim_season",
    "silver.dim_team",
    "silver.dim_games",
    "silver.dim_game_details",
    "silver.dim_game_umpires",
    "silver.fact_umpire_performance",
    "silver.dim_players",
    "silver.dim_venue",
    "silver.fact_at_bats",
    "silver.dim_draft",
    "silver.dim_award_recipient",
    "dw.mlbplayers",
]

SQL = """
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION
"""

if __name__ == "__main__":
    for full_name in TABLES:
        schema, table = full_name.split(".")
        cols = run_query(SQL, (schema, table))
        print(f"\n{full_name}")
        print("-" * len(full_name))
        if not cols:
            print("  (table not found, or you don't have access)")
            continue
        for c in cols:
            print(f"  {c['COLUMN_NAME']:<35} {c['DATA_TYPE']}")
