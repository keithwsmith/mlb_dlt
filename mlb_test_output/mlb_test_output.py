"""
dbt Test Inspector
==================
Loops through all tables in the test_results schema, identifies which ones
contain failing rows, compiles the underlying dbt test SQL, executes it,
and saves everything to a summary table called [test_sql].

Prerequisites:
    pip install pyodbc
    A working dbt project in the current directory (or set DBT_PROJECT_DIR)

Usage:
    python dbt_test_inspector.py

Environment variables (or edit the CONFIG section below):
    DB_SERVER   - SQL Server host
    DB_NAME     - Database name
    DB_SCHEMA   - Test results schema (default: silver_test_results)
    DB_DRIVER   - ODBC driver (default: ODBC Driver 17 for SQL Server)
    DB_TRUSTED  - Use Windows auth (default: yes). Set to "no" for SQL auth.
    DB_USER     - SQL auth username  (only when DB_TRUSTED=no)
    DB_PASSWORD - SQL auth password  (only when DB_TRUSTED=no)
"""

import os
import re
import json
import subprocess
import pyodbc
from datetime import datetime


# ──────────────────────────────────────────────────────────
# CONFIG — edit these or set matching environment variables
# ──────────────────────────────────────────────────────────
SERVER       = os.getenv("DB_SERVER",   "10.0.0.54")
DATABASE     = os.getenv("DB_NAME",     "dlt")
TEST_SCHEMA  = os.getenv("DB_SCHEMA",   "test_failures")
TARGET_SCHEMA = os.getenv("DB_TARGET",  "silver")
DRIVER       = os.getenv("DB_DRIVER",   "ODBC Driver 17 for SQL Server")
TRUSTED      = os.getenv("DB_TRUSTED",  "yes").lower() == "yes"
DB_USER      = os.getenv("DB_USER",     "sa")
DB_PASSWORD  = os.getenv("DB_PASSWORD", "pass0123")

# Schema + table where results are written
OUTPUT_SCHEMA = os.getenv("OUTPUT_SCHEMA", TARGET_SCHEMA)
OUTPUT_TABLE  = "test_sql"

# dbt project directory (default: current directory)
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "C:\\Users\\Keith\\baseball-sql\\DBT_BASEBALL_SQLSERVER")


# ──────────────────────────────────────────────────────────
# DATABASE CONNECTION
# ──────────────────────────────────────────────────────────
def get_connection():
    if TRUSTED:
        conn_str = (
            f"DRIVER={{{DRIVER}}};"
            f"SERVER={SERVER};"
            f"DATABASE={DATABASE};"
            f"Trusted_Connection=yes;"
        )
    else:
        conn_str = (
            f"DRIVER={{{DRIVER}}};"
            f"SERVER={SERVER};"
            f"DATABASE={DATABASE};"
            f"UID={DB_USER};"
            f"PWD={DB_PASSWORD};"
        )
    return pyodbc.connect(conn_str)


# ──────────────────────────────────────────────────────────
# STEP 1: Get all tables in the test_results schema
# ──────────────────────────────────────────────────────────
def get_test_tables(cursor):
    cursor.execute("""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ?
          AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
    """, TEST_SCHEMA)
    return [row.TABLE_NAME for row in cursor.fetchall()]


# ──────────────────────────────────────────────────────────
# STEP 2: Check which tables have records
# ──────────────────────────────────────────────────────────
def table_has_records(cursor, table_name):
    cursor.execute(f"""
        SELECT TOP 1 1
        FROM [{TEST_SCHEMA}].[{table_name}]
    """)
    return cursor.fetchone() is not None


def get_record_count(cursor, table_name):
    cursor.execute(f"""
        SELECT COUNT(*) AS cnt
        FROM [{TEST_SCHEMA}].[{table_name}]
    """)
    return cursor.fetchone().cnt


# ──────────────────────────────────────────────────────────
# STEP 3: Parse test name and model name from table name
# ──────────────────────────────────────────────────────────
def parse_test_info(table_name):
    """
    dbt test table names follow patterns like:
        unique_dim_award_award_id
        not_null_dim_game_details_game_pk
        accepted_values_dim_challenges_has_review__1
        relationships_dim_challenges_game_pk__game_pk__ref_stg_play_events_

    Returns (test_type, model_name, test_select_name)
    """
    # Common test prefixes
    test_prefixes = [
        "unique",
        "not_null",
        "accepted_values",
        "relationships",
        "dbt_utils_accepted_range",
        "dbt_utils_unique_combination_of_columns",
    ]

    test_type = "unknown"
    model_name = "unknown"

    for prefix in test_prefixes:
        if table_name.startswith(prefix + "_"):
            test_type = prefix
            remainder = table_name[len(prefix) + 1:]
            # Model name is typically the next 2 segments (e.g., dim_award)
            parts = remainder.split("_")
            if len(parts) >= 2:
                model_name = f"{parts[0]}_{parts[1]}"
            break

    # The full table name IS the dbt test select name
    test_select_name = table_name

    return test_type, model_name, test_select_name


# ──────────────────────────────────────────────────────────
# STEP 4: Run dbt compile to get the test SQL
# ──────────────────────────────────────────────────────────
def dbt_compile_test(test_select_name):
    """
    Runs: dbt compile --select <test_name>
    Returns the compiled SQL string.
    """
    try:
        result = subprocess.run(
            ["dbt", "compile", "--select", test_select_name],
            capture_output=True,
            text=True,
            cwd=DBT_PROJECT_DIR,
            timeout=120,
        )

        if result.returncode != 0:
            print(f"  [WARN] dbt compile failed for {test_select_name}")
            print(f"         stderr: {result.stderr[:500]}")
            return None

        # Try to extract SQL from the compiled file in target/compiled/
        compiled_sql = find_compiled_sql(test_select_name)
        if compiled_sql:
            return compiled_sql

        # Fallback: parse from stdout
        return parse_compiled_sql_from_stdout(result.stdout)

    except subprocess.TimeoutExpired:
        print(f"  [WARN] dbt compile timed out for {test_select_name}")
        return None
    except FileNotFoundError:
        print("  [ERROR] dbt command not found. Is dbt installed and on PATH?")
        return None


def find_compiled_sql(test_select_name):
    """
    Searches the target/compiled directory for the compiled test SQL file.
    """
    compiled_dir = os.path.join(DBT_PROJECT_DIR, "target", "compiled")
    if not os.path.exists(compiled_dir):
        return None

    for root, dirs, files in os.walk(compiled_dir):
        for f in files:
            # Match by test name (file name without extension)
            if f.replace(".sql", "") == test_select_name:
                filepath = os.path.join(root, f)
                with open(filepath, "r", encoding="utf-8") as fh:
                    return fh.read().strip()
    return None


def parse_compiled_sql_from_stdout(stdout):
    """
    Fallback: extract SQL from dbt compile stdout output.
    Looks for content between horizontal rules or after 'Compiled node'.
    """
    lines = stdout.split("\n")
    sql_lines = []
    capture = False

    for line in lines:
        if "select" in line.lower() and not capture:
            capture = True
        if capture:
            sql_lines.append(line)

    return "\n".join(sql_lines).strip() if sql_lines else None


# ──────────────────────────────────────────────────────────
# STEP 5: Execute the compiled SQL and capture results
# ──────────────────────────────────────────────────────────
def execute_test_sql(cursor, sql):
    """
    Executes the compiled test SQL and returns results as a JSON string.
    """
    try:
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()

        results = []
        for row in rows:
            row_dict = {}
            for i, col in enumerate(columns):
                val = row[i]
                # Convert non-serializable types to string
                if isinstance(val, (datetime,)):
                    val = val.isoformat()
                elif isinstance(val, bytes):
                    val = val.hex()
                row_dict[col] = val
            results.append(row_dict)

        return json.dumps(results, default=str, indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})


# ──────────────────────────────────────────────────────────
# STEP 6: Create output table and insert results
# ──────────────────────────────────────────────────────────
def build_delete_sql(table_name, sql_result_json, test_type):
    """
    Builds a DELETE statement targeting the source table using the _dlt_id
    values stored in sql_result (the JSON of failing rows from test_failures).

    For not_null and accepted_values tests: failing rows contain the full
    source row including _dlt_id, so we can DELETE directly by _dlt_id.

    For unique tests: sql_result contains unique_field + n_records, not
    full rows, so we fall back to a query that finds duplicates.

    Returns a SQL string or None if a delete cannot be built.
    """
    try:
        rows = json.loads(sql_result_json)
    except Exception:
        return None

    if not rows or isinstance(rows, dict):
        return None

    # Derive the target schema and table from the test table name.
    # Convention: not_null_stg_player_transactions_transaction_id
    #   → target table is dw.stg_player_transactions (or silver.stg_player_transactions)
    #   We extract the model name segment and look it up in TARGET_SCHEMA.
    test_prefixes = [
        "unique",
        "not_null",
        "accepted_values",
        "relationships",
        "dbt_utils_accepted_range",
        "dbt_utils_unique_combination_of_columns",
    ]
    model_part = table_name
    for prefix in test_prefixes:
        if table_name.startswith(prefix + "_"):
            model_part = table_name[len(prefix) + 1:]
            break

    # Model name = first two underscore-separated segments (e.g. stg_player)
    # but we need the full model name. Use the model_name parsed earlier —
    # passed in via table_name so we re-parse here.
    parts = model_part.split("_")
    if len(parts) >= 2:
        model_name = f"{parts[0]}_{parts[1]}"
    else:
        return None

    target_table = f"[{TARGET_SCHEMA}].[{model_name}]"

    # unique tests: rows have unique_field + n_records, no _dlt_id
    if test_type == "unique" or "_dlt_id" not in (rows[0] if rows else {}):
        unique_field = rows[0].get("unique_field") if rows else None
        if not unique_field:
            return None
        return (
            f"-- Removes duplicate rows keeping the MIN id per {unique_field}\n"
            f"DELETE FROM {target_table}\n"
            f"WHERE _dlt_id NOT IN (\n"
            f"    SELECT MIN(_dlt_id) FROM {target_table}\n"
            f"    GROUP BY {unique_field}\n"
            f");"
        )

    # All other tests: rows contain _dlt_id — build an IN-list delete
    dlt_ids = [str(r["_dlt_id"]) for r in rows if r.get("_dlt_id")]
    if not dlt_ids:
        return None

    id_list = ", ".join(f"\'{v}\'" for v in dlt_ids)
    return (
        f"-- Deletes {len(dlt_ids)} failing row(s) from {target_table}\n"
        f"-- Test: {table_name}\n"
        f"DELETE FROM {target_table}\n"
        f"WHERE _dlt_id IN ({id_list});"
    )


def create_output_table(cursor):
    cursor.execute(f"""
        IF NOT EXISTS (
            SELECT 1 FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{OUTPUT_SCHEMA}'
              AND TABLE_NAME = '{OUTPUT_TABLE}'
        )
        BEGIN
            CREATE TABLE [{OUTPUT_SCHEMA}].[{OUTPUT_TABLE}] (
                id              INT IDENTITY(1,1) PRIMARY KEY,
                table_name      NVARCHAR(500)     NOT NULL,
                model_name      NVARCHAR(500)     NOT NULL,
                test_name       NVARCHAR(500)     NOT NULL,
                test_type       NVARCHAR(200)     NULL,
                failure_count   INT               NULL,
                sql_statement   NVARCHAR(MAX)     NULL,
                sql_result      NVARCHAR(MAX)     NULL,
                run_at          DATETIME2         DEFAULT GETDATE(),
                delete_sql      NVARCHAR(MAX)     NULL
            )
        END
        ELSE
        BEGIN
            -- Add delete_sql column if upgrading from older schema
            IF NOT EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{OUTPUT_SCHEMA}'
                  AND TABLE_NAME   = '{OUTPUT_TABLE}'
                  AND COLUMN_NAME  = 'delete_sql'
            )
            BEGIN
                ALTER TABLE [{OUTPUT_SCHEMA}].[{OUTPUT_TABLE}]
                ADD delete_sql NVARCHAR(MAX) NULL
            END

            -- Truncate for fresh run
            TRUNCATE TABLE [{OUTPUT_SCHEMA}].[{OUTPUT_TABLE}]
        END
    """)
    cursor.commit()


def insert_result(cursor, table_name, model_name, test_name, test_type,
                  failure_count, sql_statement, sql_result, delete_sql=None):
    cursor.execute(f"""
        INSERT INTO [{OUTPUT_SCHEMA}].[{OUTPUT_TABLE}]
            (table_name, model_name, test_name, test_type,
             failure_count, sql_statement, sql_result, delete_sql)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, table_name, model_name, test_name, test_type,
         failure_count, sql_statement, sql_result, delete_sql)
    cursor.commit()


# ──────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("dbt Test Inspector")
    print("=" * 60)
    print(f"Server:        {SERVER}")
    print(f"Database:      {DATABASE}")
    print(f"Test Schema:   {TEST_SCHEMA}")
    print(f"Output:        [{OUTPUT_SCHEMA}].[{OUTPUT_TABLE}]")
    print(f"dbt Project:   {os.path.abspath(DBT_PROJECT_DIR)}")
    print("=" * 60)

    conn = get_connection()
    cursor = conn.cursor()

    # Step 1: Get all test tables
    print("\n[1/6] Fetching test result tables...")
    test_tables = get_test_tables(cursor)
    print(f"      Found {len(test_tables)} tables in [{TEST_SCHEMA}]")

    if not test_tables:
        print("      No tables found. Check your schema name and run dbt test first.")
        return

    # Step 2: Filter to tables with records
    print("\n[2/6] Checking for tables with failing records...")
    failing_tests = []
    for table in test_tables:
        if table_has_records(cursor, table):
            count = get_record_count(cursor, table)
            failing_tests.append((table, count))
            print(f"      FAIL  {table} ({count} rows)")
        else:
            print(f"      PASS  {table}")

    print(f"\n      {len(failing_tests)} of {len(test_tables)} tests have failures")

    if not failing_tests:
        print("\n      All tests are passing. Nothing to inspect.")
        return

    # Step 3: Create output table
    print("\n[3/6] Creating output table...")
    create_output_table(cursor)
    print(f"      [{OUTPUT_SCHEMA}].[{OUTPUT_TABLE}] ready")

    # Step 4-6: Compile, execute, and save for each failing test
    print("\n[4/6] Processing each failing test...\n")

    for i, (table_name, failure_count) in enumerate(failing_tests, 1):
        test_type, model_name, test_select_name = parse_test_info(table_name)

        print(f"  [{i}/{len(failing_tests)}] {table_name}")
        print(f"         Test type:  {test_type}")
        print(f"         Model:      {model_name}")
        print(f"         Failures:   {failure_count}")

        # Compile
        print(f"         Compiling...")
        compiled_sql = dbt_compile_test(test_select_name)

        if compiled_sql:
            print(f"         SQL length: {len(compiled_sql)} chars")

            # Execute the compiled SQL
            print(f"         Executing...")
            sql_result = execute_test_sql(cursor, compiled_sql)
            print(f"         Result length: {len(sql_result)} chars")
        else:
            compiled_sql = "COMPILATION FAILED"
            sql_result = json.dumps({"error": "Could not compile test SQL"})
            print(f"         [SKIP] Could not compile")

        # Build delete SQL from the failing _dlt_id values in sql_result
        delete_sql = build_delete_sql(table_name, sql_result, test_type)
        if delete_sql:
            print(f"         Delete SQL built ({len(delete_sql)} chars)")
        else:
            print(f"         Delete SQL: n/a (unique test or no _dlt_id)")

        # Save
        insert_result(
            cursor,
            table_name=table_name,
            model_name=model_name,
            test_name=test_select_name,
            test_type=test_type,
            failure_count=failure_count,
            sql_statement=compiled_sql,
            sql_result=sql_result,
            delete_sql=delete_sql,
        )
        print(f"         Saved.\n")

    # Summary
    print("=" * 60)
    print("COMPLETE")
    print(f"Results saved to [{OUTPUT_SCHEMA}].[{OUTPUT_TABLE}]")
    print(f"Query with:")
    print(f"  SELECT * FROM [{OUTPUT_SCHEMA}].[{OUTPUT_TABLE}] ORDER BY run_at")
    print("=" * 60)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()