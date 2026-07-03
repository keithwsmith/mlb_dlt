import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import os
import json
import pyodbc
from datetime import datetime, timedelta
from typing import List, Dict, Set, Optional, Tuple
import sys
sys.stdout.reconfigure(encoding='utf-8')
import sys
sys.path.append(r"C:\Users\Keith\PycharmProjects\mlb_agent")

from mlb_agent import run_agent

# ==========================================================
# CONFIG (FIX THESE FOR YOUR MACHINE)
# ==========================================================
DBT_PROJECT_DIR = r"C:\Users\Keith\baseball-sql\DBT_BASEBALL_SQLSERVER"
DBT_PROFILES_DIR = r"C:\Users\Keith\.dbt"
DBT_TARGET = "prod"
DBT_THREADS = 4

MAX_RETRIES = 1
RETRY_DELAY = 5  # seconds
PYTHON_EXE = r"C:\Users\Keith\PycharmProjects\mlb_dlt\.venv\Scripts\python.exe"
MAIN_SCRIPT = r"C:\Users\Keith\PycharmProjects\mlb_dlt\mlb_load.py"

# Test output inspector
TEST_OUTPUT_SCRIPT = r"C:\Users\Keith\PycharmProjects\mlb_test_output\mlb_test_output.py"

# Lineage builder
LINEAGE_OUTPUT_SCRIPT = r"C:\Users\Keith\PycharmProjects\mlb_lineage\lineage_builder.py"

# ==========================================================
# DATABASE CONFIG — used for pipeline logging & record counts
# Match these to your dbt profiles.yml prod target
# ==========================================================
DB_CONNECTION = {
    "driver":   "{ODBC Driver 17 for SQL Server}",
    "server":   "10.0.0.54",
    "database": "dlt",
	"uid": "sa",
	"pwd": "pass0123"
    #"trusted_connection": "yes",
    # If using SQL auth instead, comment out trusted_connection and set:
    # "uid": "your_user",
    # "pwd": "your_password",
}


# Schema where dbt models land (adjust if needed)
DBT_SCHEMA = "silver"

# Schema where dlt raw tables land
DLT_SCHEMA = "dw"

# ==========================================================
# MODELS
# ==========================================================
DIMENSION_MODELS = [
    "stg_play_events", "stg_rosters","stg_player_transactions","dim_award", "dim_date", "dim_games", "dim_game_status", "dim_game_type",
    "dim_game_umpires","dim_challenges","dim_game_details","dim_pitch_type", "dim_player", "dim_position", "dim_school",
    "dim_season", "dim_team", "dim_venue", "dim_zone", "dim_draft", "dim_award_recipient"
]
intermediate = [
    "int_pitches_enriched"
]
MARTS = [
    "mart_batter_game", "mart_matchups", "mart_pitcher_arsenal", "mart_pitcher_game"
]

FACT_MODEL_DEPENDENCIES = {
    "fact_games": [
        "dim_date", "dim_team", "dim_venue",
        "dim_season", "dim_game_type", "dim_game_status",
    ],
    "fact_pitches": ["stg_play_events"],
    "fact_at_bats": ["stg_play_events"],
    "fact_player_stats": ["dim_player", "dim_team", "dim_season"],
    "fact_draft": ["dim_draft"],
    "fact_award_recipient": ["dim_award", "dim_award_recipient"],
    "fact_batted_balls": ["stg_play_events"],
    "int_pitches_enriched": [],
    "fact_umpire_performance": [
        "dim_challenges",       # challenge outcomes rolled up per game
        "dim_game_umpires",     # home-plate umpire → game_pk mapping
        "int_pitches_enriched", # called-strike-outside-zone pitch data
    ],
}

MART_DEPENDENCIES = {
    "mart_batter_game": ["int_pitches_enriched", "fact_batted_balls"],
    "mart_matchups": ["int_pitches_enriched", "fact_batted_balls"],
    "mart_pitcher_arsenal": ["int_pitches_enriched", "dim_pitch_type"],
    "mart_pitcher_game": ["int_pitches_enriched"],
}

# ==========================================================
# PIPELINE RUN ID — shared across the entire execution
# ==========================================================
PIPELINE_RUN_ID = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

# ==========================================================
# BUILD THE SET OF ALL MODELS THAT ARE A DEPENDENCY
# ==========================================================
def get_required_models() -> Set[str]:
    required = set()
    for deps in FACT_MODEL_DEPENDENCIES.values():
        required.update(deps)
    for deps in MART_DEPENDENCIES.values():
        required.update(deps)
    return required

REQUIRED_MODELS = get_required_models()

# ==========================================================
# DATABASE HELPERS
# ==========================================================
_db_lock = threading.Lock()


def _get_connection() -> pyodbc.Connection:
    parts = [f"{k}={v}" for k, v in DB_CONNECTION.items()]
    return pyodbc.connect(";".join(parts))


def ensure_pipeline_table():
    """Create the pipeline logging table if it doesn't exist."""
    ddl = """
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'pipeline'
    )
    BEGIN
        CREATE TABLE dbo.pipeline (
            id                  INT IDENTITY(1,1) PRIMARY KEY,
            run_id              NVARCHAR(50)   NOT NULL,
            model_name          NVARCHAR(200)  NOT NULL,
            model_layer         NVARCHAR(50)   NOT NULL,
            status              NVARCHAR(20)   NOT NULL,
            start_record_count  BIGINT         NULL,
            end_record_count    BIGINT         NULL,
            records_added       BIGINT         NULL,
            start_time          DATETIME2      NOT NULL,
            end_time            DATETIME2      NULL,
            elapsed_seconds     FLOAT          NULL,
            error_message       NVARCHAR(MAX)  NULL,
            created_at          DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
        );

        CREATE NONCLUSTERED INDEX ix_pipeline_run_id
            ON dbo.pipeline (run_id);
        CREATE NONCLUSTERED INDEX ix_pipeline_model
            ON dbo.pipeline (model_name, start_time DESC);
    END
    """
    with _db_lock:
        conn = _get_connection()
        try:
            conn.execute(ddl)
            conn.commit()
        finally:
            conn.close()


def get_record_count(table_name: str, schema: str = None) -> Optional[int]:
    schema = schema or DBT_SCHEMA
    query = "SELECT COUNT(*) FROM [{schema}].[{table}]".format(
        schema=schema, table=table_name
    )
    print('get_record_count:' + query )
    try:
        with _db_lock:
            conn = _get_connection()
            try:
                cursor = conn.execute(query)
                row = cursor.fetchone()
                return int(row[0]) if row and row[0] is not None else 0
            finally:
                conn.close()
    except Exception as e:
        print(f"  [record_count] Could not get count for {schema}.{table_name}: {e}")
        return None
		
		
def log_pipeline_event(
    model_name: str,
    model_layer: str,
    status: str,
    start_record_count: Optional[int],
    end_record_count: Optional[int],
    start_time: datetime,
    end_time: Optional[datetime],
    error_message: Optional[str] = None,
):
    """Insert a row into the pipeline table."""
    elapsed = None
    records_added = None

    if end_time and start_time:
        elapsed = round((end_time - start_time).total_seconds(), 2)

    if start_record_count is not None and end_record_count is not None:
        records_added = end_record_count - start_record_count

    insert_sql = """
        INSERT INTO dbo.pipeline (
            run_id, model_name, model_layer, status,
            start_record_count, end_record_count, records_added,
            start_time, end_time, elapsed_seconds, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    with _db_lock:
        conn = _get_connection()
        try:
            conn.execute(
                insert_sql,
                PIPELINE_RUN_ID,
                model_name,
                model_layer,
                status,
                start_record_count,
                end_record_count,
                records_added,
                start_time,
                end_time,
                elapsed,
                error_message[:4000] if error_message else None,
            )
            conn.commit()
        except Exception as e:
            print(f"  [pipeline_log] ERROR writing log for {model_name}: {e}")
        finally:
            conn.close()


# ==========================================================
# UTIL
# ==========================================================
def run_command(cmd: str, task_name: str, allow_fail=False, cwd=None,
                max_retries=None) -> Tuple[bool, str]:
    """Run a shell command with retries. Returns (success, stderr_on_failure).

    max_retries overrides the global MAX_RETRIES for this call.
    Pass max_retries=0 to run exactly once with no retries (used for dbt
    tests — test failures are informational and should never be retried).
    """
    retries = MAX_RETRIES if max_retries is None else max_retries
    last_stderr = ""
    for attempt in range(1, retries + 2):
        print(f"\n [{task_name}] Attempt {attempt}")
        print(f"CMD: {cmd}")

        result = subprocess.run(
            cmd,
            shell=True,
            cwd=cwd or DBT_PROJECT_DIR,
            capture_output=True,
            text=True,
        )

        print(result.stdout)

        if result.returncode == 0:
            print(f" [{task_name}] SUCCESS")
            return True, ""

        print(f" [{task_name}] FAILED")
        print(result.stderr)
        last_stderr = result.stderr

        if attempt <= retries:
            print(f" Retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
        else:
            if allow_fail:
                print(f" [{task_name}] continuing despite failure")
                return False, last_stderr
            raise RuntimeError(f"{task_name} failed after retries")

    return False, last_stderr


def dbt_cmd(action: str, extra: str = ""):
    return (
        f"dbt {action} "
        f"--profiles-dir {DBT_PROFILES_DIR} "
        f"--target {DBT_TARGET} "
        f"{extra}"
    )


# ==========================================================
# MODEL LAYER LABELS
# ==========================================================
def get_model_layer(model_name: str) -> str:
    if model_name.startswith("dim_") or model_name.startswith("stg_"):
        return "dimension"
    elif model_name.startswith("fact_"):
        return "fact"
    elif model_name.startswith("mart_"):
        return "mart"
    elif model_name.startswith("int_"):
        return "intermediate"
    return "other"


# ==========================================================
# STEPS
# ==========================================================
def run_dbt_deps():
    run_command(dbt_cmd("deps"), "dbt_deps")


def run_source_freshness():
    run_command(
        dbt_cmd("source freshness"),
        "source_freshness",
        allow_fail=True,
    )


# ----------------------------------------------------------
# SOURCE TESTS
# All tests defined in sources.yml under source: dw
# Runs after DLT ingest, before any dbt models are built,
# so failures surface raw-layer data quality issues early.
# ----------------------------------------------------------

# Every source table that has tests in sources.yml.
# Mirrors the tables listed in sources.yml under sources.dw.
SOURCE_TABLES = [
    "play_events",
    "player_transactions",
    "rosters",
    "umpires",
    "game_details",
    "mlbplayers",
    "teams",
    "venues",
    "seasons",
    "games",
    "player_stats",
    "draft",
    "award_recipients",
    "pitch_type",
    "Zones",
    "school_type_lookup",
]


def run_source_tests():
    """Run all dbt source tests defined in sources.yml for the dw source.

    Tests each source table individually so failures are logged per-table
    in the pipeline table and clearly visible in the summary.
    Tests are informational — failures do not block downstream model builds.
    """
    print("\n================ SOURCE TESTS ================")

    for table in SOURCE_TABLES:
        model_name = f"source_test_{table}"
        layer = "source_test"
        start_time = datetime.utcnow()
        start_count = get_record_count(table, schema=DLT_SCHEMA)

        success, stderr = run_command(
            dbt_cmd("test", f"--select source:dw.{table}"),
            model_name,
            allow_fail=True,
            max_retries=0,   # test failures are informational — never retry
        )

        end_time = datetime.utcnow()

        log_pipeline_event(
            model_name=model_name,
            model_layer=layer,
            status="SUCCESS" if success else "FAILURE",
            start_record_count=start_count,
            end_record_count=start_count,   # tests don't change row counts
            start_time=start_time,
            end_time=end_time,
            error_message=stderr if not success else None,
        )


# ----------------------------------------------------------
# RUN + LOG A SINGLE MODEL
# ----------------------------------------------------------
def run_model(model: str, allow_fail: bool = False) -> Tuple[str, bool]:
    """Run and test a single model, logging results to the pipeline table."""
    layer = get_model_layer(model)
    start_time = datetime.utcnow()
    start_count = get_record_count(model)

    success, stderr = run_command(
        dbt_cmd("run", f"--select {model} --threads {DBT_THREADS}"),
        f"run_{model}",
        allow_fail=allow_fail,
    )

    end_time = datetime.utcnow()
    end_count = get_record_count(model) if success else start_count

    status = "SUCCESS" if success else "FAILURE"
    error_msg = stderr if not success else None

    log_pipeline_event(
        model_name=model,
        model_layer=layer,
        status=status,
        start_record_count=start_count,
        end_record_count=end_count,
        start_time=start_time,
        end_time=end_time,
        error_message=error_msg,
    )

    if success:
        run_command(
            dbt_cmd("test", f"--select {model}"),
            f"test_{model}",
            allow_fail=True,
            max_retries=0,   # test failures are informational — never retry
        )

    return model, success


def skip_model(model: str, reason: str):
    """Log a skipped model to the pipeline table."""
    layer = get_model_layer(model)
    now = datetime.utcnow()
    count = get_record_count(model)

    log_pipeline_event(
        model_name=model,
        model_layer=layer,
        status="SKIPPED",
        start_record_count=count,
        end_record_count=count,
        start_time=now,
        end_time=now,
        error_message=reason,
    )


# ----------------------------------------------------------
# DIMENSIONS (parallel)
# ----------------------------------------------------------
def run_all_dimensions():
    print("\n================ DIMENSIONS ================")
    completed = set()
    failed = set()

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {}
        for m in DIMENSION_MODELS:
            can_fail = m not in REQUIRED_MODELS
            futures[executor.submit(run_model, m, allow_fail=can_fail)] = m

        for future in as_completed(futures):
            model, success = future.result()
            if success:
                completed.add(model)
            else:
                failed.add(model)
                print(f" [{model}] failed but is not a dependency — skipping")

    if failed:
        print(f"\n Dimension failures (non-blocking): {failed}")

    return completed


# ----------------------------------------------------------
# FACTS (dependency-aware parallel)
# ----------------------------------------------------------
def run_all_facts(dim_completed: set):
    print("\n================ FACTS ================")

    completed = set()
    failed = set()
    skipped = set()
    remaining = dict(FACT_MODEL_DEPENDENCIES)

    while remaining:
        ready = []
        blocked = []

        for model, deps in remaining.items():
            missing = [d for d in deps if d not in dim_completed and d not in completed]
            if not missing:
                ready.append(model)
            else:
                blocked.append((model, missing))

        if not ready:
            for model, missing in blocked:
                reason = f"Missing dependencies: {missing}"
                print(f" [{model}] SKIPPED — {reason}")
                skip_model(model, reason)
                skipped.add(model)
            break

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {}
            for m in ready:
                can_fail = m not in REQUIRED_MODELS
                futures[executor.submit(run_model, m, allow_fail=can_fail)] = m

            for future in as_completed(futures):
                model, success = future.result()
                del remaining[model]
                if success:
                    completed.add(model)
                else:
                    failed.add(model)
                    print(f" [{model}] failed — downstream models will be skipped")

    if failed:
        print(f"\n Fact failures: {failed}")
    if skipped:
        print(f" Fact skipped (unmet deps): {skipped}")

    return completed


# ----------------------------------------------------------
# MARTS (dependency-aware parallel)
# ----------------------------------------------------------
def run_all_marts(all_completed: set):
    print("\n================ MARTS ================")
    completed = set()
    failed = set()
    skipped = set()

    ready = []
    for model, deps in MART_DEPENDENCIES.items():
        missing = [d for d in deps if d not in all_completed]
        if missing:
            reason = f"Missing dependencies: {missing}"
            print(f" [{model}] SKIPPED — {reason}")
            skip_model(model, reason)
            skipped.add(model)
        else:
            ready.append(model)

    if ready:
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(run_model, m, allow_fail=True): m for m in ready}

            for future in as_completed(futures):
                model, success = future.result()
                if success:
                    completed.add(model)
                else:
                    failed.add(model)

    if failed:
        print(f"\n Mart failures (non-blocking): {failed}")
    if skipped:
        print(f" Mart skipped (unmet deps): {skipped}")

    return completed


# ----------------------------------------------------------
# DLT INGEST (with pipeline logging + record counts)
# ----------------------------------------------------------
def run_dlt(resource: str, extra_args: str = ""):
    """Run a dlt ingestion resource and log the result to the pipeline table.

    Counts rows in dw.<resource> before and after ingestion so the pipeline
    summary shows how many records each ingest added.
    """
    layer = "ingest"
    model_name = f"dlt_{resource}"
    start_time = datetime.utcnow()
    start_count = get_record_count(resource, schema=DLT_SCHEMA)

    cmd = f'"{PYTHON_EXE}" "{MAIN_SCRIPT}" {resource} {extra_args}'
    print("dlt command:" + cmd)

    success, stderr = run_command(
        cmd, f"dlt_{resource}", allow_fail=True, cwd=os.path.dirname(MAIN_SCRIPT)
    )

    end_time = datetime.utcnow()
    end_count = get_record_count(resource, schema=DLT_SCHEMA) if success else start_count

    log_pipeline_event(
        model_name=model_name,
        model_layer=layer,
        status="SUCCESS" if success else "FAILURE",
        start_record_count=start_count,
        end_record_count=end_count,
        start_time=start_time,
        end_time=end_time,
        error_message=stderr if not success else None,
    )

    if not success:
        raise RuntimeError(f"{model_name} failed after retries")

    return success, stderr


# ----------------------------------------------------------
# FINAL STEPS
# ----------------------------------------------------------
def run_all_tests():
    run_command(dbt_cmd("test"), "dbt_test_all", allow_fail=True,
               max_retries=0)  # test failures are informational — never retry


def check_test_results():
    """Run the test output inspector to analyze stored test failures.

    Executes the mlb_test_output project which scans the test_results
    schema, compiles failing test SQL, runs it, and saves results
    to the test_sql table for review.
    """
    print("\n================ CHECK TEST RESULTS ================")

    layer = "test_analysis"
    model_name = "check_test_results"
    start_time = datetime.utcnow()

    cmd = f'"{PYTHON_EXE}" "{TEST_OUTPUT_SCRIPT}"'
    print(f"test output command: {cmd}")

    success, stderr = run_command(
        cmd,
        "check_test_results",
        allow_fail=True,
        cwd=os.path.dirname(TEST_OUTPUT_SCRIPT),
        # No max_retries override here — this runs mlb_test_output.py
        # (a Python analysis script, not a dbt test), so normal retry
        # logic applies for transient DB connection errors.
    )

    end_time = datetime.utcnow()

    log_pipeline_event(
        model_name=model_name,
        model_layer=layer,
        status="SUCCESS" if success else "FAILURE",
        start_record_count=None,
        end_record_count=None,
        start_time=start_time,
        end_time=end_time,
        error_message=stderr if not success else None,
    )

    if not success:
        print(" [check_test_results] completed with errors — see logs for details")


def generate_docs():
    run_command(dbt_cmd("docs generate"), "dbt_docs", allow_fail=True)


def run_custom_audits():
    """Run the content-correctness audits (reconciliation + business-logic
    checks defined in macros/audits/custom_audits.sql) via dbt run-operation.

    Separate from run_all_tests()/check_test_results(): those exercise
    dbt's own generic/singular test framework (schema tests, store_failures
    into test_failures). This calls the custom macro-based audits directly
    — full-table reconciliation checks (e.g. pitch counts between
    fact_at_bats and fact_pitches) and business-logic consistency checks
    (e.g. is_no_hitter/is_shutout flags vs. actual runs/hits) that aren't
    expressible as ordinary dbt schema tests. Each check logs its own row
    into silver.test_sql (detail) and dbo.transformation_audit_log (summary,
    surfaced on ETLMonitor's Model Audit tab) as it runs.

    Requires fact_games, fact_pitches, fact_at_bats, and fact_batted_balls
    to already exist — must run after facts/marts, not before. allow_fail
    is True: an audit finding failures is expected/informational (that's
    the point of the check), not a reason to halt the pipeline. A non-zero
    exit here means the run-operation itself errored (e.g. a compile error
    in the macro), not that a check found bad data.
    """
    print("\n================ CUSTOM AUDITS ================")

    layer = "custom_audit"
    model_name = "run_custom_audits"
    start_time = datetime.utcnow()

    success, stderr = run_command(
        dbt_cmd("run-operation run_custom_audits"),
        "run_custom_audits",
        allow_fail=True,
        max_retries=0,   # same reasoning as dbt test — don't retry a check
    )

    end_time = datetime.utcnow()

    log_pipeline_event(
        model_name=model_name,
        model_layer=layer,
        status="SUCCESS" if success else "FAILURE",
        start_record_count=None,
        end_record_count=None,
        start_time=start_time,
        end_time=end_time,
        error_message=stderr if not success else None,
    )

    if not success:
        print(" [run_custom_audits] completed with errors — see logs for details")


def print_pipeline_summary():
    """Query the pipeline table and print a summary for this run."""
    query = """
        SELECT
            model_layer,
            model_name,
            status,
            start_record_count,
            end_record_count,
            records_added,
            elapsed_seconds
        FROM dbo.pipeline
        WHERE run_id = ?
        ORDER BY
            CASE model_layer
                WHEN 'ingest'        THEN 0
                WHEN 'source_test'   THEN 1
                WHEN 'dimension'     THEN 2
                WHEN 'fact'          THEN 3
                WHEN 'mart'          THEN 4
                WHEN 'test_analysis' THEN 5
                ELSE 6
            END,
            model_name
    """
    try:
        conn = _get_connection()
        cursor = conn.execute(query, PIPELINE_RUN_ID)
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            print("\n  No pipeline log entries found.")
            return

        print("\n" + "=" * 100)
        print(f" PIPELINE SUMMARY — Run {PIPELINE_RUN_ID}")
        print("=" * 100)
        header = (
            f"{'Layer':<15} {'Model':<28} {'Status':<10} "
            f"{'Start Rows':>12} {'End Rows':>12} {'Added':>10} {'Elapsed(s)':>12}"
        )
        print(header)
        print("-" * 100)

        totals = {"SUCCESS": 0, "FAILURE": 0, "SKIPPED": 0}
        total_added = 0
        total_elapsed = 0.0

        for r in rows:
            layer, name, status, s_cnt, e_cnt, added, elapsed = r
            totals[status] = totals.get(status, 0) + 1
            total_added += added or 0
            total_elapsed += elapsed or 0.0

            print(
                f"{layer:<15} {name:<28} {status:<10} "
                f"{(s_cnt if s_cnt is not None else ''):>12} "
                f"{(e_cnt if e_cnt is not None else ''):>12} "
                f"{(added if added is not None else ''):>10} "
                f"{(elapsed if elapsed is not None else ''):>12}"
            )

        print("-" * 100)
        print(
            f"{'TOTALS':<15} "
            f"{'':28} {'':10} "
            f"{'':>12} {'':>12} "
            f"{total_added:>10} {total_elapsed:>12.1f}"
        )
        print(
            f"\n  Models: {len(rows)}  |  "
            f"Success: {totals.get('SUCCESS',0)}  |  "
            f"Failed: {totals.get('FAILURE',0)}  |  "
            f"Skipped: {totals.get('SKIPPED',0)}"
        )
        print("=" * 100)

    except Exception as e:
        print(f"\n  Could not print pipeline summary: {e}")



# ==========================================================
# LINEAGE BUILDER
# ==========================================================
def run_lineage_builder():
    """Run lineage_builder.py as a subprocess to parse all dbt SQL models
    and upsert column-level lineage into dbo.model_lineage.

    Runs after all models have been built so the lineage always reflects
    the current state of the project.
    """
    print("\n================ LINEAGE BUILDER ================")

    layer      = "lineage"
    model_name = "lineage_builder"
    start_time = datetime.utcnow()

    cmd = f'"{PYTHON_EXE}" "{LINEAGE_OUTPUT_SCRIPT}"'
    print(f"lineage builder command: {cmd}")

    success, stderr = run_command(
        cmd,
        "lineage_builder",
        allow_fail=True,
        cwd=os.path.dirname(LINEAGE_OUTPUT_SCRIPT),
    )

    end_time = datetime.utcnow()

    log_pipeline_event(
        model_name=model_name,
        model_layer=layer,
        status="SUCCESS" if success else "FAILURE",
        start_record_count=None,
        end_record_count=None,
        start_time=start_time,
        end_time=end_time,
        error_message=stderr if not success else None,
    )

    if not success:
        print(" [lineage_builder] completed with errors — see logs for details")


# ==========================================================
# MAIN PIPELINE
# ==========================================================
def run_pipeline():
    pipeline_start = datetime.utcnow()
    print(f"\n START PIPELINE  (run_id: {PIPELINE_RUN_ID})")

    # Create the pipeline table if it doesn't exist
    ensure_pipeline_table()

    # ----------------------------
    # DLT INGEST
    # ----------------------------
    print("\n================ DLT INGEST ================")
    run_dlt("games")
    run_dlt("play_events")
    run_dlt("player_transactions")
    run_dlt("umpires")
    run_dlt("game_details")
    run_dlt("rosters")
    run_dlt("player_stats")

    run_dbt_deps()
    #run_source_freshness()
    run_source_tests()

    dim_completed = run_all_dimensions()
    fact_completed = run_all_facts(dim_completed)

    all_completed = dim_completed | fact_completed
    run_all_marts(all_completed)

    run_all_tests()
    check_test_results()
    run_custom_audits()
    generate_docs()

    pipeline_end = datetime.utcnow()
    pipeline_elapsed = (pipeline_end - pipeline_start).total_seconds()

    # Log a summary row for the full pipeline run
    log_pipeline_event(
        model_name="_pipeline_run",
        model_layer="pipeline",
        status="COMPLETE",
        start_record_count=None,
        end_record_count=None,
        start_time=pipeline_start,
        end_time=pipeline_end,
        error_message=None,
    )

    run_lineage_builder()
    run_agent(PIPELINE_RUN_ID)

    print_pipeline_summary()
    print(f"\n PIPELINE COMPLETE in {pipeline_elapsed:.1f}s")


# ==========================================================
# ENTRY
# ==========================================================
if __name__ == "__main__":
    run_pipeline()