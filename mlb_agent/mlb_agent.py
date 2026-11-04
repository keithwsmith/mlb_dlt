import os
import pyodbc
from datetime import datetime, date
from typing import Optional
import requests
import json
import yaml

# ==========================================================
# MODULE OVERVIEW
# ==========================================================
# mlb_agent.py — post-pipeline QA & reporting agent for the MLB data warehouse
#
# WHAT THIS DOES
#   This module is a companion to a dlt/dbt pipeline (pipeline.py) that loads
#   MLB Stats API data into a SQL Server warehouse (schema `dw`, plus a `dbo.pipeline`
#   run-log table). After each pipeline run, `run_agent(run_id)` performs four
#   checks against the warehouse and writes one self-contained HTML report per
#   check to REPORTS_DIR. Where a check finds a problem, it calls the Claude
#   API (ask_claude) to generate a plain-English explanation and suggested fix,
#   which is embedded in the report.
#
# THE FOUR CHECKS (run in this order by run_agent)
#   1. New Field Detection (detect_new_fields / build_new_fields_report)
#      Compares the column list dlt *expects* per table (read from the dlt
#      schema export YAML at DLT_SCHEMA_FILE) against the columns that
#      *actually exist* in each dw.* table (read live via
#      INFORMATION_SCHEMA.COLUMNS). Flags:
#        - new_in_db     -> column exists in SQL Server but not in the dlt
#                           schema file (likely a new field added by the MLB
#                           API that dlt auto-loaded before its schema file
#                           caught up).
#        - missing_in_db -> column is in the dlt schema file but absent from
#                           SQL Server (possible failed load).
#      dlt's internal bookkeeping columns (_dlt_id, _dlt_load_id, etc.) and
#      internal tables (prefixed `_dlt`) are excluded from the comparison.
#      Claude is asked to suggest which dbt layer new fields belong in, a
#      SQL Server data type, and a schema.yml test stub; for missing fields,
#      a likely cause and next investigation step.
#
#   2. Data Validation (validate_data / build_validation_report)
#      Runs a fixed set of per-table SQL "NOT (<condition>)" checks defined
#      in VALIDATION_RULES (e.g. required keys not null, stat counts >= 0,
#      season within a sane range) and counts failing rows per rule.
#
#   3. Anomaly Detection (detect_anomalies / build_anomaly_report)
#      Runs the ad-hoc SQL queries in ANOMALY_QUERIES that surface data that
#      is technically valid but statistically suspicious — e.g. completed
#      games with no play-by-play rows, duplicate game_pk rows, games missing
#      umpire assignments, blowout/0-0 scorelines, or pipeline runs where the
#      row count went backwards. Each set of anomalous rows is sent to Claude
#      (get_claude_anomaly_analysis) for a short explanation/root-cause/fix.
#
#   4. Pipeline Summary (build_pipeline_summary_report)
#      Reads dbo.pipeline for the given run_id and renders a per-model table
#      (layer, status, row counts, records added, elapsed time, error) plus
#      an overall success/failure/skip tally. No Claude call — this is a
#      straight summary of the run log.
#
# OUTPUT
#   Each check produces one timestamped HTML file (styled via html_wrap) in
#   REPORTS_DIR: new_fields_<ts>.html, validation_<ts>.html,
#   anomalies_<ts>.html, pipeline_summary_<ts>.html.
#
# HOW IT'S INVOKED
#   Intended to be called from pipeline.py after the dlt ingest step (and/or
#   at the end of the run):
#       from mlb_agent import run_agent
#       run_agent(PIPELINE_RUN_ID)
#   Can also run standalone (`python mlb_agent.py`), which uses a generated
#   "STANDALONE_<timestamp>" run_id.
#
# EXTERNAL DEPENDENCIES / SIDE EFFECTS
#   - SQL Server via pyodbc, using the hardcoded DB_CONNECTION credentials
#     below (server 10.0.0.54, database `dlt`, sa login).
#   - Anthropic API (api.anthropic.com/v1/messages, model claude-sonnet-4-6)
#     via requests, authenticated with ANTHROPIC_API_KEY from the environment.
#     If that env var is unset, ask_claude() returns a placeholder string
#     instead of failing.
#   - Local filesystem: reads the dlt schema YAML at DLT_SCHEMA_FILE, writes
#     HTML reports to REPORTS_DIR (created if missing).
#
# SECURITY NOTE
#   DB_CONNECTION currently hardcodes a `sa` username and plaintext password
#   in source. Unlike the Anthropic key, this isn't pulled from an environment
#   variable/secret store, so anyone with read access to this file has DB
#   credentials. Worth moving to env vars or a secrets manager, consistent
#   with how ANTHROPIC_API_KEY is already handled.
# ==========================================================

# ==========================================================
# CONFIG — matches pipeline.py
# ==========================================================
DB_CONNECTION = {
    "driver":   "{ODBC Driver 17 for SQL Server}",
    "server":   "KEITH-PERSONAL",
    "database": "dlt",
    "uid":      "sa",
    "pwd":      "pass0123",
}

REPORTS_DIR       = r"C:\data\mlb\reports"
DLT_SCHEMA        = "dw"
DBT_SCHEMA        = "silver"
BASE_URL          = "https://statsapi.mlb.com/api/v1/"
ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# Path to dlt's saved schema JSON file.
# dlt writes this after every pipeline run.
DLT_SCHEMA_FILE = r"C:\Users\Keith\PycharmProjects\mlb_dlt\schemas\export\mlb_full_stats.schema.yaml"

# dlt internal metadata columns added to every table — not MLB data fields
DLT_META_COLUMNS = {
    "_dlt_id", "_dlt_load_id", "_dlt_root_id", "_dlt_parent_id",
    "_dlt_list_idx",
}


# ==========================================================
# DLT SCHEMA READER (Option 2 — schema JSON file)
# ==========================================================
def get_dlt_schema_columns() -> dict:
    """
    Read column names from dlt's saved schema yaml file.
    Returns dict of {table_name: set(column_names)} for all dw tables.
    This is the baseline — what dlt *expects* to be in each table.
    """
    if not os.path.exists(DLT_SCHEMA_FILE):
        print(f"[mlb_agent] WARNING: dlt schema file not found at {DLT_SCHEMA_FILE}")
        print( "[mlb_agent] Run the pipeline at least once to generate it.")
        return {}

    try:
        with open(DLT_SCHEMA_FILE, "r", encoding="utf-8") as f:
            schema = yaml.safe_load(f)

        columns_by_table = {}
        for table_name, table_def in schema.get("tables", {}).items():
            # Skip dlt internal tables (e.g. _dlt_loads, _dlt_pipeline_state)
            if table_name.startswith("_dlt"):
                continue
            cols = {
                col.lower()
                for col in table_def.get("columns", {}).keys()
                if col.lower() not in DLT_META_COLUMNS
            }
            if cols:
                columns_by_table[table_name] = cols

        print(f"[mlb_agent] dlt schema loaded: {len(columns_by_table)} tables found")
        return columns_by_table

    except Exception as e:
        print(f"[mlb_agent] ERROR reading dlt schema file: {e}")
        return {}


def get_db_actual_columns() -> dict:
    """
    Read actual column names from dw.* tables in SQL Server.
    Returns dict of {table_name: set(column_names)}.
    This is the current state — what is *actually* in the database.
    """
    columns_by_table = {}
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TABLE_NAME, COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ?
            ORDER BY TABLE_NAME, ORDINAL_POSITION
        """, (DLT_SCHEMA,))

        for table, column in cursor.fetchall():
            col_lower = column.lower()
            if col_lower in DLT_META_COLUMNS:
                continue
            if table not in columns_by_table:
                columns_by_table[table] = set()
            columns_by_table[table].add(col_lower)

        conn.close()
        print(f"[mlb_agent] DB actual columns loaded: {len(columns_by_table)} tables found")
    except Exception as e:
        print(f"[mlb_agent] ERROR reading actual DB columns: {e}")

    return columns_by_table

# Validation rules per dw table
VALIDATION_RULES = {
    "games": [
        ("game_pk IS NOT NULL",          "NULL game_pk found in games"),
        ("game_date IS NOT NULL",         "NULL game_date found in games"),
        ("season BETWEEN 1876 AND 2030",  "Season out of valid range in games"),
        ("teams__away__team__id IS NOT NULL",  "NULL teams__away__team__id found in games"),
        ("teams__home__team__id IS NOT NULL",  "NULL teams__home__team__id found in games"),
        ("season BETWEEN 1876 AND 2030",  "Season out of valid range in games"),
    ],
    "game_details": [
        ("game_pk IS NOT NULL",                        "NULL game_pk in game_details"),
        ("home_runs >= 0 OR home_runs IS NULL",        "Negative home runs in game_details"),
        ("away_runs >= 0 OR away_runs IS NULL",        "Negative away runs in game_details"),
        ("game_duration_minutes BETWEEN 1 AND 600 OR game_duration_minutes IS NULL in game_details",
         "game_duration_minutes out of range (1-600)"),
        ("attendance >= 0 OR attendance IS NULL",      "Negative attendance in game_details"),
        ("home_hits >= 0 OR home_hits IS NULL",        "Negative home hits in game_details"),
        ("away_hits >= 0 OR away_hits IS NULL",        "Negative away hits in game_details"),
    ],
    "play_events": [
        ("game_pk IS NOT NULL",       "NULL game_pk in play_events"),
        ("batter_id IS NOT NULL",     "NULL batter_id in play_events"),
        ("pitcher_id IS NOT NULL",    "NULL pitcher_id in play_events"),
    ],
    "player_transactions": [
        ("transaction_id IS NOT NULL", "NULL transaction_id"),
        ("date IS NOT NULL",           "NULL transaction date"),
    ],
    "umpires": [
        ("game_pk IS NOT NULL",       "NULL game_pk in umpires"),
        ("official_id IS NOT NULL",   "NULL official_id in umpires"),
    ],
    "rosters": [
        ("person__id IS NOT NULL",    "NULL person__id in rosters"),
        ("season IS NOT NULL",        "NULL season in rosters"),
    ],
    "draft": [
        ("person__id IS NOT NULL",    "NULL person__id in draft"),
        ("pick_round IS NOT NULL",    "NULL pick_round in draft"),
        ("pick_number IS NOT NULL",   "NULL pick_number in draft"),
    ],
    "award_recipients": [
        ("award_id IS NOT NULL",      "NULL award_id in award_recipients"),
        ("award_name IS NOT NULL",    "NULL award_name in award_recipients"),
        ("award_date IS NOT NULL",    "NULL award_date in award_recipients"),
        ("season IS NOT NULL",        "NULL season in award_recipients"),
        ("player_id IS NOT NULL",     "NULL player_id in award_recipients"),
    ],
}

# Anomaly detection queries — returns rows when something looks wrong
ANOMALY_QUERIES = {
    "games_with_no_play_events": """
        SELECT TOP 20 g.game_pk, g.game_date, g.season
        FROM dw.games g
        LEFT JOIN dw.play_events pe ON pe.game_pk = g.game_pk
        WHERE pe.game_pk IS NULL
          AND g.status__abstract_game_state = 'Final'
          AND g.season >= YEAR(GETDATE()) - 1
    """,
    "duplicate_game_pks_in_details": """
        SELECT game_pk, COUNT(*) AS cnt
        FROM dw.game_details
        GROUP BY game_pk
        HAVING COUNT(*) > 1
    """,
    "games_no_umpires": """
        SELECT TOP 20 g.game_pk, g.game_date
        FROM dw.games g
        LEFT JOIN dw.umpires u ON u.game_pk = g.game_pk
        WHERE u.game_pk IS NULL
          AND g.game_type = 'R'
          AND g.season >= YEAR(GETDATE()) - 1
          AND g.status__detailed_state NOT IN ('Cancelled','Postponed')
    """,
    "extreme_game_scores": """
        SELECT game_pk, home_runs, away_runs
        FROM dw.game_details
        WHERE home_runs > 25 OR away_runs > 25
         -- OR (home_runs = 0 AND away_runs = 0) There are Some Ties for records w/no scores
    """,
    "pipeline_record_drops": """
        SELECT model_name, model_layer, records_added, start_record_count,
               end_record_count, start_time
        FROM dbo.pipeline
        WHERE records_added < 0
          AND start_time >= DATEADD(DAY, -7, GETDATE())
    """,
}


# ==========================================================
# HELPERS
# ==========================================================
def _get_connection() -> pyodbc.Connection:
    parts = [f"{k}={v}" for k, v in DB_CONNECTION.items()]
    return pyodbc.connect(";".join(parts), timeout=15)


def ensure_reports_dir():
    os.makedirs(REPORTS_DIR, exist_ok=True)


def report_path(name: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(REPORTS_DIR, f"{name}_{ts}.html")


def ask_claude(prompt: str) -> str:
    """Send a prompt to Claude and return the text response."""
    if not ANTHROPIC_API_KEY:
        return "(Claude API key not set — set ANTHROPIC_API_KEY env var)"
    try:
        resp = requests.post(
            ANTHROPIC_API_URL,
            headers={"Content-Type": "application/json",
                     "x-api-key": ANTHROPIC_API_KEY,
                     "anthropic-version": "2023-06-01"},
            json={
                "model": "claude-sonnet-4-6",
                "max_tokens": 1000,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        return data["content"][0]["text"]
    except Exception as e:
        return f"(Claude API error: {e})"


def html_wrap(title: str, body: str, run_id: str) -> str:
    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>{title}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 32px; color: #222; }}
    h1   {{ color: #1a3a5c; border-bottom: 2px solid #1a3a5c; padding-bottom: 8px; }}
    h2   {{ color: #2c5f8a; margin-top: 28px; }}
    h3   {{ color: #444; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 12px; }}
    th    {{ background: #1a3a5c; color: #fff; padding: 8px 12px; text-align: left; }}
    td    {{ padding: 7px 12px; border-bottom: 1px solid #ddd; }}
    tr:hover td {{ background: #f0f6ff; }}
    .ok      {{ color: #2a7a2a; font-weight: bold; }}
    .warn    {{ color: #b36b00; font-weight: bold; }}
    .fail    {{ color: #c0392b; font-weight: bold; }}
    .section {{ background: #f7f9fc; border-left: 4px solid #2c5f8a;
                padding: 12px 16px; margin: 16px 0; border-radius: 4px; }}
    .claude  {{ background: #eef6ee; border-left: 4px solid #2a7a2a;
                padding: 12px 16px; margin: 16px 0; border-radius: 4px;
                white-space: pre-wrap; font-size: 0.95em; }}
    .meta    {{ color: #666; font-size: 0.85em; margin-bottom: 24px; }}
  </style>
</head>
<body>
  <h1>⚾ MLB Pipeline — {title}</h1>
  <div class="meta">Run ID: {run_id} &nbsp;|&nbsp; Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
  {body}
</body>
</html>"""


# ==========================================================
# 1. NEW FIELD DETECTION
# ==========================================================
def detect_new_fields(run_id: str) -> dict:
    """
    Compare actual dw.* columns (from SQL Server) against the dlt schema
    JSON file baseline. Reports three categories per table:

      new_in_db     — columns in SQL Server but NOT in the dlt schema file
                      (MLB API added a new field; dlt auto-loaded it but
                       schema file hasn't caught up yet — investigate)

      missing_in_db — columns in the dlt schema file but NOT in SQL Server
                      (dlt knows about the field but it never landed in the DB
                       — possible load failure or the resource wasn't run)

      matched       — columns present in both (all good)
    """
    print("\n[mlb_agent] Detecting new fields (dlt schema vs actual DB)...")

    dlt_schema  = get_dlt_schema_columns()   # baseline from schema JSON
    db_actual   = get_db_actual_columns()    # current state from SQL Server

    if not dlt_schema:
        print("[mlb_agent] Cannot compare — dlt schema file unavailable.")
        return {}

    findings = {}
    all_tables = set(dlt_schema.keys()) | set(db_actual.keys())

    for table in sorted(all_tables):
        dlt_cols = dlt_schema.get(table, set())
        db_cols  = db_actual.get(table, set())

        new_in_db     = sorted(db_cols  - dlt_cols)   # in DB but not in schema
        missing_in_db = sorted(dlt_cols - db_cols)    # in schema but not in DB
        matched       = sorted(dlt_cols & db_cols)

        has_issues = bool(new_in_db or missing_in_db)

        if has_issues:
            print(f"  [{table}] new_in_db={len(new_in_db)}  "
                  f"missing_in_db={len(missing_in_db)}  matched={len(matched)}")
        else:
            print(f"  [{table}] OK — {len(matched)} columns matched")

        findings[table] = {
            "new_in_db":     new_in_db,
            "missing_in_db": missing_in_db,
            "matched":       matched,
            "has_issues":    has_issues,
        }

    return findings


def get_claude_field_suggestions(table: str, new_in_db: list, missing_in_db: list) -> str:
    """Ask Claude to suggest dbt model updates for field discrepancies."""
    prompt = f"""You are a dbt/SQL Server expert helping maintain an MLB data warehouse.

The dlt ingestion layer loads data into dw.{table} in SQL Server.
We compared the dlt schema file (baseline) against the actual SQL Server table and found discrepancies.

Columns in SQL Server but NOT in dlt schema file (new_in_db — MLB API may have added these):
{json.dumps(new_in_db, indent=2) if new_in_db else "None"}

Columns in dlt schema file but NOT in SQL Server (missing_in_db — may have failed to load):
{json.dumps(missing_in_db, indent=2) if missing_in_db else "None"}

For new_in_db fields, please suggest:
1. Which dbt model layer (staging, dimension, fact, or mart) each field belongs in
2. The appropriate SQL Server data type
3. A short dbt schema.yml test entry where relevant

For missing_in_db fields, please suggest:
1. The most likely reason the field didn't land in SQL Server
2. How to investigate or fix it

Keep your response concise and practical."""
    return ask_claude(prompt)


def build_new_fields_report(findings: dict, run_id: str) -> str:
    any_issues = any(v["has_issues"] for v in findings.values())

    if not findings:
        body = "<div class='section'><span class='warn'>⚠ dlt schema file not available — skipping field comparison.</span></div>"
        return html_wrap("New Field Detection", body, run_id)

    if not any_issues:
        total = sum(len(v["matched"]) for v in findings.values())
        body = f"<div class='section'><span class='ok'>✓ All columns match between dlt schema and SQL Server ({total} total columns across {len(findings)} tables).</span></div>"
        return html_wrap("New Field Detection", body, run_id)

    sections = []
    for table, data in findings.items():
        new_in_db     = data["new_in_db"]
        missing_in_db = data["missing_in_db"]
        matched       = data["matched"]

        if not data["has_issues"]:
            sections.append(f"""
            <h2>dw.{table} <span style="color:#2a7a2a;font-size:0.85em">✓ OK ({len(matched)} columns)</span></h2>""")
            continue

        suggestion = get_claude_field_suggestions(table, new_in_db, missing_in_db)

        new_rows = "".join(
            f"<tr><td>{c}</td><td class='warn'>In DB only — not in dlt schema</td></tr>"
            for c in new_in_db
        )
        missing_rows = "".join(
            f"<tr><td>{c}</td><td class='fail'>In dlt schema only — missing from DB</td></tr>"
            for c in missing_in_db
        )

        sections.append(f"""
        <h2>dw.{table}</h2>
        <table>
          <tr><th>Column</th><th>Status</th></tr>
          {new_rows}
          {missing_rows}
        </table>
        <p style="color:#666;font-size:0.85em">
          {len(new_in_db)} new in DB &nbsp;|&nbsp;
          {len(missing_in_db)} missing from DB &nbsp;|&nbsp;
          {len(matched)} matched
        </p>
        <h3>Claude's Suggested Actions</h3>
        <div class="claude">{suggestion}</div>
        """)

    body = "\n".join(sections)
    return html_wrap("New Field Detection", body, run_id)


# ==========================================================
# 2. DATA VALIDATION
# ==========================================================
def validate_data(run_id: str) -> dict:
    """Run validation rules against each dw.* table."""
    print("\n[mlb_agent] Validating incoming data...")
    results = {}

    try:
        conn = _get_connection()
        cursor = conn.cursor()

        for table, rules in VALIDATION_RULES.items():
            table_results = []
            for condition, label in rules:
                try:
                    sql = f"""
                        SELECT COUNT(*) 
                        FROM {DLT_SCHEMA}.{table}
                        WHERE NOT ({condition})
                    """
                    cursor.execute(sql)
                    fail_count = cursor.fetchone()[0]
                    status = "PASS" if fail_count == 0 else "FAIL"
                    if fail_count > 0:
                        print(f"  [{table}] FAIL: {label} — {fail_count} rows")
                    table_results.append({
                        "rule":       condition,
                        "label":      label,
                        "fail_count": fail_count,
                        "status":     status,
                    })
                except Exception as e:
                    print(f"  [{table}] Rule error ({label}): {e}")
                    table_results.append({
                        "rule":       condition,
                        "label":      label,
                        "fail_count": -1,
                        "status":     "ERROR",
                    })
            results[table] = table_results

        conn.close()
    except Exception as e:
        print(f"[mlb_agent] DB connection error in validate_data: {e}")

    return results


def build_validation_report(results: dict, run_id: str) -> str:
    sections = []
    total_fails = 0

    for table, rules in results.items():
        rows_html = ""
        for r in rules:
            css = "ok" if r["status"] == "PASS" else ("fail" if r["status"] == "FAIL" else "warn")
            icon = "✓" if r["status"] == "PASS" else ("✗" if r["status"] == "FAIL" else "⚠")
            fail_display = r["fail_count"] if r["fail_count"] >= 0 else "ERROR"
            rows_html += f"""
            <tr>
              <td>{r['label']}</td>
              <td><code>{r['rule']}</code></td>
              <td class="{css}">{icon} {r['status']}</td>
              <td>{fail_display}</td>
            </tr>"""
            if r["fail_count"] > 0:
                total_fails += 1

        sections.append(f"""
        <h2>dw.{table}</h2>
        <table>
          <tr><th>Check</th><th>Condition</th><th>Status</th><th>Failing Rows</th></tr>
          {rows_html}
        </table>""")

    summary = f"<div class='section'>"
    if total_fails == 0:
        summary += "<span class='ok'>✓ All validation checks passed.</span>"
    else:
        summary += f"<span class='fail'>✗ {total_fails} validation check(s) failed — review below.</span>"
    summary += "</div>"

    body = summary + "\n".join(sections)
    return html_wrap("Data Validation", body, run_id)


# ==========================================================
# 3. ANOMALY DETECTION
# ==========================================================
def detect_anomalies(run_id: str) -> dict:
    """Run anomaly queries and return rows found."""
    print("\n[mlb_agent] Running anomaly detection...")
    anomalies = {}

    try:
        conn = _get_connection()
        cursor = conn.cursor()

        for name, sql in ANOMALY_QUERIES.items():
            try:
                cursor.execute(sql)
                rows = cursor.fetchall()
                cols = [d[0] for d in cursor.description]
                if rows:
                    print(f"  [anomaly] {name}: {len(rows)} issue(s) found")
                    anomalies[name] = {"columns": cols, "rows": [list(r) for r in rows]}
                else:
                    print(f"  [anomaly] {name}: OK")
            except Exception as e:
                print(f"  [anomaly] {name}: query error — {e}")

        conn.close()
    except Exception as e:
        print(f"[mlb_agent] DB connection error in detect_anomalies: {e}")

    return anomalies


def get_claude_anomaly_analysis(anomaly_name: str, columns: list, rows: list) -> str:
    """Ask Claude to explain and suggest fixes for an anomaly."""
    sample = rows[:10]
    prompt = f"""You are an MLB data warehouse expert.

The following anomaly was detected in the pipeline: "{anomaly_name}"

Columns: {columns}
Sample rows (up to 10): {json.dumps(sample, default=str)}

Please:
1. Briefly explain what this anomaly means in baseball data context
2. Suggest the most likely root cause
3. Recommend a specific fix or investigation step

Keep your response to 3-5 sentences."""
    return ask_claude(prompt)


def build_anomaly_report(anomalies: dict, run_id: str) -> str:
    if not anomalies:
        body = "<div class='section'><span class='ok'>✓ No anomalies detected.</span></div>"
        return html_wrap("Anomaly Detection", body, run_id)

    sections = []
    for name, data in anomalies.items():
        cols = data["columns"]
        rows = data["rows"]
        analysis = get_claude_anomaly_analysis(name, cols, rows)

        header = "".join(f"<th>{c}</th>" for c in cols)
        body_rows = ""
        for row in rows[:20]:
            body_rows += "<tr>" + "".join(f"<td>{v}</td>" for v in row) + "</tr>"

        sections.append(f"""
        <h2>⚠ {name.replace('_', ' ').title()}</h2>
        <p><span class="fail">{len(rows)} issue(s) found</span></p>
        <table>
          <tr>{header}</tr>
          {body_rows}
        </table>
        <h3>Claude's Analysis</h3>
        <div class="claude">{analysis}</div>
        """)

    body = "\n".join(sections)
    return html_wrap("Anomaly Detection", body, run_id)


# ==========================================================
# 4. PIPELINE SUMMARY REPORT
# ==========================================================
def build_pipeline_summary_report(run_id: str) -> str:
    """Query dbo.pipeline for this run and write an HTML summary report."""
    print("\n[mlb_agent] Building pipeline summary report...")

    query = """
        SELECT model_layer, model_name, status,
               start_record_count, end_record_count, records_added,
               elapsed_seconds, error_message
        FROM dbo.pipeline
        WHERE run_id = ?
        ORDER BY
            CASE model_layer
                WHEN 'ingest'        THEN 0
                WHEN 'dimension'     THEN 1
                WHEN 'fact'          THEN 2
                WHEN 'mart'          THEN 3
                WHEN 'test_analysis' THEN 4
                WHEN 'lineage'       THEN 5
                ELSE 6
            END, model_name
    """

    rows = []
    try:
        conn = _get_connection()
        cursor = conn.cursor()
        cursor.execute(query, run_id)
        rows = cursor.fetchall()
        conn.close()
    except Exception as e:
        print(f"[mlb_agent] Could not query pipeline table: {e}")

    if not rows:
        body = f"<div class='section'><span class='warn'>No pipeline entries found for run_id: {run_id}</span></div>"
        return html_wrap("Pipeline Summary", body, run_id)

    totals = {"SUCCESS": 0, "FAILURE": 0, "SKIPPED": 0}
    total_added = 0
    total_elapsed = 0.0
    table_rows = ""

    for r in rows:
        layer, name, status, s_cnt, e_cnt, added, elapsed, err = r
        css = "ok" if status == "SUCCESS" else ("fail" if status == "FAILURE" else "warn")
        icon = "✓" if status == "SUCCESS" else ("✗" if status == "FAILURE" else "—")
        totals[status] = totals.get(status, 0) + 1
        total_added += added or 0
        total_elapsed += elapsed or 0.0

        err_cell = f"<span class='fail' title='{str(err)[:200]}'>See log</span>" if err else ""
        table_rows += f"""
        <tr>
          <td>{layer}</td>
          <td>{name}</td>
          <td class="{css}">{icon} {status}</td>
          <td>{s_cnt if s_cnt is not None else ''}</td>
          <td>{e_cnt if e_cnt is not None else ''}</td>
          <td>{added if added is not None else ''}</td>
          <td>{f'{elapsed:.1f}' if elapsed is not None else ''}</td>
          <td>{err_cell}</td>
        </tr>"""

    summary_color = "ok" if totals.get("FAILURE", 0) == 0 else "fail"
    summary_icon  = "✓" if totals.get("FAILURE", 0) == 0 else "✗"

    body = f"""
    <div class="section">
      <span class="{summary_color}">{summary_icon}
        Models: {len(rows)} &nbsp;|&nbsp;
        Success: {totals.get('SUCCESS',0)} &nbsp;|&nbsp;
        Failed: {totals.get('FAILURE',0)} &nbsp;|&nbsp;
        Skipped: {totals.get('SKIPPED',0)} &nbsp;|&nbsp;
        Records Added: {total_added:,} &nbsp;|&nbsp;
        Total Time: {total_elapsed:.1f}s
      </span>
    </div>
    <table>
      <tr>
        <th>Layer</th><th>Model</th><th>Status</th>
        <th>Start Rows</th><th>End Rows</th><th>Added</th>
        <th>Elapsed (s)</th><th>Errors</th>
      </tr>
      {table_rows}
    </table>"""

    return html_wrap("Pipeline Summary", body, run_id)


# ==========================================================
# MAIN ENTRY POINT — call this from pipeline.py
# ==========================================================
def run_agent(run_id: str):
    """
    Run all agent checks and write HTML reports to REPORTS_DIR.
    Call this from pipeline.py after DLT ingest and before dbt runs,
    or at the end of the pipeline for a full summary.

    Usage in pipeline.py:
        from mlb_agent import run_agent
        run_agent(PIPELINE_RUN_ID)
    """
    print(f"\n{'='*60}")
    print(f" MLB AGENT  (run_id: {run_id})")
    print(f"{'='*60}")

    ensure_reports_dir()

    # 1. New field detection
    new_fields = detect_new_fields(run_id)
    fields_html = build_new_fields_report(new_fields, run_id)
    fields_path = report_path("new_fields")
    with open(fields_path, "w", encoding="utf-8") as f:
        f.write(fields_html)
    print(f"[mlb_agent] New fields report → {fields_path}")

    # 2. Data validation
    validation = validate_data(run_id)
    val_html = build_validation_report(validation, run_id)
    val_path = report_path("validation")
    with open(val_path, "w", encoding="utf-8") as f:
        f.write(val_html)
    print(f"[mlb_agent] Validation report  → {val_path}")

    # 3. Anomaly detection
    anomalies = detect_anomalies(run_id)
    anom_html = build_anomaly_report(anomalies, run_id)
    anom_path = report_path("anomalies")
    with open(anom_path, "w", encoding="utf-8") as f:
        f.write(anom_html)
    print(f"[mlb_agent] Anomaly report     → {anom_path}")

    # 4. Pipeline summary
    summary_html = build_pipeline_summary_report(run_id)
    sum_path = report_path("pipeline_summary")
    with open(sum_path, "w", encoding="utf-8") as f:
        f.write(summary_html)
    print(f"[mlb_agent] Summary report     → {sum_path}")

    print(f"\n[mlb_agent] All reports written to {REPORTS_DIR}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    # Run standalone with a test run_id
    run_agent("STANDALONE_" + datetime.now().strftime("%Y%m%d_%H%M%S"))