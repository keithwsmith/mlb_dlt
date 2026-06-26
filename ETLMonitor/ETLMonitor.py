"""
ETL Monitoring & Notification Web Application
Monitors dbt artifacts and pipeline tables for the MLB data pipeline.

Changes vs prior version:
  - Zero-row load detection: models that succeed but load 0 rows are flagged
    with a distinct amber warning (separate from red range violations).
  - External alert endpoint: /api/alert returns a structured JSON payload for
    schedulers / webhook consumers. A lightweight built-in webhook dispatcher
    (POST to a configurable URL) keeps all notification logic outside the ETL
    itself, per ETL Best Practices (Get Your Email Out of My ETL).
  - PASS / RENAME lineage badges: improved contrast (sky-blue / amber, bold).
  - Auto-refresh removed: manual refresh only via navbar button.
  - Surrogate key / long expressions: shown in full scrollable <pre> block,
    no 180-character truncation.
"""

from flask import Flask, render_template_string, jsonify, request, redirect, url_for
import pyodbc
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
import json
import threading

app = Flask(__name__)

# ── DB CONNECTION ──────────────────────────────────────────────────────────────

def get_conn():
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=KEITH-PERSONAL;"
        "DATABASE=dlt;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def query(sql, params=None, fetchall=True):
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(sql, params or [])
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall() if fetchall else [cur.fetchone()]
            return [dict(zip(cols, r)) for r in rows if r]
    except Exception as e:
        print(f"  [query ERROR] {e}\n  SQL: {sql[:200]}")
        return []


def execute(sql, params=None):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params or [])
        conn.commit()


# ── SETUP: ensure tables exist ─────────────────────────────────────────────────

SETUP_SQL = """
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='pipeline_ranges'
)
BEGIN
    CREATE TABLE dbo.pipeline_ranges (
        id                       INT IDENTITY(1,1) PRIMARY KEY,
        model_name               NVARCHAR(200) NOT NULL UNIQUE,
        start_record_count_low   BIGINT,
        start_record_count_high  BIGINT,
        end_record_count_low     BIGINT,
        end_record_count_high    BIGINT,
        records_added_count_low  BIGINT,
        records_added_count_high BIGINT,
        elapsed_seconds_low      FLOAT,
        elapsed_seconds_high     FLOAT,
        updated_at               DATETIME2 DEFAULT SYSUTCDATETIME()
    )
END

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='pipeline_range_audit'
)
BEGIN
    CREATE TABLE dbo.pipeline_range_audit (
        id           INT IDENTITY(1,1) PRIMARY KEY,
        model_name   NVARCHAR(200) NOT NULL,
        changed_at   DATETIME2 DEFAULT SYSUTCDATETIME(),
        changed_by   NVARCHAR(100) DEFAULT SYSTEM_USER,
        field_name   NVARCHAR(100) NOT NULL,
        old_value    NVARCHAR(100),
        new_value    NVARCHAR(100)
    )
END

-- ── vw_lineage_with_tests: lineage joined to latest test results ────────────
-- Recreated on every startup so it always reflects the current schema.
-- Source: dbo.dbt_test_log (command_invocation_id, test_name, model_name,
--         column_name, status, failures, execution_time_seconds,
--         compiled_sql, run_started_at)
IF OBJECT_ID('dbo.vw_lineage_with_tests', 'V') IS NOT NULL
    DROP VIEW dbo.vw_lineage_with_tests
EXEC('
CREATE VIEW dbo.vw_lineage_with_tests AS
SELECT
    ml.model                                        AS model_name,
    ml.layer,
    ml.column_name,
    ml.source_table,
    ml.source_column,
    ml.transformation_type,
    ml.expression,
    ml.refreshed_at                                 AS lineage_refreshed_at,
    -- test coverage (NULL = no test defined for this column)
    ts.test_name,
    ts.failures,
    ts.status                                       AS test_status_raw,
    ts.execution_time_seconds,
    ts.compiled_sql,
    ts.run_started_at                               AS test_run_at,
    CASE
        WHEN ts.test_name IS NULL                        THEN ''no_test''
        WHEN ts.status IN (''fail'', ''error'')
          OR COALESCE(ts.failures, 0) > 0               THEN ''fail''
        ELSE                                                  ''pass''
    END                                             AS test_status
FROM dbo.model_lineage ml
LEFT JOIN (
    -- Most-recent result per model_name + column_name + test_name
    -- model_name in dbt_test_log is ''package.model'' (e.g. ''baseball_dw.dim_games'');
    -- strip everything up to and including the last ''.'' to get the bare model name.
    SELECT
        model_name,
        CASE
            WHEN CHARINDEX(''.'', model_name) > 0
            THEN SUBSTRING(model_name, LEN(model_name) - CHARINDEX(''.'', REVERSE(model_name)) + 2, 200)
            ELSE model_name
        END                         AS model_name_bare,
        column_name,
        test_name,
        status,
        failures,
        execution_time_seconds,
        compiled_sql,
        run_started_at
    FROM dbo.dbt_test_log dtl
    WHERE run_started_at = (
        SELECT MAX(run_started_at)
        FROM dbo.dbt_test_log dtl2
        WHERE dtl2.model_name  = dtl.model_name
          AND dtl2.test_name   = dtl.test_name
          AND dtl2.column_name = dtl.column_name
    )
) ts
    ON  LOWER(ts.model_name_bare) = LOWER(ml.model)
    AND LOWER(COALESCE(ts.column_name, '''')) = LOWER(COALESCE(ml.column_name, ''''))
')

-- ── alert_config: stores the webhook URL and last-dispatched state ──────────
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='alert_config'
)
BEGIN
    CREATE TABLE dbo.alert_config (
        id              INT IDENTITY(1,1) PRIMARY KEY,
        webhook_url     NVARCHAR(500),
        enabled         BIT NOT NULL DEFAULT 0,
        last_dispatched DATETIME2,
        last_health     NVARCHAR(20),
        updated_at      DATETIME2 DEFAULT SYSUTCDATETIME()
    )
    -- seed one row so there is always exactly one config record
    INSERT INTO dbo.alert_config (webhook_url, enabled) VALUES (NULL, 0)
END
"""

with app.app_context():
    try:
        execute(SETUP_SQL)
    except Exception as e:
        print(f"Setup warning: {e}")


# ── HELPERS ────────────────────────────────────────────────────────────────────

def seed_ranges_from_history():
    """Seed pipeline_ranges from 30-day averages where ranges don't exist yet."""
    seed_sql = """
    MERGE dbo.pipeline_ranges AS tgt
    USING (
        SELECT
            model_name,
            CAST(AVG(CAST(start_record_count AS FLOAT)) * 0.8 AS BIGINT) AS src_low,
            CAST(AVG(CAST(start_record_count AS FLOAT)) * 1.2 AS BIGINT) AS src_high,
            CAST(AVG(CAST(end_record_count   AS FLOAT)) * 0.8 AS BIGINT) AS end_low,
            CAST(AVG(CAST(end_record_count   AS FLOAT)) * 1.2 AS BIGINT) AS end_high,
            CAST(AVG(CAST(records_added      AS FLOAT)) * 0.8 AS BIGINT) AS add_low,
            CAST(AVG(CAST(records_added      AS FLOAT)) * 1.2 AS BIGINT) AS add_high,
            AVG(elapsed_seconds) * 0.5                                    AS elap_low,
            AVG(elapsed_seconds) * 2.0                                    AS elap_high
        FROM dbo.pipeline
        WHERE created_at >= DATEADD(day, -30, SYSUTCDATETIME())
          AND status = 'success'
        GROUP BY model_name
    ) AS src ON tgt.model_name = src.model_name
    WHEN NOT MATCHED THEN
        INSERT (model_name,
                start_record_count_low, start_record_count_high,
                end_record_count_low,   end_record_count_high,
                records_added_count_low,records_added_count_high,
                elapsed_seconds_low,    elapsed_seconds_high)
        VALUES (src.model_name,
                src.src_low, src.src_high,
                src.end_low, src.end_high,
                src.add_low, src.add_high,
                src.elap_low,src.elap_high);
    """
    execute(seed_sql)


# ── ZERO-ROW DETECTION ─────────────────────────────────────────────────────────

def check_zero_rows(run):
    """
    Return a list of zero-row warning strings for a successful pipeline run.
    Empty list = no zero-row issue.
    Only called for runs whose status is 'success'.
    """
    warnings = []
    end   = run.get('end_record_count')
    added = run.get('records_added')

    if end is not None and end == 0:
        warnings.append('end_record_count is 0 — destination table empty after load')
    elif added is not None and added == 0 and (end is None or end > 0):
        warnings.append('records_added is 0 — no new rows loaded this run')

    return warnings


def check_out_of_range(run):
    """Return list of fields that are out of range for a pipeline run."""
    rows = query("SELECT * FROM dbo.pipeline_ranges WHERE model_name = ?", [run['model_name']])
    if not rows:
        return []
    r = rows[0]
    issues = []

    def chk(val, lo, hi, label):
        if val is None:
            return
        if lo is not None and val < lo:
            issues.append(f"{label} {val} < min {lo}")
        if hi is not None and val > hi:
            issues.append(f"{label} {val} > max {hi}")

    chk(run.get('start_record_count'), r['start_record_count_low'],  r['start_record_count_high'],  'start_record_count')
    chk(run.get('end_record_count'),   r['end_record_count_low'],    r['end_record_count_high'],    'end_record_count')
    chk(run.get('records_added'),      r['records_added_count_low'], r['records_added_count_high'], 'records_added')
    chk(run.get('elapsed_seconds'),    r['elapsed_seconds_low'],     r['elapsed_seconds_high'],     'elapsed_seconds')
    return issues


def pipeline_status_summary():
    sql = """
    SELECT
        p.model_name,
        p.model_layer,
        p.status,
        p.start_record_count,
        p.end_record_count,
        p.records_added,
        p.elapsed_seconds,
        p.start_time,
        p.end_time,
        p.error_message,
        p.run_id
    FROM dbo.pipeline p
    INNER JOIN (
        SELECT model_name, MAX(id) AS max_id
        FROM dbo.pipeline
        GROUP BY model_name
    ) latest ON p.model_name = latest.model_name AND p.id = latest.max_id
    ORDER BY p.model_layer, p.model_name
    """
    runs = query(sql)
    for run in runs:
        run['issues']        = check_out_of_range(run)
        run['zero_warnings'] = (
            check_zero_rows(run)
            if run.get('status') == 'success' and not run['issues']
            else []
        )
        if run['status'] in ('failed', 'failure', 'error') or run['issues']:
            run['status_color'] = 'danger'
        elif run['zero_warnings']:
            run['status_color'] = 'warning'
        else:
            run['status_color'] = 'success'

        for k in ('start_time', 'end_time'):
            if run.get(k):
                run[k] = str(run[k])[:19]
    return runs


def failure_rates(days=7):
    sql = f"""
    SELECT
        model_name,
        COUNT(*) AS total_runs,
        SUM(CASE WHEN status IN ('failed','failure','error') THEN 1 ELSE 0 END) AS failures,
        CAST(
            100.0 * SUM(CASE WHEN status IN ('failed','failure','error') THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0)
        AS DECIMAL(5,2)) AS failure_rate_pct,
        MAX(CASE WHEN status IN ('failed','failure','error') THEN start_time END) AS last_failure
    FROM dbo.pipeline
    WHERE start_time >= DATEADD(day, -{days}, SYSUTCDATETIME())
    GROUP BY model_name
    ORDER BY failure_rate_pct DESC
    """
    rows = query(sql)
    for r in rows:
        if r.get('last_failure'):
            r['last_failure'] = str(r['last_failure'])[:19]
    return rows


def pipeline_history_last_run():
    sql = """
    SELECT
        p.id, p.run_id, p.model_name, p.model_layer, p.status,
        p.start_record_count, p.end_record_count, p.records_added,
        p.elapsed_seconds, p.start_time, p.end_time, p.error_message, p.created_at
    FROM dbo.pipeline p
    INNER JOIN (
        SELECT model_name, MAX(id) AS max_id
        FROM dbo.pipeline
        GROUP BY model_name
    ) latest ON p.model_name = latest.model_name AND p.id = latest.max_id
    ORDER BY p.model_layer, p.model_name
    """
    rows = query(sql)
    for r in rows:
        for k in ('start_time', 'end_time', 'created_at'):
            if r.get(k):
                r[k] = str(r[k])[:19]
    return rows


def pipeline_history(model_name=None, days=30):
    where = "WHERE start_time >= DATEADD(day, ?, SYSUTCDATETIME())"
    params = [-days]
    if model_name:
        where += " AND model_name = ?"
        params.append(model_name)
    sql = f"""
    SELECT TOP 200
        id, run_id, model_name, model_layer, status,
        start_record_count, end_record_count, records_added,
        elapsed_seconds, start_time, end_time, error_message, created_at
    FROM dbo.pipeline
    {where}
    ORDER BY start_time DESC
    """
    rows = query(sql, params)
    for r in rows:
        for k in ('start_time', 'end_time', 'created_at'):
            if r.get(k):
                r[k] = str(r[k])[:19]
    return rows


def _latest_invocation_id():
    try:
        rows = query(
            "SELECT TOP 1 command_invocation_id FROM silver.invocations ORDER BY run_started_at DESC"
        )
        return rows[0]['command_invocation_id'] if rows else None
    except Exception as e:
        print(f"  [latest_invocation_id] {e}")
        return None


def dbt_model_executions(invocation_id=None, limit=100):
    try:
        inv_id = invocation_id or _latest_invocation_id()
        if inv_id:
            sql = f"""
            SELECT TOP {limit}
                me.name, me.status, me.total_node_runtime, me.rows_affected,
                me.message, me.materialization, me.compile_started_at,
                me.query_completed_at, me.command_invocation_id
            FROM silver.model_executions me
            WHERE me.command_invocation_id = ?
            ORDER BY me.total_node_runtime DESC
            """
            rows = query(sql, [inv_id])
        else:
            rows = []
        if not rows:
            sql_fallback = f"""
            SELECT TOP {limit}
                me.name, me.status, me.total_node_runtime, me.rows_affected,
                me.message, me.materialization, me.compile_started_at,
                me.query_completed_at, me.command_invocation_id
            FROM silver.model_executions me
            WHERE me.command_invocation_id = (
                SELECT TOP 1 command_invocation_id
                FROM silver.model_executions
                ORDER BY compile_started_at DESC
            )
            ORDER BY me.total_node_runtime DESC
            """
            rows = query(sql_fallback)
        for r in rows:
            for k in ('compile_started_at', 'query_completed_at'):
                if r.get(k):
                    r[k] = str(r[k])[:19]
            r['status_color'] = 'danger' if r.get('status') in ('error', 'fail') else 'success'
        return rows
    except Exception as e:
        print(f"  [dbt_model_executions] {e}")
        return []


def dbt_test_executions(invocation_id=None, limit=200):
    try:
        inv_id = invocation_id or _latest_invocation_id()
        if inv_id:
            sql = f"""
            SELECT TOP {limit}
                COALESCE(t.name, te.node_id) AS test_name,
                te.status, te.failures, te.message, te.total_node_runtime,
                te.rows_affected, te.node_id, te.command_invocation_id
            FROM silver.test_executions te
            LEFT JOIN silver.tests t
                   ON te.command_invocation_id = t.command_invocation_id
                  AND te.node_id = t.node_id
            WHERE te.command_invocation_id = ?
            ORDER BY
                CASE WHEN te.status IN ('fail','error') THEN 0 ELSE 1 END,
                COALESCE(te.failures, 0) DESC,
                te.total_node_runtime DESC
            """
            rows = query(sql, [inv_id])
        else:
            rows = []
        if not rows:
            sql_fallback = f"""
            SELECT TOP {limit}
                COALESCE(t.name, te.node_id) AS test_name,
                te.status, te.failures, te.message, te.total_node_runtime,
                te.rows_affected, te.node_id, te.command_invocation_id
            FROM silver.test_executions te
            LEFT JOIN silver.tests t
                   ON te.command_invocation_id = t.command_invocation_id
                  AND te.node_id = t.node_id
            WHERE te.command_invocation_id = (
                SELECT TOP 1 command_invocation_id
                FROM silver.test_executions
                ORDER BY run_started_at DESC
            )
            ORDER BY
                CASE WHEN te.status IN ('fail','error') THEN 0 ELSE 1 END,
                COALESCE(te.failures, 0) DESC,
                te.total_node_runtime DESC
            """
            rows = query(sql_fallback)
        for r in rows:
            failures = r.get('failures') or 0
            status   = r.get('status') or ''
            r['status_color'] = (
                'danger' if status in ('fail', 'error') or failures > 0 else 'success'
            )
        return rows
    except Exception as e:
        print(f"  [dbt_test_executions] {e}")
        return []


def test_sql_results(limit=50):
    sql = f"""
    SELECT TOP {limit}
        id, table_name, model_name, test_name, test_type,
        failure_count, sql_statement, sql_result, run_at
    FROM silver.test_sql
    ORDER BY run_at DESC
    """
    rows = query(sql)
    for r in rows:
        if r.get('run_at'):
            r['run_at'] = str(r['run_at'])[:19]
    return rows


def lineage_summary():
    sql = """
    SELECT
        model, layer,
        COUNT(*) AS total_columns,
        SUM(CASE WHEN transformation_type = 'Pass-through'       THEN 1 ELSE 0 END) AS pass_through,
        SUM(CASE WHEN transformation_type = 'Rename'             THEN 1 ELSE 0 END) AS renames,
        SUM(CASE WHEN transformation_type = 'Surrogate Key'      THEN 1 ELSE 0 END) AS surrogate_keys,
        SUM(CASE WHEN transformation_type = 'CAST'               THEN 1 ELSE 0 END) AS casts,
        SUM(CASE WHEN transformation_type = 'TRY_CAST'           THEN 1 ELSE 0 END) AS try_casts,
        SUM(CASE WHEN transformation_type = 'CASE Statement'     THEN 1 ELSE 0 END) AS case_stmts,
        SUM(CASE WHEN transformation_type = 'String Cleansing'   THEN 1 ELSE 0 END) AS str_cleansing,
        SUM(CASE WHEN transformation_type = 'Derived Expression' THEN 1 ELSE 0 END) AS derived,
        MAX(refreshed_at) AS last_refreshed
    FROM dbo.model_lineage
    GROUP BY model, layer
    ORDER BY
        CASE layer
            WHEN 'Stage'        THEN 1
            WHEN 'Dimension'    THEN 2
            WHEN 'Intermediate' THEN 3
            WHEN 'Fact'         THEN 4
            WHEN 'Mart'         THEN 5
            ELSE 6
        END,
        model
    """
    rows = query(sql)
    for r in rows:
        if r.get('last_refreshed'):
            r['last_refreshed'] = str(r['last_refreshed'])[:19]
    return rows


def lineage_with_tests(model_name):
    """Return column lineage for a model joined to latest dbt_test_log results."""
    sql = """
    SELECT
        column_name,
        source_table,
        source_column,
        transformation_type,
        expression,
        test_name,
        failures,
        execution_time_seconds,
        compiled_sql,
        test_run_at,
        test_status
    FROM dbo.vw_lineage_with_tests
    WHERE LOWER(model_name) = LOWER(?)
    ORDER BY column_name
    """
    rows = query(sql, [model_name])
    for r in rows:
        if r.get('test_run_at'):
            r['test_run_at'] = str(r['test_run_at'])[:19]
    return rows


def get_pipeline_ranges():
    return query("SELECT * FROM dbo.pipeline_ranges ORDER BY model_name")


def overall_health():
    """
    Green  = all models succeeded, no range violations, no zero-row warnings
    Amber  = at least one zero-row warning but no hard failures
    Red    = at least one hard failure or range violation
    """
    runs = pipeline_status_summary()
    if not runs:
        return 'unknown'
    reds   = sum(1 for r in runs if r['status_color'] == 'danger')
    ambers = sum(1 for r in runs if r['status_color'] == 'warning')
    if reds > 0:
        return 'red'
    if ambers > 0:
        return 'amber'
    return 'green'


# ── EXTERNAL ALERTING ──────────────────────────────────────────────────────────

def get_alert_config():
    rows = query("SELECT TOP 1 * FROM dbo.alert_config ORDER BY id")
    return rows[0] if rows else {'webhook_url': None, 'enabled': 0, 'last_health': None}


def save_alert_config(webhook_url, enabled):
    execute(
        """
        UPDATE dbo.alert_config
        SET webhook_url = ?, enabled = ?, updated_at = SYSUTCDATETIME()
        WHERE id = (SELECT MIN(id) FROM dbo.alert_config)
        """,
        [webhook_url or None, 1 if enabled else 0]
    )


def build_alert_payload(runs, health):
    """Build the structured alert JSON payload consumed by external systems."""
    reds    = [r for r in runs if r['status_color'] == 'danger']
    ambers  = [r for r in runs if r['status_color'] == 'warning']
    return {
        'generated_at': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'health':       health,
        'total_models': len(runs),
        'red_count':    len(reds),
        'amber_count':  len(ambers),
        'failures': [
            {
                'model':   r['model_name'],
                'layer':   r.get('model_layer'),
                'status':  r['status'],
                'issues':  r['issues'],
                'error':   r.get('error_message'),
            }
            for r in reds
        ],
        'zero_row_warnings': [
            {
                'model':    r['model_name'],
                'layer':    r.get('model_layer'),
                'warnings': r['zero_warnings'],
            }
            for r in ambers
        ],
    }


def _dispatch_webhook(payload, webhook_url):
    """Fire-and-forget POST to the configured webhook URL."""
    try:
        body = json.dumps(payload).encode('utf-8')
        req  = urllib.request.Request(
            webhook_url,
            data=body,
            headers={'Content-Type': 'application/json', 'User-Agent': 'ETLMonitor/1.0'},
            method='POST',
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"  [webhook] dispatched → {webhook_url}  status={resp.status}")
        execute(
            """
            UPDATE dbo.alert_config
            SET last_dispatched = SYSUTCDATETIME(),
                last_health     = ?,
                updated_at      = SYSUTCDATETIME()
            WHERE id = (SELECT MIN(id) FROM dbo.alert_config)
            """,
            [payload['health']]
        )
    except Exception as e:
        print(f"  [webhook ERROR] {e}")


def maybe_dispatch_webhook(runs, health):
    """Dispatch a webhook POST only when health has changed and is non-green."""
    cfg = get_alert_config()
    if not cfg.get('enabled') or not cfg.get('webhook_url'):
        return
    last_health = cfg.get('last_health')
    if health == last_health:
        return
    payload = build_alert_payload(runs, health)
    t = threading.Thread(target=_dispatch_webhook, args=(payload, cfg['webhook_url']), daemon=True)
    t.start()


# ── HTML TEMPLATE ──────────────────────────────────────────────────────────────

TEMPLATE = r"""
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ETL Monitor ⚡</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.css" rel="stylesheet">
<style>
  body { background:#0f1117; color:#e0e0e0; font-family:'Segoe UI',sans-serif; }
  .navbar { background:#1a1d27 !important; border-bottom:1px solid #2d3142; }
  .navbar-brand { font-weight:700; color:#7c83fd !important; font-size:1.3rem; }
  .card  { background:#1e2130; border:1px solid #2d3142; border-radius:10px; }
  .card-header { background:#252839; border-bottom:1px solid #2d3142; font-weight:600; }
  .table  { color:#e0e0e0; }
  .table thead th { background:#252839; color:#9aa0b4; font-size:.8rem;
                    text-transform:uppercase; letter-spacing:.05em; }
  .table tbody tr:hover { background:#2a2d3e; }
  .badge-layer { background:#2d3142; color:#9aa0b4; font-size:.7rem; padding:2px 7px; border-radius:20px; }
  .health-dot  { width:18px; height:18px; border-radius:50%; display:inline-block; }
  .health-green   { background:#22c55e; box-shadow:0 0 8px #22c55e88; }
  .health-amber   { background:#f59e0b; box-shadow:0 0 8px #f59e0b88; }
  .health-red     { background:#ef4444; box-shadow:0 0 8px #ef444488; }
  .health-unknown { background:#6b7280; }
  .stat-card { background:#252839; border-radius:10px; padding:18px 22px; }
  .stat-val  { font-size:2rem; font-weight:700; }
  .nav-tabs .nav-link { color:#9aa0b4; border:none; padding:10px 18px; }
  .nav-tabs .nav-link.active { color:#7c83fd; border-bottom:2px solid #7c83fd; background:transparent; }
  .nav-tabs { border-bottom:1px solid #2d3142; }
  pre { background:#0f1117; color:#a0f0a0; padding:10px; border-radius:6px;
        font-size:.75rem; max-height:200px; overflow-y:auto; }
  .issue-badge { background:#ef444422; color:#ef4444; border:1px solid #ef444455;
                 border-radius:4px; font-size:.7rem; padding:1px 6px; margin:1px; display:inline-block; }
  .zero-badge  { background:#f59e0b22; color:#f59e0b; border:1px solid #f59e0b55;
                 border-radius:4px; font-size:.7rem; padding:1px 6px; margin:1px; display:inline-block; }
  .btn-sm-custom { font-size:.75rem; padding:3px 10px; }
  input[type=number],input[type=url],input[type=text] {
    background:#0f1117; border:1px solid #2d3142; color:#e0e0e0;
    border-radius:4px; padding:4px 8px; width:100%; }
  input[type=number]:focus,input[type=url]:focus,input[type=text]:focus {
    outline:none; border-color:#7c83fd; }
  .spinner-border-sm { width:1rem; height:1rem; }
  .history-summary-row:hover { background:#2a2d3e !important; }
  /* Lineage type badges */
  .lbadge { font-size:.68rem; padding:2px 7px; border-radius:4px; font-weight:600; white-space:nowrap; }
  /* CHANGED: PASS — was near-invisible dark grey; now sky-blue with border */
  .lb-pass  { background:#0c2d4a; color:#7dd3fc; font-weight:700; border:1px solid #1e4a6e; }
  /* CHANGED: RENAME — was low-contrast grey; now amber with border */
  .lb-ren   { background:#2e1f00; color:#fbbf24; font-weight:700; border:1px solid #5c3d00; }
  .lb-key   { background:#1e4838;   color:#4ade80; }
  .lb-cast  { background:#1e2f4a;   color:#60a5fa; }
  .lb-tcast { background:#1a3535;   color:#2dd4bf; }
  .lb-case  { background:#3b2e00;   color:#f59e0b; }
  .lb-str   { background:#2d1f4a;   color:#c084fc; }
  .lb-drv   { background:#3b1f00;   color:#fb923c; }
  .lineage-summary-row { cursor:pointer; }
  .lineage-summary-row:hover { background:#2a2d3e !important; }
  .lineage-detail-row { display:none; }
  .lineage-detail-row.open { display:table-row; }
  .filter-pill { background:#2d3142; color:#9aa0b4; border:1px solid #3d4256;
                 border-radius:20px; font-size:.75rem; padding:4px 12px; cursor:pointer;
                 transition:all .15s; }
  .filter-pill.active { background:#7c83fd22; color:#7c83fd; border-color:#7c83fd; }
  .alert-config-card { background:#1a1d27; border:1px solid #2d3142; border-radius:8px; padding:16px 20px; }
  .webhook-status-dot { width:10px; height:10px; border-radius:50%; display:inline-block; margin-right:6px; }
  .wdot-on  { background:#22c55e; box-shadow:0 0 6px #22c55e88; }
  .wdot-off { background:#6b7280; }
  @media(max-width:768px){ .stat-val{font-size:1.4rem;} }
</style>
</head>
<body>

<nav class="navbar navbar-expand-lg">
  <div class="container-fluid px-4">
    <span class="navbar-brand"><i class="bi bi-activity me-2"></i>ETL Monitor</span>
    <div class="d-flex align-items-center gap-3">
      <span class="health-dot health-{{ health }}" title="Overall: {{ health }}"></span>
      <span class="text-muted" style="font-size:.85rem">{{ health|upper }}</span>
      <a href="/dbt-docs" class="btn btn-outline-secondary btn-sm" target="_blank">
        <i class="bi bi-book me-1"></i>DBT Docs
      </a>
      <button class="btn btn-outline-primary btn-sm" onclick="seedRanges()">
        <i class="bi bi-magic me-1"></i>Auto-seed Ranges
      </button>
      <button class="btn btn-outline-light btn-sm" onclick="location.reload()">
        <i class="bi bi-arrow-clockwise"></i>
      </button>
    </div>
  </div>
</nav>

<div class="container-fluid px-4 py-3">

  <!-- STAT CARDS -->
  <div class="row g-3 mb-4">
    <div class="col-6 col-md-3">
      <div class="stat-card">
        <div class="text-muted" style="font-size:.8rem">TOTAL MODELS</div>
        <div class="stat-val text-white">{{ runs|length }}</div>
        <div style="font-size:.8rem;color:#9aa0b4;margin-top:4px">
          <i class="bi bi-layers me-1"></i>Latest run per model
        </div>
      </div>
    </div>
    <div class="col-6 col-md-3">
      <div class="stat-card">
        <div class="text-muted" style="font-size:.8rem">FAILURES</div>
        <div class="stat-val text-danger">{{ runs|selectattr('status_color','eq','danger')|list|length }}</div>
        <div style="font-size:.8rem;color:#9aa0b4;margin-top:4px">
          <i class="bi bi-x-circle me-1"></i>Failed or out-of-range
        </div>
      </div>
    </div>
    <div class="col-6 col-md-3">
      <div class="stat-card">
        <div class="text-muted" style="font-size:.8rem">ZERO-ROW WARNINGS</div>
        <div class="stat-val" style="color:#f59e0b">{{ runs|selectattr('status_color','eq','warning')|list|length }}</div>
        <div style="font-size:.8rem;color:#9aa0b4;margin-top:4px">
          <i class="bi bi-exclamation-triangle me-1"></i>Succeeded but loaded 0 rows
        </div>
      </div>
    </div>
    <div class="col-6 col-md-3">
      <div class="stat-card">
        <div class="text-muted" style="font-size:.8rem">PASSING</div>
        <div class="stat-val text-success">{{ runs|selectattr('status_color','eq','success')|list|length }}</div>
        <div style="font-size:.8rem;color:#9aa0b4;margin-top:4px">
          <i class="bi bi-check-circle me-1"></i>Within range &amp; succeeded
        </div>
      </div>
    </div>
  </div>

  <!-- TABS -->
  <ul class="nav nav-tabs mb-3" id="mainTabs">
    <li class="nav-item"><button class="nav-link active" data-bs-toggle="tab" data-bs-target="#tab-pipeline">
      <i class="bi bi-pipe me-1"></i>Pipeline</button></li>
    <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-history">
      <i class="bi bi-clock-history me-1"></i>History</button></li>
    <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-failures">
      <i class="bi bi-bar-chart me-1"></i>Failure Rates</button></li>
    <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-ranges">
      <i class="bi bi-sliders me-1"></i>Ranges</button></li>
    <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-lineage">
      <i class="bi bi-diagram-2 me-1"></i>Lineage
      {% if lineage %}<span class="badge rounded-pill ms-1" style="background:#7c83fd33;color:#7c83fd;font-size:.65rem">{{ lineage|length }}</span>{% endif %}
    </button></li>
    <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-lineage-tests">
      <i class="bi bi-diagram-2 me-1"></i>Lineage + Tests
    </button></li>
    <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-dbt">
      <i class="bi bi-diagram-3 me-1"></i>dbt Models</button></li>
    <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-tests">
      <i class="bi bi-check2-circle me-1"></i>dbt Tests</button></li>
    <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-testsql">
      <i class="bi bi-code-square me-1"></i>Test SQL</button></li>
    <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-alerts">
      <i class="bi bi-bell me-1"></i>Alerts
      {% if alert_cfg.enabled %}<span class="badge rounded-pill ms-1" style="background:#22c55e33;color:#22c55e;font-size:.65rem">ON</span>{% endif %}
    </button></li>
  </ul>

  <div class="tab-content">

    <!-- ── PIPELINE STATUS ── -->
    <div class="tab-pane fade show active" id="tab-pipeline">
      <div class="card">
        <div class="card-header d-flex justify-content-between">
          <span><i class="bi bi-pipe me-2"></i>Latest Pipeline Run per Model</span>
          <span class="text-muted" style="font-size:.8rem">{{ now }}</span>
        </div>
        <div class="card-body p-0">
          <div class="table-responsive">
          <table class="table table-sm mb-0" id="pipelineTable">
            <thead><tr>
              <th>Model</th><th>Layer</th><th>Status</th>
              <th>Start Rows</th><th>End Rows</th><th>Added</th>
              <th>Elapsed (s)</th><th>Started</th><th>Issues / Warnings</th><th></th>
            </tr></thead>
            <tbody>
            {% for r in runs %}
            <tr class="{{ 'table-danger' if r.status_color == 'danger' else ('table-warning bg-opacity-10' if r.status_color == 'warning' else '') }}"
                style="{{ 'background:rgba(245,158,11,.07)' if r.status_color == 'warning' else '' }}">
              <td>
                <strong>{{ r.model_name }}</strong><br>
                <small class="text-muted">{{ r.run_id }}</small>
              </td>
              <td><span class="badge-layer">{{ r.model_layer }}</span></td>
              <td>
                {% if r.status_color == 'danger' %}
                  <span class="badge bg-danger"><i class="bi bi-x-circle me-1"></i>{{ r.status }}</span>
                {% elif r.status_color == 'warning' %}
                  <span class="badge" style="background:#f59e0b;color:#000">
                    <i class="bi bi-exclamation-triangle me-1"></i>{{ r.status }}
                  </span>
                {% else %}
                  <span class="badge bg-success"><i class="bi bi-check-circle me-1"></i>{{ r.status }}</span>
                {% endif %}
              </td>
              <td>{{ '{:,}'.format(r.start_record_count) if r.start_record_count is not none else '—' }}</td>
              <td>{{ '{:,}'.format(r.end_record_count)   if r.end_record_count   is not none else '—' }}</td>
              <td>{{ '{:,}'.format(r.records_added)      if r.records_added      is not none else '—' }}</td>
              <td>{{ '%.1f'|format(r.elapsed_seconds)    if r.elapsed_seconds    is not none else '—' }}</td>
              <td><small>{{ r.start_time or '—' }}</small></td>
              <td>
                {% if r.error_message %}
                  <span class="issue-badge" title="{{ r.error_message }}">
                    <i class="bi bi-exclamation-triangle me-1"></i>error
                  </span>
                {% endif %}
                {% for iss in r.issues %}
                  <span class="issue-badge" style="cursor:pointer"
                        title="Click to view/edit range for {{ r.model_name }}"
                        onclick="openRangeForModel('{{ r.model_name }}')">
                    <i class="bi bi-sliders me-1"></i>{{ iss }}
                  </span>
                {% endfor %}
                {% for w in r.zero_warnings %}
                  <span class="zero-badge" title="Zero-row load detected — review if this model should always produce rows">
                    <i class="bi bi-exclamation-triangle me-1"></i>{{ w }}
                  </span>
                {% endfor %}
              </td>
              <td>
                <button class="btn btn-sm btn-sm-custom"
                        style="background:#7c83fd22;color:#7c83fd;border:1px solid #7c83fd44;"
                        onclick="openLineageForModel('{{ r.model_name }}')"
                        title="View column lineage for {{ r.model_name }}">
                  <i class="bi bi-diagram-2 me-1"></i>Lineage
                </button>
              </td>
            </tr>
            {% else %}
            <tr><td colspan="10" class="text-center text-muted py-4">No pipeline data found.</td></tr>
            {% endfor %}
            </tbody>
          </table>
          </div>
        </div>
      </div>
    </div>

    <!-- ── HISTORY ── -->
    <div class="tab-pane fade" id="tab-history">
      <div class="card">
        <div class="card-header d-flex justify-content-between align-items-center">
          <span><i class="bi bi-clock-history me-2"></i>Last Run per Model</span>
          <small class="text-muted"><i class="bi bi-hand-index me-1"></i>Click a row to see last 30 runs</small>
        </div>
        <div class="card-body p-0">
          <div class="table-responsive">
          <table class="table table-sm mb-0" id="historyTable">
            <thead><tr>
              <th></th>
              <th>Model</th><th>Layer</th><th>Status</th>
              <th>Start Rows</th><th>End Rows</th><th>Added</th>
              <th>Elapsed (s)</th><th>Last Run At</th><th>Error</th>
            </tr></thead>
            <tbody>
            {% for r in history %}
            <tr class="history-summary-row {{ 'table-danger' if r.status in ('failed','failure','error') else '' }}"
                style="cursor:pointer" data-model="{{ r.model_name }}"
                onclick="toggleModelHistory(this, '{{ r.model_name }}')">
              <td class="expand-icon text-muted" style="width:28px">
                <i class="bi bi-chevron-right" style="font-size:.75rem"></i>
              </td>
              <td><strong>{{ r.model_name }}</strong></td>
              <td><span class="badge-layer">{{ r.model_layer }}</span></td>
              <td>
                {% if r.status in ('failed','failure','error') %}
                  <span class="badge bg-danger">{{ r.status }}</span>
                {% else %}
                  <span class="badge bg-success">{{ r.status }}</span>
                {% endif %}
              </td>
              <td>{{ '{:,}'.format(r.start_record_count) if r.start_record_count is not none else '—' }}</td>
              <td>{{ '{:,}'.format(r.end_record_count)   if r.end_record_count   is not none else '—' }}</td>
              <td>{{ '{:,}'.format(r.records_added)      if r.records_added      is not none else '—' }}</td>
              <td>{{ '%.1f'|format(r.elapsed_seconds)    if r.elapsed_seconds    is not none else '—' }}</td>
              <td><small>{{ r.start_time or r.created_at or '—' }}</small></td>
              <td><small class="text-danger">{{ (r.error_message or '')[:60] }}</small></td>
            </tr>
            <tr class="history-detail-row d-none" id="detail-{{ r.model_name | replace(' ','_') | replace('.','_') }}">
              <td colspan="10" class="p-0">
                <div class="detail-inner" style="background:#12151f;border-top:1px solid #2d3142;border-bottom:2px solid #7c83fd33;">
                  <div class="p-2 text-muted" style="font-size:.78rem">
                    <i class="bi bi-hourglass-split me-1"></i>Loading last 30 runs…
                  </div>
                </div>
              </td>
            </tr>
            {% else %}
            <tr><td colspan="10" class="text-center text-muted py-4">No history data.</td></tr>
            {% endfor %}
            </tbody>
          </table>
          </div>
        </div>
      </div>
    </div>

    <!-- ── FAILURE RATES ── -->
    <div class="tab-pane fade" id="tab-failures">
      <div class="row g-3">
        <div class="col-md-6">
          <div class="card">
            <div class="card-header"><i class="bi bi-bar-chart me-2"></i>Last 7 Days</div>
            <div class="card-body p-0">
              <table class="table table-sm mb-0">
                <thead><tr><th>Model</th><th>Runs</th><th>Failures</th><th>Rate %</th><th>Last Failure</th></tr></thead>
                <tbody>
                {% for r in fr7 %}
                <tr class="{{ 'table-danger' if r.failure_rate_pct and r.failure_rate_pct > 0 else '' }}">
                  <td>{{ r.model_name }}</td>
                  <td>{{ r.total_runs }}</td>
                  <td>{{ r.failures }}</td>
                  <td>
                    <div class="d-flex align-items-center gap-2">
                      <div class="progress flex-grow-1" style="height:6px;background:#2d3142">
                        <div class="progress-bar bg-danger" style="width:{{ r.failure_rate_pct or 0 }}%"></div>
                      </div>
                      {{ r.failure_rate_pct or 0 }}%
                    </div>
                  </td>
                  <td><small class="{{ 'text-danger' if r.last_failure else 'text-muted' }}">{{ r.last_failure or '—' }}</small></td>
                </tr>
                {% else %}
                <tr><td colspan="5" class="text-center text-muted">No data</td></tr>
                {% endfor %}
                </tbody>
              </table>
            </div>
          </div>
        </div>
        <div class="col-md-6">
          <div class="card">
            <div class="card-header"><i class="bi bi-bar-chart me-2"></i>Last 30 Days</div>
            <div class="card-body p-0">
              <table class="table table-sm mb-0">
                <thead><tr><th>Model</th><th>Runs</th><th>Failures</th><th>Rate %</th><th>Last Failure</th></tr></thead>
                <tbody>
                {% for r in fr30 %}
                <tr class="{{ 'table-danger' if r.failure_rate_pct and r.failure_rate_pct > 0 else '' }}">
                  <td>{{ r.model_name }}</td>
                  <td>{{ r.total_runs }}</td>
                  <td>{{ r.failures }}</td>
                  <td>
                    <div class="d-flex align-items-center gap-2">
                      <div class="progress flex-grow-1" style="height:6px;background:#2d3142">
                        <div class="progress-bar bg-danger" style="width:{{ r.failure_rate_pct or 0 }}%"></div>
                      </div>
                      {{ r.failure_rate_pct or 0 }}%
                    </div>
                  </td>
                  <td><small class="{{ 'text-danger' if r.last_failure else 'text-muted' }}">{{ r.last_failure or '—' }}</small></td>
                </tr>
                {% else %}
                <tr><td colspan="5" class="text-center text-muted">No data</td></tr>
                {% endfor %}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- ── RANGES ── -->
    <div class="tab-pane fade" id="tab-ranges">
      <div class="card">
        <div class="card-header d-flex justify-content-between">
          <span><i class="bi bi-sliders me-2"></i>Pipeline Ranges</span>
          <small class="text-muted">Edit inline and click Save</small>
        </div>
        <div class="card-body p-0">
          <div class="table-responsive">
          <table class="table table-sm mb-0">
            <thead><tr>
              <th>Model</th>
              <th>Start Low</th><th>Start High</th>
              <th>End Low</th><th>End High</th>
              <th>Added Low</th><th>Added High</th>
              <th>Elapsed Low</th><th>Elapsed High</th>
              <th></th>
            </tr></thead>
            <tbody>
            {% for r in ranges %}
            <tr id="range-row-{{ r.id }}">
              <td><strong>{{ r.model_name }}</strong></td>
              <td><input type="number" value="{{ r.start_record_count_low  or '' }}" id="r{{ r.id }}_src_lo"></td>
              <td><input type="number" value="{{ r.start_record_count_high or '' }}" id="r{{ r.id }}_src_hi"></td>
              <td><input type="number" value="{{ r.end_record_count_low    or '' }}" id="r{{ r.id }}_end_lo"></td>
              <td><input type="number" value="{{ r.end_record_count_high   or '' }}" id="r{{ r.id }}_end_hi"></td>
              <td><input type="number" value="{{ r.records_added_count_low or '' }}" id="r{{ r.id }}_add_lo"></td>
              <td><input type="number" value="{{ r.records_added_count_high or '' }}" id="r{{ r.id }}_add_hi"></td>
              <td><input type="number" step="0.1" value="{{ r.elapsed_seconds_low  or '' }}" id="r{{ r.id }}_ela_lo"></td>
              <td><input type="number" step="0.1" value="{{ r.elapsed_seconds_high or '' }}" id="r{{ r.id }}_ela_hi"></td>
              <td>
                <button class="btn btn-success btn-sm btn-sm-custom"
                        onclick="saveRange({{ r.id }}, '{{ r.model_name }}')">
                  <i class="bi bi-save"></i> Save
                </button>
              </td>
            </tr>
            {% else %}
            <tr><td colspan="10" class="text-center text-muted py-4">
              No ranges defined. Click <strong>Auto-seed Ranges</strong> to populate from history.
            </td></tr>
            {% endfor %}
            </tbody>
          </table>
          </div>
        </div>
      </div>
    </div>

    <!-- ── LINEAGE ── -->
    <div class="tab-pane fade" id="tab-lineage">
      <div class="card">
        <div class="card-header d-flex justify-content-between align-items-center">
          <span><i class="bi bi-diagram-2 me-2"></i>Column Lineage — All Models</span>
          <small class="text-muted"><i class="bi bi-hand-index me-1"></i>Click a model row to see all columns</small>
        </div>
        <div class="px-3 pt-3 pb-2 d-flex flex-wrap gap-2 align-items-center" id="lineageFilters">
          <span style="font-size:.8rem;color:#9aa0b4">Filter by layer:</span>
          <span class="filter-pill active" onclick="filterLineage(this,'all')">All</span>
          <span class="filter-pill" onclick="filterLineage(this,'Stage')">Stage</span>
          <span class="filter-pill" onclick="filterLineage(this,'Dimension')">Dimension</span>
          <span class="filter-pill" onclick="filterLineage(this,'Intermediate')">Intermediate</span>
          <span class="filter-pill" onclick="filterLineage(this,'Fact')">Fact</span>
          <span class="filter-pill" onclick="filterLineage(this,'Mart')">Mart</span>
          <span class="ms-3" style="font-size:.8rem;color:#9aa0b4">Search:</span>
          <input type="text" id="lineageSearch" placeholder="model name…"
                 style="width:160px;" oninput="filterLineage(null,'__search__')">
        </div>
        <div class="card-body p-0">
          <div class="table-responsive">
          <table class="table table-sm mb-0" id="lineageTable">
            <thead><tr>
              <th style="width:28px"></th>
              <th>Model</th><th>Layer</th>
              <th class="text-center">Cols</th>
              <th class="text-center">Pass-thru</th><th class="text-center">Rename</th>
              <th class="text-center">Key</th><th class="text-center">CAST</th>
              <th class="text-center">TRY_CAST</th><th class="text-center">CASE</th>
              <th class="text-center">Str Clean</th><th class="text-center">Derived</th>
              <th style="font-size:.75rem;color:#9aa0b4">Last Refreshed</th>
            </tr></thead>
            <tbody id="lineageTbody">
            {% for r in lineage %}
            <tr class="lineage-summary-row"
                data-layer="{{ r.layer }}" data-model="{{ r.model }}"
                onclick="toggleLineageRow(this, '{{ r.model }}')">
              <td class="lineage-expand-icon text-muted" style="width:28px">
                <i class="bi bi-chevron-right" style="font-size:.75rem"></i>
              </td>
              <td><strong>{{ r.model }}</strong></td>
              <td><span class="badge-layer">{{ r.layer or '—' }}</span></td>
              <td class="text-center"><span class="badge bg-secondary">{{ r.total_columns }}</span></td>
              <td class="text-center">{% if r.pass_through %}<span class="lbadge lb-pass">{{ r.pass_through }}</span>{% else %}<span class="text-muted">·</span>{% endif %}</td>
              <td class="text-center">{% if r.renames %}<span class="lbadge lb-ren">{{ r.renames }}</span>{% else %}<span class="text-muted">·</span>{% endif %}</td>
              <td class="text-center">{% if r.surrogate_keys %}<span class="lbadge lb-key">{{ r.surrogate_keys }}</span>{% else %}<span class="text-muted">·</span>{% endif %}</td>
              <td class="text-center">{% if r.casts %}<span class="lbadge lb-cast">{{ r.casts }}</span>{% else %}<span class="text-muted">·</span>{% endif %}</td>
              <td class="text-center">{% if r.try_casts %}<span class="lbadge lb-tcast">{{ r.try_casts }}</span>{% else %}<span class="text-muted">·</span>{% endif %}</td>
              <td class="text-center">{% if r.case_stmts %}<span class="lbadge lb-case">{{ r.case_stmts }}</span>{% else %}<span class="text-muted">·</span>{% endif %}</td>
              <td class="text-center">{% if r.str_cleansing %}<span class="lbadge lb-str">{{ r.str_cleansing }}</span>{% else %}<span class="text-muted">·</span>{% endif %}</td>
              <td class="text-center">{% if r.derived %}<span class="lbadge lb-drv">{{ r.derived }}</span>{% else %}<span class="text-muted">·</span>{% endif %}</td>
              <td><small class="text-muted">{{ r.last_refreshed or '—' }}</small></td>
            </tr>
            <tr class="lineage-detail-row" id="ldetail-{{ r.model | replace(' ','_') | replace('.','_') }}">
              <td colspan="13" class="p-0">
                <div class="lineage-col-inner" style="background:#0d1016;border-top:1px solid #2d3142;border-bottom:2px solid #7c83fd33;">
                  <div class="p-2 text-muted" style="font-size:.78rem">
                    <i class="bi bi-hourglass-split me-1"></i>Loading columns…
                  </div>
                </div>
              </td>
            </tr>
            {% else %}
            <tr><td colspan="13" class="text-center text-muted py-4">
              No lineage data found. Run the pipeline to populate <code>dbo.model_lineage</code>.
            </td></tr>
            {% endfor %}
            </tbody>
          </table>
          </div>
        </div>
      </div>
    </div>

    <!-- ── LINEAGE + TESTS ── -->
    <div class="tab-pane fade" id="tab-lineage-tests">
      <div class="card">
        <div class="card-header d-flex justify-content-between align-items-center">
          <span><i class="bi bi-diagram-2 me-2"></i>Column Lineage with Test Coverage</span>
          <small class="text-muted"><i class="bi bi-hand-index me-1"></i>Select a model to inspect columns + test status</small>
        </div>
        <div class="px-3 pt-3 pb-2 d-flex flex-wrap gap-2 align-items-center">
          <span style="font-size:.8rem;color:#9aa0b4">Model:</span>
          <select id="ltModelSelect" onchange="loadLineageTests()"
                  style="background:#0f1117;border:1px solid #2d3142;color:#e0e0e0;border-radius:4px;padding:4px 10px;min-width:220px">
            <option value="">— select a model —</option>
            {% for r in lineage %}
            <option value="{{ r.model }}">{{ r.model }}</option>
            {% endfor %}
          </select>
          <span class="ms-3" style="font-size:.8rem;color:#9aa0b4">Filter:</span>
          <span class="filter-pill active"  onclick="filterLT(this,'all')"      id="ltf-all">All</span>
          <span class="filter-pill"         onclick="filterLT(this,'no_test')"  id="ltf-none">No Test</span>
          <span class="filter-pill"         onclick="filterLT(this,'pass')"     id="ltf-pass">Pass</span>
          <span class="filter-pill"         onclick="filterLT(this,'fail')"     id="ltf-fail">Fail</span>
        </div>
        <div class="card-body p-0">
          <div class="table-responsive">
          <table class="table table-sm mb-0" id="ltTable">
            <thead><tr>
              <th>#</th>
              <th>Column</th>
              <th>Source Table</th>
              <th>Source Column</th>
              <th>Transform Type</th>
              <th>Test Name</th>
              <th class="text-center">Failures</th>
              <th class="text-center">Exec (s)</th>
              <th class="text-center">Test Status</th>
              <th>Last Tested</th>
            </tr></thead>
            <tbody id="ltTbody">
              <tr><td colspan="10" class="text-center text-muted py-4">
                Select a model above to load lineage + test data.
              </td></tr>
            </tbody>
          </table>
          </div>
        </div>
        <!-- Coverage summary bar -->
        <div class="px-3 py-2" id="ltSummaryBar" style="display:none;border-top:1px solid #2d3142;font-size:.8rem;color:#9aa0b4">
          <span id="ltSummaryText"></span>
        </div>
      </div>
    </div>

    <!-- ── DBT MODELS ── -->
    <div class="tab-pane fade" id="tab-dbt">
      <div class="card">
        <div class="card-header"><i class="bi bi-diagram-3 me-2"></i>Latest dbt Model Executions</div>
        <div class="card-body p-0">
          <div class="table-responsive">
          <table class="table table-sm mb-0">
            <thead><tr>
              <th>Model</th><th>Status</th><th>Materialization</th>
              <th>Runtime (s)</th><th>Rows Affected</th>
              <th>Compiled</th><th>Completed</th><th>Message</th>
            </tr></thead>
            <tbody>
            {% for r in dbt_models %}
            <tr class="{{ 'table-danger' if r.status_color == 'danger' else '' }}">
              <td><strong>{{ r.name }}</strong></td>
              <td>{% if r.status_color == 'danger' %}<span class="badge bg-danger">{{ r.status }}</span>{% else %}<span class="badge bg-success">{{ r.status }}</span>{% endif %}</td>
              <td><span class="badge-layer">{{ r.materialization or '—' }}</span></td>
              <td>{{ '%.2f'|format(r.total_node_runtime) if r.total_node_runtime is not none else '—' }}</td>
              <td>{{ '{:,}'.format(r.rows_affected) if r.rows_affected is not none else '—' }}</td>
              <td><small>{{ r.compile_started_at or '—' }}</small></td>
              <td><small>{{ r.query_completed_at or '—' }}</small></td>
              <td><small class="text-muted">{{ (r.message or '')[:80] }}</small></td>
            </tr>
            {% else %}
            <tr><td colspan="8" class="text-center text-muted py-4">No dbt model executions found.</td></tr>
            {% endfor %}
            </tbody>
          </table>
          </div>
        </div>
      </div>
    </div>

    <!-- ── DBT TESTS ── -->
    <div class="tab-pane fade" id="tab-tests">
      <div class="card">
        <div class="card-header"><i class="bi bi-check2-circle me-2"></i>Latest dbt Test Executions</div>
        <div class="card-body p-0">
          <div class="table-responsive">
          <table class="table table-sm mb-0">
            <thead><tr>
              <th>Test</th><th>Status</th><th>Failures</th><th>Rows Affected</th><th>Runtime (s)</th><th>Message</th>
            </tr></thead>
            <tbody>
            {% for r in dbt_tests %}
            <tr class="{{ 'table-danger' if r.status_color == 'danger' else '' }}">
              <td>{{ r.test_name or r.node_id or '—' }}</td>
              <td>{% if r.status_color == 'danger' %}<span class="badge bg-danger">{{ r.status }}</span>{% elif r.status_color == 'warning' %}<span class="badge bg-warning text-dark">{{ r.status }}</span>{% else %}<span class="badge bg-success">{{ r.status }}</span>{% endif %}</td>
              <td>{{ r.failures if r.failures is not none else '—' }}</td>
              <td>{{ r.rows_affected if r.rows_affected is not none else '—' }}</td>
              <td>{{ '%.2f'|format(r.total_node_runtime) if r.total_node_runtime is not none else '—' }}</td>
              <td><small class="text-muted">{{ (r.message or '')[:100] }}</small></td>
            </tr>
            {% else %}
            <tr><td colspan="6" class="text-center text-muted py-4">No dbt test executions found.</td></tr>
            {% endfor %}
            </tbody>
          </table>
          </div>
        </div>
      </div>
    </div>

    <!-- ── TEST SQL ── -->
    <div class="tab-pane fade" id="tab-testsql">
      <div class="card">
        <div class="card-header"><i class="bi bi-code-square me-2"></i>Custom Test SQL Results</div>
        <div class="card-body p-0">
          <div class="table-responsive">
          <table class="table table-sm mb-0">
            <thead><tr>
              <th>Table</th><th>Model</th><th>Test</th><th>Type</th>
              <th>Failures</th><th>Run At</th><th>Result</th>
            </tr></thead>
            <tbody>
            {% for r in test_sql %}
            <tr class="{{ 'table-danger' if r.failure_count and r.failure_count > 0 else '' }}">
              <td>{{ r.table_name }}</td><td>{{ r.model_name }}</td>
              <td>{{ r.test_name }}</td>
              <td><span class="badge-layer">{{ r.test_type or '—' }}</span></td>
              <td>{% if r.failure_count and r.failure_count > 0 %}<span class="badge bg-danger">{{ r.failure_count }}</span>{% else %}<span class="badge bg-success">0</span>{% endif %}</td>
              <td><small>{{ r.run_at }}</small></td>
              <td>{% if r.sql_result %}<button class="btn btn-outline-secondary btn-sm btn-sm-custom" onclick="document.getElementById('res-{{ r.id }}').classList.toggle('d-none')">Show</button><pre id="res-{{ r.id }}" class="d-none mt-1">{{ r.sql_result[:500] }}</pre>{% endif %}</td>
            </tr>
            {% else %}
            <tr><td colspan="7" class="text-center text-muted py-4">No test SQL results found.</td></tr>
            {% endfor %}
            </tbody>
          </table>
          </div>
        </div>
      </div>
    </div>

    <!-- ── ALERTS TAB ── -->
    <div class="tab-pane fade" id="tab-alerts">
      <div class="row g-3">

        <div class="col-lg-5">
          <div class="card h-100">
            <div class="card-header"><i class="bi bi-bell me-2"></i>External Alert Configuration</div>
            <div class="card-body">

              <p class="text-muted" style="font-size:.85rem">
                ETL Monitor does <strong>not</strong> send email or SMS directly —
                notifications belong in the scheduler layer (per ETL Best Practices).
                Use one or both mechanisms below to wire up your existing tools.
              </p>

              <div class="alert-config-card mb-3">
                <div class="fw-600 mb-1" style="font-size:.9rem;color:#7c83fd">
                  <i class="bi bi-arrow-repeat me-1"></i>Mechanism 1 — Poll Endpoint
                </div>
                <p style="font-size:.8rem;color:#9aa0b4;margin-bottom:8px">
                  Schedule any tool (Task Scheduler, cron, Airflow) to GET this URL.
                  <code style="color:#f59e0b">health != "green"</code> is your trigger.
                </p>
                <div class="d-flex align-items-center gap-2">
                  <code style="background:#0f1117;border:1px solid #2d3142;border-radius:4px;padding:4px 10px;font-size:.8rem;color:#93c5fd;flex:1">
                    GET http://127.0.0.1:5050/api/alert
                  </code>
                  <button class="btn btn-sm btn-sm-custom" style="background:#7c83fd22;color:#7c83fd;border:1px solid #7c83fd44;white-space:nowrap"
                          onclick="testAlertEndpoint()">
                    <i class="bi bi-play me-1"></i>Test
                  </button>
                </div>
                <div id="alertEndpointResult" class="mt-2" style="font-size:.78rem;display:none"></div>
              </div>

              <div class="alert-config-card">
                <div class="fw-600 mb-1" style="font-size:.9rem;color:#7c83fd">
                  <i class="bi bi-send me-1"></i>Mechanism 2 — Webhook (POST on health change)
                </div>
                <p style="font-size:.8rem;color:#9aa0b4;margin-bottom:10px">
                  ETL Monitor POSTs JSON to this URL when health transitions (e.g. green→red).
                  Wire it to Teams, Slack, PagerDuty, or a custom script.
                  Fires once per transition, not on every refresh.
                </p>
                <div class="mb-2">
                  <label style="font-size:.8rem;color:#9aa0b4;margin-bottom:3px">Webhook URL</label>
                  <input type="url" id="webhookUrl" placeholder="https://hooks.example.com/etl-monitor"
                         value="{{ alert_cfg.webhook_url or '' }}">
                </div>
                <div class="d-flex align-items-center justify-content-between mt-2">
                  <div class="form-check form-switch mb-0">
                    <input class="form-check-input" type="checkbox" id="webhookEnabled"
                           {{ 'checked' if alert_cfg.enabled else '' }}>
                    <label class="form-check-label" for="webhookEnabled" style="font-size:.85rem;color:#e0e0e0">
                      Enable webhook
                    </label>
                  </div>
                  <div class="d-flex gap-2">
                    <button class="btn btn-sm btn-sm-custom" style="background:#f59e0b22;color:#f59e0b;border:1px solid #f59e0b44;"
                            onclick="testWebhook()">
                      <i class="bi bi-send me-1"></i>Send Test
                    </button>
                    <button class="btn btn-success btn-sm btn-sm-custom" onclick="saveAlertConfig()">
                      <i class="bi bi-save me-1"></i>Save
                    </button>
                  </div>
                </div>
                {% if alert_cfg.last_dispatched %}
                <div class="mt-2" style="font-size:.75rem;color:#9aa0b4">
                  <i class="bi bi-clock me-1"></i>Last dispatched: {{ alert_cfg.last_dispatched }}
                  · health was <strong>{{ alert_cfg.last_health or '?' }}</strong>
                </div>
                {% endif %}
              </div>

            </div>
          </div>
        </div>

        <div class="col-lg-7">
          <div class="card h-100">
            <div class="card-header d-flex justify-content-between align-items-center">
              <span><i class="bi bi-code me-2"></i>Current Alert Payload Preview</span>
              <small class="text-muted">What external systems receive from <code>/api/alert</code></small>
            </div>
            <div class="card-body p-0">
              <pre id="alertPayloadPre" style="margin:0;border-radius:0 0 10px 10px;max-height:420px">
Loading…</pre>
            </div>
          </div>
        </div>

      </div>

      <div class="card mt-3">
        <div class="card-header"><i class="bi bi-windows me-2"></i>Windows Task Scheduler — Quick Setup</div>
        <div class="card-body">
          <p class="text-muted" style="font-size:.85rem;margin-bottom:10px">
            Save the script below as <code>check_etl.ps1</code> and schedule it with Task Scheduler
            to run after each pipeline completion. It reads <code>/api/alert</code> and sends a
            Windows toast notification if health is non-green. No email config inside the ETL.
          </p>
          <pre style="max-height:none">
# check_etl.ps1  — scheduled by Task Scheduler, NOT embedded in ETL
$response = Invoke-RestMethod -Uri 'http://127.0.0.1:5050/api/alert' -Method Get
if ($response.health -ne 'green') {
    $reds   = $response.red_count
    $ambers = $response.amber_count
    $msg    = "ETL Monitor: $reds failure(s), $ambers zero-row warning(s). Health: $($response.health.ToUpper())"
    # Option A: Windows toast
    [Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType=WindowsRuntime] | Out-Null
    $template = [Windows.UI.Notifications.ToastTemplateType]::ToastText01
    $xml = [Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent($template)
    $xml.SelectSingleNode('//text[@id="1"]').InnerText = $msg
    $toast = [Windows.UI.Notifications.ToastNotification]::new($xml)
    [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier('ETL Monitor').Show($toast)
    # Option B: write to Windows Event Log (uncomment if preferred)
    # Write-EventLog -LogName Application -Source 'ETL Monitor' -EventId 1001 -EntryType Warning -Message $msg
}
          </pre>
        </div>
      </div>

    </div><!-- /tab-alerts -->

  </div><!-- tab-content -->
</div><!-- container -->

<!-- TOAST -->
<div class="position-fixed bottom-0 end-0 p-3" style="z-index:9999">
  <div id="liveToast" class="toast text-bg-dark border-secondary" role="alert">
    <div class="toast-header text-bg-dark border-secondary">
      <strong class="me-auto" id="toastTitle">ETL Monitor</strong>
      <button type="button" class="btn-close btn-close-white" data-bs-dismiss="toast"></button>
    </div>
    <div class="toast-body" id="toastBody"></div>
  </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
<script>
// ── Tab memory ──────────────────────────────────────────────────────────────
const TAB_KEY = 'etlmonitor_active_tab';
document.addEventListener('DOMContentLoaded', () => {
  const saved = localStorage.getItem(TAB_KEY);
  if (saved) {
    const tabEl = document.querySelector(`[data-bs-target="${saved}"]`);
    if (tabEl) bootstrap.Tab.getOrCreateInstance(tabEl).show();
  }
  document.querySelectorAll('[data-bs-toggle="tab"]').forEach(btn => {
    btn.addEventListener('shown.bs.tab', e => {
      localStorage.setItem(TAB_KEY, e.target.getAttribute('data-bs-target'));
      if (e.target.getAttribute('data-bs-target') === '#tab-alerts') {
        loadAlertPayload();
      }
    });
  });
  if (localStorage.getItem(TAB_KEY) === '#tab-alerts') loadAlertPayload();
});

// ── History expand ──────────────────────────────────────────────────────────
function toggleModelHistory(summaryRow, modelName) {
  const safeId    = modelName.replace(/[\s.]/g, '_');
  const detailRow = document.getElementById('detail-' + safeId);
  if (!detailRow) return;
  const isOpen = !detailRow.classList.contains('d-none');
  const icon   = summaryRow.querySelector('.expand-icon i');
  if (isOpen) {
    detailRow.classList.add('d-none');
    if (icon) icon.className = 'bi bi-chevron-right';
    summaryRow.style.outline = '';
    return;
  }
  detailRow.classList.remove('d-none');
  if (icon) icon.className = 'bi bi-chevron-down';
  summaryRow.style.outline = '2px solid #7c83fd44';
  const inner = detailRow.querySelector('.detail-inner');
  if (inner.dataset.loaded === '1') return;
  fetch('/api/history/' + encodeURIComponent(modelName))
    .then(r => r.json())
    .then(data => {
      if (!data.ok) { inner.innerHTML = `<div class="p-2 text-danger">Error: ${data.error}</div>`; return; }
      const rows = data.rows;
      if (!rows.length) { inner.innerHTML = '<div class="p-2 text-muted">No runs found.</div>'; return; }
      let html = `<table class="table table-sm mb-0" style="font-size:.8rem">
        <thead><tr style="background:#1a1d27">
          <th style="width:28px"></th><th>Status</th><th>Start Rows</th><th>End Rows</th>
          <th>Added</th><th>Elapsed (s)</th><th>Started</th><th>Error</th>
        </tr></thead><tbody>`;
      rows.forEach((r, i) => {
        const isFail = ['failed','failure','error'].includes((r.status||'').toLowerCase());
        const fmt  = n => n != null ? Number(n).toLocaleString() : '\u2014';
        const fmtF = n => n != null ? parseFloat(n).toFixed(1) : '\u2014';
        const badge = isFail
          ? `<span class="badge bg-danger">${r.status}</span>`
          : `<span class="badge bg-success">${r.status}</span>`;
        html += `<tr class="${isFail?'table-danger':''}" style="background:${i===0?'#1e2a1e':''}">
          <td class="text-center">${i===0?'<i class="bi bi-star-fill text-warning" style="font-size:.65rem"></i>':''}</td>
          <td>${badge}</td><td>${fmt(r.start_record_count)}</td><td>${fmt(r.end_record_count)}</td>
          <td>${fmt(r.records_added)}</td><td>${fmtF(r.elapsed_seconds)}</td>
          <td><small>${r.start_time||r.created_at||'\u2014'}</small></td>
          <td><small class="text-danger">${(r.error_message||'').slice(0,80)}</small></td>
        </tr>`;
      });
      html += '</tbody></table>';
      inner.innerHTML = html;
      inner.dataset.loaded = '1';
    })
    .catch(e => { inner.innerHTML = `<div class="p-2 text-danger">${e}</div>`; });
}

// ── Lineage expand ──────────────────────────────────────────────────────────
function toggleLineageRow(summaryRow, modelName) {
  const safeId    = modelName.replace(/[\s.]/g, '_');
  const detailRow = document.getElementById('ldetail-' + safeId);
  if (!detailRow) return;
  const isOpen = detailRow.classList.contains('open');
  const icon   = summaryRow.querySelector('.lineage-expand-icon i');
  if (isOpen) {
    detailRow.classList.remove('open');
    if (icon) icon.className = 'bi bi-chevron-right';
    summaryRow.style.outline = '';
    return;
  }
  detailRow.classList.add('open');
  if (icon) icon.className = 'bi bi-chevron-down';
  summaryRow.style.outline = '2px solid #7c83fd44';
  const inner = detailRow.querySelector('.lineage-col-inner');
  if (inner.dataset.loaded === '1') return;
  fetch('/api/lineage/' + encodeURIComponent(modelName))
    .then(r => r.json())
    .then(data => {
      if (!data.ok) { inner.innerHTML = `<div class="p-2 text-danger">Error: ${data.error}</div>`; return; }
      const cols = data.columns;
      if (!cols.length) { inner.innerHTML = '<div class="p-2 text-muted">No columns found.</div>'; return; }
      const typeBadge = t => {
        const map = {
          'Pass-through':    ['lb-pass', 'PASS'],
          'Rename':          ['lb-ren',  'RENAME'],
          'Surrogate Key':   ['lb-key',  'KEY'],
          'CAST':            ['lb-cast', 'CAST'],
          'TRY_CAST':        ['lb-tcast','TCAST'],
          'CASE Statement':  ['lb-case', 'CASE'],
          'String Cleansing':['lb-str',  'STR'],
          'Derived Expression':['lb-drv','DERIVED'],
        };
        const [cls, lbl] = map[t] || ['lb-pass', t];
        return `<span class="lbadge ${cls}">${lbl}</span>`;
      };
      let html = `<table class="table table-sm mb-0" style="font-size:.8rem">
        <thead><tr style="background:#111827">
          <th style="width:30px">#</th><th>Column</th><th>Source Table / Model</th>
          <th>Source Column</th><th>Type</th><th>Expression / Transformation</th>
        </tr></thead><tbody>`;
      // Cache of model source SQL, keyed by model name, to avoid repeated fetches
      const _srcCache = {};

      function renderExprCell(c, modelName) {
        const rawExpr = c.expression || '';
        const isSurrogate = c.transformation_type === 'Surrogate Key';
        const preStyle = 'margin:0;background:#0a0e18;color:#fcd34d;font-size:.72rem;' +
          'border:1px solid #2d3142;border-radius:4px;padding:4px 8px;' +
          'max-height:140px;overflow-y:auto;white-space:pre-wrap;word-break:break-all';
        if (!rawExpr) return '<span class="text-muted">\u2014</span>';

        const exprHtml = `<pre style="${preStyle}">${rawExpr.replace(/</g,'&lt;').replace(/>/g,'&gt;')}</pre>`;

        // For surrogate key columns, also offer a "View full source" button that
        // extracts the surrogate_key call from the raw model .sql file.
        if (!isSurrogate) return exprHtml;

        const btnId  = 'src-btn-' + c.column_name.replace(/\W/g,'_');
        const preId  = 'src-pre-' + c.column_name.replace(/\W/g,'_');
        const btn = `<button id="${btnId}" onclick="loadSurrogateSource('${modelName}','${c.column_name}','${btnId}','${preId}')"
          style="margin-top:4px;font-size:.68rem;padding:1px 8px;background:#1e2f4a;
                 color:#60a5fa;border:1px solid #2d5082;border-radius:3px;cursor:pointer">
          &#128196; View full source
        </button>
        <pre id="${preId}" style="${preStyle};display:none;margin-top:4px;color:#86efac"></pre>`;
        return exprHtml + btn;
      }

      cols.forEach((c, i) => {
        const fill = i % 2 === 0 ? '' : 'background:#0d1016';
        html += `<tr style="${fill}">
          <td class="text-muted">${i+1}</td>
          <td><strong style="color:#93c5fd">${c.column_name}</strong></td>
          <td style="color:#6ee7b7;font-size:.76rem">${c.source_table||'\u2014'}</td>
          <td style="color:#9aa0b4;font-size:.76rem;font-family:monospace">${c.source_column||'\u2014'}</td>
          <td>${typeBadge(c.transformation_type)}</td>
          <td style="font-family:monospace;font-size:.76rem">${renderExprCell(c, modelName)}</td>
        </tr>`;
      });
      html += '</tbody></table>';
      inner.innerHTML = html;
      inner.dataset.loaded = '1';
    })
    .catch(e => { inner.innerHTML = `<div class="p-2 text-danger">${e}</div>`; });
}

function openLineageForModel(modelName) {
  const lineageTab = document.querySelector('[data-bs-target="#tab-lineage"]');
  if (lineageTab) bootstrap.Tab.getOrCreateInstance(lineageTab).show();
  setTimeout(() => {
    const safeId     = modelName.replace(/[\s.]/g, '_');
    const summaryRow = document.querySelector(`#lineageTbody tr[data-model="${modelName}"]`);
    const detailRow  = document.getElementById('ldetail-' + safeId);
    if (!summaryRow) return;
    summaryRow.scrollIntoView({ behavior:'smooth', block:'center' });
    summaryRow.style.outline = '2px solid #7c83fd';
    setTimeout(() => {
      if (detailRow && !detailRow.classList.contains('open')) toggleLineageRow(summaryRow, modelName);
      setTimeout(() => { summaryRow.style.outline = '2px solid #7c83fd44'; }, 800);
    }, 300);
  }, 200);
}

let _lineageLayerFilter = 'all';
let _lineageSearchFilter = '';
function filterLineage(pill, layer) {
  if (pill) {
    document.querySelectorAll('#lineageFilters .filter-pill').forEach(p => p.classList.remove('active'));
    pill.classList.add('active');
    _lineageLayerFilter = layer;
  }
  if (layer === '__search__') {
    _lineageSearchFilter = (document.getElementById('lineageSearch').value || '').toLowerCase();
  }
  document.querySelectorAll('#lineageTbody .lineage-summary-row').forEach(row => {
    const rowLayer  = row.dataset.layer || '';
    const rowModel  = (row.dataset.model || '').toLowerCase();
    const safeId    = row.dataset.model.replace(/[\s.]/g, '_');
    const detailRow = document.getElementById('ldetail-' + safeId);
    const show = (_lineageLayerFilter === 'all' || rowLayer === _lineageLayerFilter)
              && (!_lineageSearchFilter || rowModel.includes(_lineageSearchFilter));
    row.style.display = show ? '' : 'none';
    if (detailRow) detailRow.style.display = show ? '' : 'none';
  });
}

// ── Alert tab helpers ───────────────────────────────────────────────────────
function loadAlertPayload() {
  fetch('/api/alert')
    .then(r => r.json())
    .then(data => {
      document.getElementById('alertPayloadPre').textContent = JSON.stringify(data, null, 2);
    })
    .catch(e => {
      document.getElementById('alertPayloadPre').textContent = 'Error: ' + e;
    });
}

function testAlertEndpoint() {
  const el = document.getElementById('alertEndpointResult');
  el.style.display = 'block';
  el.innerHTML = '<span class="text-muted">Fetching…</span>';
  fetch('/api/alert')
    .then(r => r.json())
    .then(data => {
      const color = data.health === 'green' ? '#22c55e' : (data.health === 'amber' ? '#f59e0b' : '#ef4444');
      el.innerHTML = `<span style="color:${color}">✓ OK — health: <strong>${data.health}</strong>, `
                   + `red: ${data.red_count}, amber: ${data.amber_count}</span>`;
      document.getElementById('alertPayloadPre').textContent = JSON.stringify(data, null, 2);
    })
    .catch(e => { el.innerHTML = `<span class="text-danger">Error: ${e}</span>`; });
}

function saveAlertConfig() {
  const url     = document.getElementById('webhookUrl').value.trim();
  const enabled = document.getElementById('webhookEnabled').checked;
  fetch('/api/alert/config', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ webhook_url: url, enabled })
  })
  .then(r => r.json())
  .then(d => d.ok
    ? showToast('Alert config saved' + (enabled ? ' — webhook active' : ' — webhook disabled'))
    : showToast(d.error || 'Save failed', 'Error', true))
  .catch(e => showToast(String(e), 'Error', true));
}

function testWebhook() {
  const url = document.getElementById('webhookUrl').value.trim();
  if (!url) { showToast('Enter a webhook URL first', 'Alert', true); return; }
  fetch('/api/alert/test', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ webhook_url: url })
  })
  .then(r => r.json())
  .then(d => d.ok
    ? showToast('Test payload dispatched to webhook ✓')
    : showToast(d.error || 'Dispatch failed', 'Error', true))
  .catch(e => showToast(String(e), 'Error', true));
}

// ── Shared utilities ────────────────────────────────────────────────────────
function showToast(msg, title='ETL Monitor', isError=false) {
  document.getElementById('toastTitle').textContent = title;
  document.getElementById('toastBody').textContent  = msg;
  const el = document.getElementById('liveToast');
  el.classList.toggle('text-bg-danger', isError);
  el.classList.toggle('text-bg-dark',  !isError);
  new bootstrap.Toast(el, { delay: 4000 }).show();
}

function openRangeForModel(modelName) {
  const rangesTab = document.querySelector('[data-bs-target="#tab-ranges"]');
  if (rangesTab) bootstrap.Tab.getOrCreateInstance(rangesTab).show();
  setTimeout(() => {
    const rows = document.querySelectorAll('#tab-ranges table tbody tr');
    for (const row of rows) {
      const cell = row.querySelector('td strong');
      if (cell && cell.textContent.trim() === modelName) {
        row.scrollIntoView({ behavior: 'smooth', block: 'center' });
        row.style.outline = '2px solid #7c83fd';
        row.style.outlineOffset = '-2px';
        setTimeout(() => { row.style.outline = ''; }, 3000);
        break;
      }
    }
  }, 150);
}

function saveRange(id, model) {
  const g = k => { const el = document.getElementById(k); const v = el ? el.value.trim() : ''; return v === '' ? null : parseFloat(v); };
  const data = {
    id, model_name: model,
    start_record_count_low:   g(`r${id}_src_lo`), start_record_count_high:  g(`r${id}_src_hi`),
    end_record_count_low:     g(`r${id}_end_lo`), end_record_count_high:    g(`r${id}_end_hi`),
    records_added_count_low:  g(`r${id}_add_lo`), records_added_count_high: g(`r${id}_add_hi`),
    elapsed_seconds_low:      g(`r${id}_ela_lo`), elapsed_seconds_high:     g(`r${id}_ela_hi`),
  };
  fetch('/api/ranges/save', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(data) })
    .then(r => r.json())
    .then(d => d.ok ? showToast(`Saved ranges for ${model}`) : showToast(d.error||'Save failed','Error',true))
    .catch(e => showToast(String(e),'Error',true));
}

function seedRanges() {
  fetch('/api/ranges/seed', { method: 'POST' })
    .then(r => r.json())
    .then(d => d.ok
      ? (showToast('Ranges seeded from 30-day history'), setTimeout(() => location.reload(), 1500))
      : showToast(d.error||'Seed failed','Error',true))
    .catch(e => showToast(String(e),'Error',true));
}

// ── Lineage + Tests tab ─────────────────────────────────────────────────────
let _ltFilter = 'all';
let _ltAllRows = [];

function filterLT(pill, status) {
  _ltFilter = status;
  document.querySelectorAll('#tab-lineage-tests .filter-pill').forEach(p => p.classList.remove('active'));
  if (pill) pill.classList.add('active');
  renderLTRows();
}

function renderLTRows() {
  const tbody = document.getElementById('ltTbody');
  const filtered = _ltFilter === 'all'
    ? _ltAllRows
    : _ltAllRows.filter(r => (r.test_status || 'no_test') === _ltFilter);
  if (!filtered.length) {
    tbody.innerHTML = `<tr><td colspan="10" class="text-center text-muted py-3">No columns match the selected filter.</td></tr>`;
    return;
  }
  const typeBadge = t => {
    const map = {
      'Pass-through':      ['lb-pass',  'PASS'],
      'Rename':            ['lb-ren',   'RENAME'],
      'Surrogate Key':     ['lb-key',   'KEY'],
      'CAST':              ['lb-cast',  'CAST'],
      'TRY_CAST':          ['lb-tcast', 'TCAST'],
      'CASE Statement':    ['lb-case',  'CASE'],
      'String Cleansing':  ['lb-str',   'STR'],
      'Derived Expression':['lb-drv',   'DERIVED'],
    };
    const [cls, lbl] = map[t] || ['lb-pass', t || '—'];
    return `<span class="lbadge ${cls}">${lbl}</span>`;
  };
  const testBadge = status => {
    if (status === 'pass')    return `<span class="badge bg-success"><i class="bi bi-check-circle me-1"></i>PASS</span>`;
    if (status === 'fail')    return `<span class="badge bg-danger"><i class="bi bi-x-circle me-1"></i>FAIL</span>`;
    return `<span class="badge" style="background:#374151;color:#9ca3af">NO TEST</span>`;
  };
  let html = '';
  filtered.forEach((c, i) => {
    const rowBg = c.test_status === 'fail'    ? 'background:rgba(239,68,68,.08)'
                : c.test_status === 'no_test' ? 'background:rgba(245,158,11,.04)'
                : '';
    const failures = c.failures;
    const execTime = c.execution_time_seconds != null ? c.execution_time_seconds.toFixed(2) + 's' : '—';
    const compiledSql = c.compiled_sql
      ? `<details style="font-size:.7rem"><summary style="cursor:pointer;color:#7c83fd">SQL</summary><pre style="max-height:120px;font-size:.68rem;margin-top:4px">${c.compiled_sql.replace(/</g,'&lt;').replace(/>/g,'&gt;')}</pre></details>`
      : '';
    html += `<tr style="${rowBg}" data-test-status="${c.test_status || 'no_test'}">
      <td class="text-muted">${i+1}</td>
      <td><strong style="color:#93c5fd">${c.column_name || '—'}</strong></td>
      <td style="color:#6ee7b7;font-size:.76rem">${c.source_table || '—'}</td>
      <td style="color:#9aa0b4;font-size:.76rem;font-family:monospace">${c.source_column || '—'}</td>
      <td>${typeBadge(c.transformation_type)}</td>
      <td style="font-size:.76rem;font-family:monospace">${c.test_name || '<span class="text-muted">—</span>'}${compiledSql}</td>
      <td class="text-center">${failures != null ? `<span class="${failures > 0 ? 'badge bg-danger' : 'badge bg-success'}">${failures}</span>` : '<span class="text-muted">—</span>'}</td>
      <td class="text-center" style="font-size:.75rem;color:#9aa0b4">${execTime}</td>
      <td class="text-center">${testBadge(c.test_status)}</td>
      <td><small class="text-muted">${c.test_run_at || '—'}</small></td>
    </tr>`;
  });
  tbody.innerHTML = html;
}

function loadLineageTests() {
  const model  = document.getElementById('ltModelSelect').value;
  const tbody  = document.getElementById('ltTbody');
  const bar    = document.getElementById('ltSummaryBar');
  if (!model) {
    tbody.innerHTML = `<tr><td colspan="10" class="text-center text-muted py-4">Select a model above to load lineage + test data.</td></tr>`;
    bar.style.display = 'none';
    return;
  }
  tbody.innerHTML = `<tr><td colspan="10" class="text-center text-muted py-3"><span class="spinner-border spinner-border-sm me-2"></span>Loading…</td></tr>`;
  fetch('/api/lineage-tests/' + encodeURIComponent(model))
    .then(r => r.json())
    .then(data => {
      if (!data.ok) {
        tbody.innerHTML = `<tr><td colspan="10" class="text-danger p-3">Error: ${data.error}</td></tr>`;
        return;
      }
      _ltAllRows = data.columns || [];
      // reset filter
      _ltFilter = 'all';
      document.querySelectorAll('#tab-lineage-tests .filter-pill').forEach(p => p.classList.remove('active'));
      document.getElementById('ltf-all').classList.add('active');
      renderLTRows();
      // coverage summary
      const total    = _ltAllRows.length;
      const tested   = _ltAllRows.filter(r => r.test_status !== 'no_test').length;
      const failing  = _ltAllRows.filter(r => r.test_status === 'fail').length;
      const pct      = total ? Math.round(100 * tested / total) : 0;
      const pctColor = pct >= 80 ? '#22c55e' : pct >= 50 ? '#f59e0b' : '#ef4444';
      bar.style.display = '';
      document.getElementById('ltSummaryText').innerHTML =
        `<i class="bi bi-bar-chart me-1"></i>`
        + `<strong style="color:${pctColor}">${pct}% coverage</strong> — `
        + `${tested} of ${total} columns have tests`
        + (failing ? ` · <span class="text-danger"><strong>${failing} failing</strong></span>` : '')
        + ` · <span class="text-muted">${total - tested} untested</span>`;
    })
    .catch(e => {
      tbody.innerHTML = `<tr><td colspan="10" class="text-danger p-3">${e}</td></tr>`;
    });
}

{% raw %}
// ── Surrogate key full source viewer ────────────────────────────────────────
function loadSurrogateSource(modelName, columnName, btnId, preId) {
  const btn = document.getElementById(btnId);
  const pre = document.getElementById(preId);
  if (!btn || !pre) return;

  // Toggle off if already loaded
  if (pre.dataset.loaded === '1') {
    pre.style.display = pre.style.display === 'none' ? '' : 'none';
    btn.textContent = pre.style.display === 'none' ? '📄 View full source' : '🔼 Hide source';
    return;
  }

  btn.textContent = '⏳ Loading…';
  btn.disabled = true;

  fetch('/api/model-source/' + encodeURIComponent(modelName))
    .then(r => r.json())
    .then(data => {
      if (!data.ok) {
        pre.textContent = 'Error: ' + data.error;
        pre.style.display = '';
        btn.textContent = '⚠ Source not found';
        return;
      }
      // Extract just the surrogate_key call block for this column.
      // Pattern: {{ dbt_utils.generate_surrogate_key([...]) }}
      //                          as <column_name>
      const src = data.source;
      const colLower = columnName.toLowerCase();
      // Try to find the generate_surrogate_key expression that maps to this column.
      // Match from {{ to the alias line containing the column name.
      const re = /(\{\{[\s\S]*?generate_surrogate_key[\s\S]*?\}\}[\s\S]*?as\s+)/gi;
      let bestMatch = null;
      let m;
      while ((m = re.exec(src)) !== null) {
        // grab from the {{ to end of alias line
        const afterAlias = src.indexOf('\n', m.index + m[0].length);
        const chunk = src.slice(m.index, afterAlias > -1 ? afterAlias : m.index + m[0].length + 40);
        if (chunk.toLowerCase().includes(colLower)) {
          bestMatch = chunk.trim();
          break;
        }
      }
      pre.textContent = bestMatch || src;  // fallback: show full source
      pre.style.display = '';
      pre.dataset.loaded = '1';
      btn.textContent = '🔼 Hide source';
      btn.disabled = false;
    })
    .catch(e => {
      pre.textContent = 'Fetch error: ' + e;
      pre.style.display = '';
      btn.textContent = '⚠ Error';
      btn.disabled = false;
    });
}

// Auto-refresh disabled — use the manual refresh button in the navbar
{% endraw %}
</script>
</body>
</html>
"""


# ── ROUTES ─────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    try:
        runs        = pipeline_status_summary()
        history     = pipeline_history_last_run()
        fr7         = failure_rates(7)
        fr30        = failure_rates(30)
        ranges      = get_pipeline_ranges()
        dbt_models  = dbt_model_executions()
        dbt_tests   = dbt_test_executions()
        t_sql       = test_sql_results()
        lineage     = lineage_summary()
        health      = overall_health()
        alert_cfg   = get_alert_config()
        now         = datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')

        maybe_dispatch_webhook(runs, health)

    except Exception as e:
        return f"<pre style='color:red;background:#111;padding:20px'>DB Error: {e}</pre>", 500

    return render_template_string(
        TEMPLATE,
        runs=runs,
        history=history,
        fr7=fr7,
        fr30=fr30,
        ranges=ranges,
        dbt_models=dbt_models,
        dbt_tests=dbt_tests,
        test_sql=t_sql,
        lineage=lineage,
        health=health,
        alert_cfg=alert_cfg,
        now=now,
    )


@app.route('/api/history/<path:model_name>')
def api_model_history(model_name):
    try:
        sql = """
        SELECT TOP 30
            id, run_id, model_name, model_layer, status,
            start_record_count, end_record_count, records_added,
            elapsed_seconds, start_time, end_time, error_message, created_at
        FROM dbo.pipeline
        WHERE model_name = ?
        ORDER BY start_time DESC
        """
        rows = query(sql, [model_name])
        for r in rows:
            for k in ('start_time', 'end_time', 'created_at'):
                if r.get(k):
                    r[k] = str(r[k])[:19]
        return jsonify({'ok': True, 'rows': rows})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


@app.route('/api/lineage/<path:model_name>')
def api_model_lineage(model_name):
    try:
        sql = """
        SELECT column_name, source_table, source_column, transformation_type, expression
        FROM dbo.model_lineage
        WHERE model = ?
        ORDER BY id
        """
        rows = query(sql, [model_name])
        return jsonify({'ok': True, 'columns': rows})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


@app.route('/api/lineage-tests/<path:model_name>')
def api_lineage_with_tests(model_name):
    """Return column lineage joined to test coverage for a given model."""
    try:
        rows = lineage_with_tests(model_name)
        return jsonify({'ok': True, 'columns': rows})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


@app.route('/api/ranges/save', methods=['POST'])
def api_save_range():
    data = request.get_json()
    try:
        range_id   = data.get('id')
        model_name = data.get('model_name', '')
        old_rows   = query("SELECT * FROM dbo.pipeline_ranges WHERE id = ?", [range_id])
        old        = old_rows[0] if old_rows else {}
        fields = [
            ('start_record_count_low',   'start_record_count_low'),
            ('start_record_count_high',  'start_record_count_high'),
            ('end_record_count_low',     'end_record_count_low'),
            ('end_record_count_high',    'end_record_count_high'),
            ('records_added_count_low',  'records_added_count_low'),
            ('records_added_count_high', 'records_added_count_high'),
            ('elapsed_seconds_low',      'elapsed_seconds_low'),
            ('elapsed_seconds_high',     'elapsed_seconds_high'),
        ]
        execute("""
        UPDATE dbo.pipeline_ranges SET
            start_record_count_low=?, start_record_count_high=?,
            end_record_count_low=?, end_record_count_high=?,
            records_added_count_low=?, records_added_count_high=?,
            elapsed_seconds_low=?, elapsed_seconds_high=?,
            updated_at=SYSUTCDATETIME()
        WHERE id=?
        """, [
            data.get('start_record_count_low'),  data.get('start_record_count_high'),
            data.get('end_record_count_low'),    data.get('end_record_count_high'),
            data.get('records_added_count_low'), data.get('records_added_count_high'),
            data.get('elapsed_seconds_low'),     data.get('elapsed_seconds_high'),
            range_id,
        ])
        audit_sql = "INSERT INTO dbo.pipeline_range_audit (model_name,field_name,old_value,new_value) VALUES (?,?,?,?)"
        for data_key, db_col in fields:
            new_val = data.get(data_key)
            old_val = old.get(db_col)
            if str(old_val) != str(new_val):
                execute(audit_sql, [model_name, data_key,
                                    str(old_val) if old_val is not None else None,
                                    str(new_val) if new_val is not None else None])
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


@app.route('/api/ranges/seed', methods=['POST'])
def api_seed_ranges():
    try:
        seed_ranges_from_history()
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


# ── Alert API endpoints ────────────────────────────────────────────────────────

@app.route('/api/alert')
def api_alert():
    try:
        runs    = pipeline_status_summary()
        health  = overall_health()
        payload = build_alert_payload(runs, health)
        return jsonify(payload)
    except Exception as e:
        return jsonify({'health': 'unknown', 'error': str(e)}), 500


@app.route('/api/alert/config', methods=['POST'])
def api_save_alert_config():
    data = request.get_json()
    try:
        save_alert_config(
            webhook_url=data.get('webhook_url', '').strip() or None,
            enabled=bool(data.get('enabled', False)),
        )
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


@app.route('/api/alert/test', methods=['POST'])
def api_test_webhook():
    data        = request.get_json()
    webhook_url = (data.get('webhook_url') or '').strip()
    if not webhook_url:
        return jsonify({'ok': False, 'error': 'No webhook_url provided'})
    try:
        runs    = pipeline_status_summary()
        health  = overall_health()
        payload = build_alert_payload(runs, health)
        payload['_test'] = True
        _dispatch_webhook(payload, webhook_url)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


@app.route('/api/status')
def api_status():
    """Legacy JSON endpoint — preserved for backward compatibility."""
    try:
        runs   = pipeline_status_summary()
        health = overall_health()
        reds   = [r for r in runs if r['status_color'] == 'danger']
        ambers = [r for r in runs if r['status_color'] == 'warning']
        return jsonify({
            'health':         health,
            'total':          len(runs),
            'red':            len(reds),
            'amber':          len(ambers),
            'alerts': [
                {'model': r['model_name'], 'issues': r['issues'], 'error': r['error_message']}
                for r in reds
            ],
            'zero_row_warnings': [
                {'model': r['model_name'], 'warnings': r['zero_warnings']}
                for r in ambers
            ],
        })
    except Exception as e:
        return jsonify({'health': 'unknown', 'error': str(e)}), 500


# ── Model source reader ────────────────────────────────────────────────────────

DBT_MODELS_DIR = r'C:\Users\Keith\baseball-sql\DBT_BASEBALL_SQLSERVER\models'

def _find_model_sql(model_name):
    """
    Walk DBT_MODELS_DIR to find <model_name>.sql.
    Returns the file path if found, else None.
    """
    import os
    if not os.path.isdir(DBT_MODELS_DIR):
        return None
    for root, _dirs, files in os.walk(DBT_MODELS_DIR):
        for fname in files:
            if fname.lower() == f'{model_name.lower()}.sql':
                return os.path.join(root, fname)
    return None


@app.route('/api/model-source/<path:model_name>')
def api_model_source(model_name):
    """Return the raw Jinja SQL source for a dbt model."""
    import os
    path = _find_model_sql(model_name)
    if not path:
        return jsonify({'ok': False, 'error': f'Source file not found for model: {model_name}',
                        'searched': DBT_MODELS_DIR})
    try:
        with open(path, 'r', encoding='utf-8', errors='replace') as fh:
            source = fh.read()
        return jsonify({'ok': True, 'model': model_name, 'path': path, 'source': source})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})


# ── dbt docs ───────────────────────────────────────────────────────────────────

DBT_TARGET_DIR = r'C:\Users\Keith\baseball-sql\DBT_BASEBALL_SQLSERVER\target'
DBT_DOCS_PORT  = 8080
_dbt_proc      = None

def _ensure_dbt_docs_running():
    import subprocess, os, time, socket
    global _dbt_proc
    def port_open(port):
        with socket.socket() as s:
            s.settimeout(0.3)
            return s.connect_ex(('127.0.0.1', port)) == 0
    if port_open(DBT_DOCS_PORT):
        return True
    if not os.path.isdir(DBT_TARGET_DIR):
        return False
    if _dbt_proc and _dbt_proc.poll() is not None:
        _dbt_proc = None
    if _dbt_proc is None:
        project_dir = os.path.dirname(DBT_TARGET_DIR)
        try:
            _dbt_proc = subprocess.Popen(
                ['dbt', 'docs', 'serve', '--port', str(DBT_DOCS_PORT)],
                cwd=project_dir, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True,
            )
            for _ in range(10):
                time.sleep(0.4)
                if port_open(DBT_DOCS_PORT):
                    return True
        except Exception as e:
            print(f"  [dbt docs serve] {e}")
    return port_open(DBT_DOCS_PORT)


@app.route('/dbt-docs')
@app.route('/dbt-docs/')
def dbt_docs():
    import os
    if not os.path.isdir(DBT_TARGET_DIR):
        return (
            "<pre style='color:#ef4444;background:#111;padding:20px'>"
            f"dbt target directory not found:\n  {DBT_TARGET_DIR}\n\n"
            "Run <b>dbt docs generate</b> first, then restart ETL Monitor.</pre>"
        ), 404
    ok = _ensure_dbt_docs_running()
    if not ok:
        return (
            "<pre style='color:#ef4444;background:#111;padding:20px'>"
            f"Could not start dbt docs serve on port {DBT_DOCS_PORT}.\n\n"
            "Make sure <b>dbt</b> is on your PATH, then try again.</pre>"
        ), 503
    from flask import redirect as _redirect
    return _redirect(f'http://127.0.0.1:{DBT_DOCS_PORT}', code=302)


# ── MAIN ───────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    print("\n  ⚡ ETL Monitor starting...")
    print("  → http://127.0.0.1:5050")
    print("  → Alert endpoint: http://127.0.0.1:5050/api/alert\n")
    app.run(host='127.0.0.1', port=5050, debug=True)