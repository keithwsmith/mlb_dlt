"""
lineage_builder.py
==================
Parses every dbt SQL model in DBT_PROJECT_DIR/models/ and upserts a row
into dbo.model_lineage for every output column found, recording:

    model             – dbt model name           e.g. stg_play_events
    column_name       – output column alias       e.g. load_id
    source_table      – upstream table(s) / model e.g. dw.play_events
    source_column     – raw source field name     e.g. _dlt_load_id
    transformation_type – one of:
                          Pass-through | Rename | Surrogate Key | CAST |
                          TRY_CAST | CASE Statement | String Cleansing |
                          Derived Expression
    expression        – the actual SQL expression (blank for pass-throughs)

Called by pipeline.py as the final step via run_lineage_builder().
Can also be run standalone:  python lineage_builder.py
"""

import re
import os
import sys
import pyodbc
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional

# ---------------------------------------------------------------------------
# CONFIG  – these are imported from pipeline.py when called from there,
#           or can be set here for standalone use.
# ---------------------------------------------------------------------------
try:
    from pipeline import DB_CONNECTION, DBT_PROJECT_DIR  # noqa: F401 (used below)
except ImportError:
    # Standalone defaults — adjust as needed
    DBT_PROJECT_DIR = r"C:\Users\Keith\baseball-sql\DBT_BASEBALL_SQLSERVER"
    DB_CONNECTION = {
        "driver":   "{ODBC Driver 17 for SQL Server}",
        "server":   "KEITH-PERSONAL",
        "database": "dlt",
        "uid":      "sa",
        "pwd":      "pass0123",
    }

MODELS_DIR = os.path.join(DBT_PROJECT_DIR, "models")

# ---------------------------------------------------------------------------
# SQL TYPE KEYWORDS  – used to prevent aliased type names being mistaken
#                      for column aliases inside CAST(… AS <type>)
# ---------------------------------------------------------------------------
SQL_TYPES = {
    "INT", "BIGINT", "SMALLINT", "TINYINT", "BIT",
    "VARCHAR", "NVARCHAR", "CHAR", "NCHAR", "TEXT", "NTEXT",
    "DATE", "DATETIME", "DATETIME2", "SMALLDATETIME", "TIME",
    "FLOAT", "REAL", "DECIMAL", "NUMERIC", "MONEY", "SMALLMONEY",
    "UNIQUEIDENTIFIER", "VARBINARY", "BINARY", "IMAGE", "MAX",
}

# Layer detection based on model name prefix / folder name
LAYER_MAP = {
    "stage":        "Stage",
    "dimensions":   "Dimension",
    "intermediate": "Intermediate",
    "facts":        "Fact",
    "mart":         "Mart",
}


# ===========================================================================
# DATABASE HELPERS
# ===========================================================================

def _get_conn() -> pyodbc.Connection:
    parts = [f"{k}={v}" for k, v in DB_CONNECTION.items()]
    return pyodbc.connect(";".join(parts))


def ensure_lineage_table():
    """Create dbo.model_lineage if it does not exist."""
    ddl = """
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'model_lineage'
    )
    BEGIN
        CREATE TABLE dbo.model_lineage (
            id                  INT IDENTITY(1,1) PRIMARY KEY,
            model               NVARCHAR(200)  NOT NULL,
            column_name         NVARCHAR(200)  NOT NULL,
            source_table        NVARCHAR(500)  NULL,
            source_column       NVARCHAR(200)  NULL,
            transformation_type NVARCHAR(50)   NOT NULL,
            expression          NVARCHAR(MAX)  NULL,
            layer               NVARCHAR(50)   NULL,
            refreshed_at        DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
            CONSTRAINT uq_model_lineage UNIQUE (model, column_name)
        );
        CREATE NONCLUSTERED INDEX ix_lineage_model
            ON dbo.model_lineage (model);
    END
    """
    conn = _get_conn()
    try:
        conn.execute(ddl)
        conn.commit()
    finally:
        conn.close()


def upsert_lineage_rows(rows: List[Dict]):
    """
    MERGE dbo.model_lineage on (model, column_name).
    Rows that already exist are updated; new rows are inserted.
    """
    if not rows:
        return

    merge_sql = """
    MERGE dbo.model_lineage AS tgt
    USING (SELECT ? AS model,
                  ? AS column_name,
                  ? AS source_table,
                  ? AS source_column,
                  ? AS transformation_type,
                  ? AS expression,
                  ? AS layer) AS src
        ON tgt.model = src.model AND tgt.column_name = src.column_name
    WHEN MATCHED THEN
        UPDATE SET
            source_table        = src.source_table,
            source_column       = src.source_column,
            transformation_type = src.transformation_type,
            expression          = src.expression,
            layer               = src.layer,
            refreshed_at        = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (model, column_name, source_table, source_column,
                transformation_type, expression, layer)
        VALUES (src.model, src.column_name, src.source_table,
                src.source_column, src.transformation_type,
                src.expression, src.layer);
    """
    conn = _get_conn()
    try:
        for row in rows:
            conn.execute(
                merge_sql,
                row["model"],
                row["column_name"],
                row["source_table"],
                row["source_column"],
                row["transformation_type"],
                row["expression"],
                row["layer"],
            )
        conn.commit()
    finally:
        conn.close()


def delete_model_rows(model_name: str):
    """Remove all lineage rows for a model before re-inserting (clean refresh)."""
    conn = _get_conn()
    try:
        conn.execute(
            "DELETE FROM dbo.model_lineage WHERE model = ?", model_name
        )
        conn.commit()
    finally:
        conn.close()


# ===========================================================================
# SQL PARSING
# ===========================================================================

def _clean_sql(sql: str) -> str:
    """Strip Jinja, block comments, line comments; mark surrogate key calls."""
    # Surrogate key — mark before removing other Jinja
    sql = re.sub(
        r"\{\{\s*dbt_utils\.generate_surrogate_key\s*\(.*?\)\s*\}\}",
        "__SURROGATE_KEY__",
        sql,
        flags=re.DOTALL,
    )
    sql = re.sub(r"\{\{.*?\}\}", "__JINJA__", sql, flags=re.DOTALL)
    sql = re.sub(r"\{%.*?%\}", "", sql, flags=re.DOTALL)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    sql = re.sub(r"--[^\n]*", "", sql)
    return sql


def _extract_upstream(sql_raw: str) -> Tuple[List[str], List[str]]:
    """Return (source_tables, ref_models) from Jinja source()/ref() calls."""
    sources = re.findall(
        r"\{\{\s*source\s*\(\s*['\"](\w+)['\"]\s*,\s*['\"](\w+)['\"]\s*\)\s*\}\}",
        sql_raw, re.IGNORECASE,
    )
    refs = re.findall(
        r"\{\{\s*ref\s*\(\s*['\"](\w+)['\"]\s*\)\s*\}\}",
        sql_raw, re.IGNORECASE,
    )
    return [f"dw.{tbl}" for _, tbl in sources], list(refs)


def _classify(expr: str) -> str:
    """Return the transformation type label for an expression."""
    eu = expr.upper().strip()

    if not expr.strip() or expr.strip() == "__SURROGATE_KEY__":
        return "Surrogate Key"

    # Plain identifier: col, table.col, [bracketed_col], or table.[col]
    if re.match(r"^[a-zA-Z_]\w*(\.\w+)?$", expr.strip()):
        return "Pass-through"
    if re.match(r"^\[\w+\]$", expr.strip()):
        return "Pass-through"

    if eu.startswith("CASE") or eu.startswith("(CASE"):
        return "CASE Statement"
    if "TRY_CAST(" in eu:
        return "TRY_CAST"
    if re.search(r"\bCAST\s*\(", eu):
        return "CAST"

    for pattern in [
        r"\bREPLACE\s*\(",
        r"\bLTRIM\s*\(",
        r"\bRTRIM\s*\(",
        r"\bTRIM\s*\(",
        r"COALESCE\s*\(\s*NULLIF\s*\(",
        r"\bISNULL\s*\(\s*NULLIF\s*\(",
    ]:
        if re.search(pattern, eu):
            return "String Cleansing"

    return "Derived Expression"


SKIP_STARTS = (
    "FROM ", "WHERE ", "GROUP ", "ORDER ", "HAVING ",
    "JOIN ", "LEFT ", "RIGHT ", "INNER ", "OUTER ", "CROSS ",
    "ON ", "AND ", "OR ", "WITH ", "INSERT", "UPDATE",
    "UNION", "EXCEPT", "INTERSECT", "MERGE ", "USING ",
    "WHEN NOT", "WHEN MATCHED", "PARTITION ", "BEGIN",
    "VALUES", "BETWEEN ",
)
SKIP_EXACT = {
    "END", "END)", ")", "(", "*",
    "ELSE NULL", "ELSE 0", "ELSE 1", "",
}


def _parse_columns(sql_raw: str) -> List[Dict]:
    """
    Walk SELECT statement(s) and return one dict per output column:
        {output_col, transform_type, expression, source_col}
    """
    src_tables, ref_models = _extract_upstream(sql_raw)
    upstream_all = src_tables + ref_models
    upstream_str = ", ".join(upstream_all) if upstream_all else "(static/computed)"

    sql = _clean_sql(sql_raw)

    # ── Special case: VALUES-based model (e.g. dim_zone) ──────────────────
    values_match = re.search(
        r"\)\s*as\s+\w+\s*\(\s*([\w\s,]+)\s*\)", sql, re.IGNORECASE
    )
    if values_match and "values" in sql.lower():
        col_names = [c.strip() for c in values_match.group(1).split(",") if c.strip()]
        return [
            {
                "output_col": c,
                "transform_type": "Pass-through",
                "expression": "",
                "source_col": c,
                "upstream": "(static inline values)",
            }
            for c in col_names
        ]

    lines = sql.split("\n")
    results: List[Dict] = []
    seen: set = set()
    in_select = False

    i = 0
    while i < len(lines):
        raw_line = lines[i]
        line = raw_line.strip()
        i += 1

        # Track when we enter a SELECT block
        if re.match(r"^\s*select\b", raw_line, re.IGNORECASE):
            in_select = True
            continue

        if not in_select:
            continue
        if not line or line.upper() in SKIP_EXACT:
            continue

        lu = line.upper()
        if any(lu.startswith(s) for s in SKIP_STARTS):
            in_select = False
            continue

        # Collapse multi-line CASE … END
        case_open  = len(re.findall(r"\bCASE\b", line, re.IGNORECASE))
        case_close = len(re.findall(r"\bEND\b",  line, re.IGNORECASE))
        if case_open > case_close:
            depth = case_open - case_close
            collected = line
            while i < len(lines) and depth > 0:
                nxt = lines[i].strip()
                collected += " " + nxt
                depth += len(re.findall(r"\bCASE\b", nxt, re.IGNORECASE))
                depth -= len(re.findall(r"\bEND\b",  nxt, re.IGNORECASE))
                i += 1
            line = collected

        stripped = line.rstrip(",").strip()

        # ── Surrogate key ─────────────────────────────────────────────────
        if "__SURROGATE_KEY__" in stripped:
            as_m = re.search(r"\bAS\s+(\w+)\s*$", stripped, re.IGNORECASE)
            alias = as_m.group(1) if as_m else "surrogate_key"
            if alias.lower() not in seen:
                seen.add(alias.lower())
                results.append(
                    {
                        "output_col": alias,
                        "transform_type": "Surrogate Key",
                        "expression": "dbt_utils.generate_surrogate_key([…])",
                        "source_col": "",
                        "upstream": upstream_str,
                    }
                )
            continue

        # ── Find AS alias at end of line ──────────────────────────────────
        as_m = re.search(r"\bAS\s+(\w+)\s*$", stripped, re.IGNORECASE)

        if not as_m:
            # Bare identifier — no alias
            bare = re.match(r"^(?:[\w]+\.)?(\w+)$", stripped)
            if bare:
                col = bare.group(1)
                if col.upper() not in SQL_TYPES and col.lower() not in seen:
                    seen.add(col.lower())
                    results.append(
                        {
                            "output_col": col,
                            "transform_type": "Pass-through",
                            "expression": "",
                            "source_col": col,
                            "upstream": upstream_str,
                        }
                    )
            continue

        alias = as_m.group(1)
        if alias.upper() in SQL_TYPES or alias.lower() in seen:
            continue
        seen.add(alias.lower())

        expr = stripped[: as_m.start()].strip().lstrip(",").strip()

        if not expr or "__JINJA__" in expr:
            continue

        ttype = _classify(expr)

        # Source column identification
        src_col = ""
        if ttype == "Pass-through":
            # Handle plain col, table.col, and [bracketed] SQL Server identifiers
            m = re.match(r"^(?:\w+\.)?(\w+)$", expr)
            if not m:
                m = re.match(r"^\[(\w+)\]$", expr.strip())
            src_col = m.group(1) if m else alias
            if src_col.lower() != alias.lower():
                ttype = "Rename"
        elif ttype == "Rename":
            m = re.match(r"^(?:\w+\.)?(\w+)$", expr)
            if not m:
                m = re.match(r"^\[(\w+)\]$", expr.strip())
            src_col = m.group(1) if m else ""

        # Build display expression
        if ttype == "Pass-through":
            display_expr = ""
        elif ttype == "Rename":
            display_expr = f"{src_col} renamed to {alias}"
        else:
            display_expr = expr[:2000]  # cap at 2000 chars

        results.append(
            {
                "output_col": alias,
                "transform_type": ttype,
                "expression": display_expr,
                "source_col": src_col,
                "upstream": upstream_str,
            }
        )

    return results


# ===========================================================================
# FILE DISCOVERY
# ===========================================================================

def _get_layer(folder_name: str, model_name: str) -> str:
    """Derive layer from folder name."""
    return LAYER_MAP.get(folder_name.lower(), "other")


def discover_models(models_dir: str) -> List[Dict]:
    """
    Walk models_dir recursively and return one entry per .sql file:
        {model_name, layer, sql_path}
    """
    entries = []
    base = Path(models_dir)
    for sql_path in sorted(base.rglob("*.sql")):
        folder = sql_path.parent.name.lower()
        layer  = _get_layer(folder, sql_path.stem)
        entries.append(
            {
                "model_name": sql_path.stem,
                "layer":      layer,
                "sql_path":   str(sql_path),
            }
        )
    return entries


# ===========================================================================
# MAIN RUNNER
# ===========================================================================

def build_and_load_lineage(models_dir: str = MODELS_DIR, verbose: bool = True):
    """
    Parse all dbt SQL models in models_dir, derive column lineage, and
    upsert everything into dbo.model_lineage.

    Returns the total number of lineage rows written.
    """
    print("\n================ LINEAGE BUILDER ================")
    start = datetime.utcnow()

    # Ensure the target table exists
    ensure_lineage_table()

    model_entries = discover_models(models_dir)
    if not model_entries:
        print(f"  [lineage] No .sql files found under {models_dir}")
        return 0

    total_rows = 0
    model_count = 0

    for entry in model_entries:
        model_name = entry["model_name"]
        layer      = entry["layer"]
        sql_path   = entry["sql_path"]

        try:
            with open(sql_path, encoding="utf-8", errors="replace") as fh:
                sql_raw = fh.read()

            cols = _parse_columns(sql_raw)

            if not cols:
                if verbose:
                    print(f"  [lineage] {model_name}: 0 columns parsed — skipping")
                continue

            # Build row dicts for DB upsert
            db_rows = []
            for col in cols:
                db_rows.append(
                    {
                        "model":               model_name,
                        "column_name":         col["output_col"],
                        "source_table":        col.get("upstream", ""),
                        "source_column":       col.get("source_col", ""),
                        "transformation_type": col["transform_type"],
                        "expression":          col.get("expression", ""),
                        "layer":               layer,
                    }
                )

            # Delete existing rows for this model then upsert fresh rows
            delete_model_rows(model_name)
            upsert_lineage_rows(db_rows)

            total_rows += len(db_rows)
            model_count += 1

            if verbose:
                # Summarise transform types
                tc: Dict[str, int] = {}
                for r in db_rows:
                    tc[r["transformation_type"]] = tc.get(r["transformation_type"], 0) + 1
                tc_str = "  ".join(f"{k}: {v}" for k, v in sorted(tc.items()))
                print(f"  [lineage] {model_name:<30} {len(db_rows):>3} cols  |  {tc_str}")

        except Exception as exc:
            print(f"  [lineage] ERROR processing {model_name}: {exc}")

    elapsed = (datetime.utcnow() - start).total_seconds()
    print(
        f"\n  [lineage] COMPLETE — {model_count} models, "
        f"{total_rows} lineage rows written in {elapsed:.1f}s"
    )
    print("=================================================\n")
    return total_rows


# ===========================================================================
# STANDALONE ENTRY POINT
# ===========================================================================
if __name__ == "__main__":
    # Allow overriding models directory from command line
    models_path = sys.argv[1] if len(sys.argv) > 1 else MODELS_DIR
    build_and_load_lineage(models_dir=models_path, verbose=True)
