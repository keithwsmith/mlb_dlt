"""
DB connection helper for the MLB app.

Uses the same SQL Server instance as mlb_load.py, but reads/writes are
scoped to the `silver` schema (plus `dw.mlbplayers` for player bios,
per the spec). Connection string mirrors DW_CONNECTION_STRING from
mlb_load.py — update here if credentials ever change, no need to touch
every query file.
"""
import struct
import datetime
import pyodbc
from contextlib import contextmanager

CONNECTION_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=KEITH-PERSONAL;"
    "DATABASE=dlt;"
    "username=sa;"
    "password=pass0123;"
    "Trusted_Connection=yes;"
    "trust_server_certificate=true;"
)

# pyodbc has no native support for SQL Server's DATETIMEOFFSET type (used by
# columns like silver.dim_game_details.first_pitch). Without this converter,
# reading a row containing one raises a low-level ODBC error mid-fetchall(),
# which breaks the HTTP response stream — the browser sees a raw network
# failure ("Failed to fetch") instead of a clean 500 with a real error message.
# https://github.com/mkleehammer/pyodbc/issues/134 — this converter is the
# standard fix.
SQL_SS_TIMESTAMPOFFSET = -155


def _handle_datetimeoffset(dto_value: bytes) -> datetime.datetime:
    # Raw bytes layout: 6 shorts (Y, M, D, h, m, s), an unsigned int
    # (nanoseconds), then 2 shorts (tz hour offset, tz minute offset).
    tup = struct.unpack("<6hI2h", dto_value)
    return datetime.datetime(
        tup[0], tup[1], tup[2], tup[3], tup[4], tup[5], tup[6] // 1000,
        datetime.timezone(datetime.timedelta(hours=tup[7], minutes=tup[8])),
    )


@contextmanager
def get_cursor():
    conn = pyodbc.connect(CONNECTION_STRING, timeout=10)
    conn.add_output_converter(SQL_SS_TIMESTAMPOFFSET, _handle_datetimeoffset)
    try:
        cursor = conn.cursor()
        yield cursor
        conn.commit()
    finally:
        conn.close()


def run_query(sql: str, params: tuple = ()) -> list[dict]:
    """Run a SELECT and return rows as a list of dicts."""
    with get_cursor() as cursor:
        cursor.execute(sql, params)
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def run_query_one(sql: str, params: tuple = ()) -> dict | None:
    rows = run_query(sql, params)
    return rows[0] if rows else None