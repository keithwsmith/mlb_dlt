import json
import urllib.request
import datetime
import pyodbc
import sys
import subprocess
import os

# Database credentials (from user)
DRIVER = "ODBC Driver 17 for SQL Server"
SERVER = "KEITH-PERSONAL"
DATABASE = "dlt"
UID = "sa"
PWD = "pass0123"
TRUST = "yes"

DW_CONNECTION_STRING = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={UID};"
    f"PWD={PWD};"
    f"TrustServerCertificate={TRUST};"
)

def get_max_game_date():
    try:
        conn = pyodbc.connect(DW_CONNECTION_STRING, timeout=10)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT MAX(CAST(game_date AS DATETIME))
            FROM dw.games
        """)
        row = cursor.fetchone()
        conn.close()

        if row and row[0]:
            return row[0]
        return None

    except pyodbc.Error as e:
        print(f"Could not query max game_date: {e}")
        print("   Falling back to full load.")
        return None


def fetch_schedule(start_date: str, end_date: str):
    url = (
        f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate={start_date}&endDate={end_date}"
    )
    with urllib.request.urlopen(url, timeout=20) as resp:
        data = json.load(resp)
    return data


def parse_latest_game(schedule_json: dict):
    dates = schedule_json.get("dates", [])
    if not dates:
        return None

    games = dates[0].get("games", [])
    if not games:
        return None

    latest = None
    for g in games:
        gd = g.get("gameDate")
        if not gd:
            continue
        # normalize ISO with Z -> +00:00 for fromisoformat
        if gd.endswith("Z"):
            gd2 = gd.replace("Z", "+00:00")
        else:
            gd2 = gd
        try:
            dt = datetime.datetime.fromisoformat(gd2)
        except Exception:
            continue

        if latest is None or dt > latest["gameDate"]:
            latest = {"gamePk": g.get("gamePk"), "gameDate": dt}

    return latest


def main():
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    start_date = yesterday.isoformat()
    end_date = today.isoformat()

    print(f"Fetching schedule {start_date} -> {end_date}")
    try:
        schedule = fetch_schedule(start_date, end_date)
    except Exception as e:
        print(f"Failed to fetch schedule: {e}")
        sys.exit(2)

    latest = parse_latest_game(schedule)
    if not latest:
        print("No latest game found in schedule JSON.")
        sys.exit(0)

    latest_dt = latest["gameDate"]
    print(f"Latest game: gamePk={latest['gamePk']} gameDate={latest_dt.isoformat()}")

    db_max = get_max_game_date()
    print(f"DB max game_date: {db_max}")

    # normalize db_max to timezone-aware UTC if naive so it can be compared
    if db_max is not None and db_max.tzinfo is None:
        db_max = db_max.replace(tzinfo=datetime.timezone.utc)

    if db_max is None:
        print("DB has no max game_date (or query failed). Not running pipeline by rule.")
        sys.exit(0)

    # Per user instruction: run pipeline if gameDate is less than DB max
    if latest_dt > db_max:
        print("Latest gameDate is more than DB max. Running pipeline.py...")
        pipeline_path = r"C:\Users\Keith\PycharmProjects\mlb_orchestration\pipeline.py"
        if not os.path.exists(pipeline_path):
            print(f"pipeline.py not found at {pipeline_path}")
            sys.exit(1)

        try:
            res = subprocess.run([sys.executable, pipeline_path], check=False)
            print(f"pipeline.py exited with returncode={res.returncode}")
        except Exception as e:
            print(f"Failed to run pipeline.py: {e}")
            sys.exit(1)
    else:
        print("Condition not met: not running pipeline.py.")


if __name__ == "__main__":
    main()
