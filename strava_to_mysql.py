"""
Strava → MySQL Import Tool
--------------------------
Reads the most recent run.json export and writes it to a MySQL database.

First run:
  - Connects as root to create the database and application user
  - Creates the `runs` table

Subsequent runs:
  - Upserts rows (INSERT ... ON DUPLICATE KEY UPDATE) so re-running is safe

Usage:
    1. Fill in the MySQL credentials in .env
    2. Run: python strava_to_mysql.py
"""

import json
import os
from datetime import datetime
from pathlib import Path

import mysql.connector
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

MYSQL_HOST     = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "strava_data")
MYSQL_USER     = os.getenv("MYSQL_USER", "strava_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
ROOT_USER      = os.getenv("MYSQL_ROOT_USER", "root")
ROOT_PASSWORD  = os.getenv("MYSQL_ROOT_PASSWORD", "")
OUTPUT_DIR     = Path(os.getenv("OUTPUT_DIR", "output"))

# ── SQL ───────────────────────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS runs (
    id                       BIGINT PRIMARY KEY,
    name                     VARCHAR(255),
    sport_type               VARCHAR(50),
    distance                 FLOAT COMMENT 'meters',
    moving_time              INT   COMMENT 'seconds',
    elapsed_time             INT   COMMENT 'seconds',
    total_elevation_gain     FLOAT COMMENT 'meters',
    elev_high                FLOAT,
    elev_low                 FLOAT,
    start_date               DATETIME,
    start_date_local         DATETIME,
    timezone                 VARCHAR(100),
    average_speed            FLOAT COMMENT 'm/s',
    max_speed                FLOAT COMMENT 'm/s',
    average_cadence          FLOAT,
    average_watts            FLOAT,
    max_watts                INT,
    weighted_average_watts   INT,
    kilojoules               FLOAT,
    average_heartrate        FLOAT,
    max_heartrate            FLOAT,
    suffer_score             INT,
    calories                 FLOAT,
    kudos_count              INT,
    comment_count            INT,
    achievement_count        INT,
    pr_count                 INT,
    start_latlng             JSON,
    end_latlng               JSON,
    map_summary_polyline     LONGTEXT,
    gear_id                  VARCHAR(50),
    commute                  TINYINT(1),
    trainer                  TINYINT(1),
    manual                   TINYINT(1),
    private                  TINYINT(1),
    flagged                  TINYINT(1),
    workout_type             INT,
    description              TEXT,
    imported_at              DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# Columns that map directly from JSON → table (excluding 'id' which is the key)
COLUMNS = [
    "name", "sport_type", "distance", "moving_time", "elapsed_time",
    "total_elevation_gain", "elev_high", "elev_low",
    "start_date", "start_date_local", "timezone",
    "average_speed", "max_speed", "average_cadence",
    "average_watts", "max_watts", "weighted_average_watts", "kilojoules",
    "average_heartrate", "max_heartrate", "suffer_score", "calories",
    "kudos_count", "comment_count", "achievement_count", "pr_count",
    "start_latlng", "end_latlng", "map_summary_polyline",
    "gear_id", "commute", "trainer", "manual", "private",
    "flagged", "workout_type", "description",
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_datetime(value: str | None) -> str | None:
    """Convert ISO-8601 string to MySQL DATETIME format."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _coerce(col: str, value) -> object:
    """Coerce a JSON value to a MySQL-compatible Python type."""
    if value is None:
        return None
    if col in ("start_date", "start_date_local"):
        return _parse_datetime(value)
    if col in ("start_latlng", "end_latlng"):
        return json.dumps(value) if isinstance(value, list) else value
    if col in ("commute", "trainer", "manual", "private", "flagged"):
        return int(bool(value))
    return value


def _latest_run_json() -> Path:
    """Return path to run.json in the most recent export folder."""
    runs = sorted(OUTPUT_DIR.glob("*/json/run.json"))
    if not runs:
        raise FileNotFoundError(
            f"No run.json found in {OUTPUT_DIR}/*/json/. "
            "Run strava_export.py first."
        )
    return runs[-1]


# ── Database setup ────────────────────────────────────────────────────────────

def setup_database():
    """Create database and application user using root credentials."""
    print(f"[setup] Connecting as root to {MYSQL_HOST}:{MYSQL_PORT}...")
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=ROOT_USER,
        password=ROOT_PASSWORD,
    )
    cur = conn.cursor()

    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
    print(f"[setup] Database `{MYSQL_DATABASE}` ready.")

    # Create user if not exists (MySQL 5.7+ syntax)
    cur.execute(
        f"CREATE USER IF NOT EXISTS '{MYSQL_USER}'@'%' IDENTIFIED BY '{MYSQL_PASSWORD}';"
    )
    cur.execute(
        f"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, INDEX ON `{MYSQL_DATABASE}`.* TO '{MYSQL_USER}'@'%';"
    )
    cur.execute("FLUSH PRIVILEGES;")
    print(f"[setup] User `{MYSQL_USER}` created/updated with privileges on `{MYSQL_DATABASE}`.")

    cur.close()
    conn.close()


def create_table(conn):
    cur = conn.cursor()
    cur.execute(f"USE `{MYSQL_DATABASE}`;")
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    cur.close()
    print("[setup] Table `runs` ready.")


# ── Import ────────────────────────────────────────────────────────────────────

def build_upsert_sql() -> str:
    all_cols  = ["id"] + COLUMNS
    col_list  = ", ".join(f"`{c}`" for c in all_cols)
    placeholders = ", ".join(["%s"] * len(all_cols))
    updates   = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in COLUMNS)
    return (
        f"INSERT INTO runs ({col_list}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {updates};"
    )


def import_runs(conn, path: Path):
    print(f"[import] Reading {path}...")
    activities = json.loads(path.read_text(encoding="utf-8"))
    print(f"[import] {len(activities)} runs to import...")

    sql = build_upsert_sql()
    cur = conn.cursor()
    inserted = updated = 0

    for act in activities:
        row = [act.get("id")] + [_coerce(col, act.get(col)) for col in COLUMNS]
        cur.execute(sql, row)
        # affected_rows: 1 = insert, 2 = update, 0 = no change
        if cur.rowcount == 1:
            inserted += 1
        elif cur.rowcount == 2:
            updated += 1

    conn.commit()
    cur.close()
    print(f"[import] Done — {inserted} inserted, {updated} updated.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not ROOT_PASSWORD:
        raise SystemExit(
            "ERROR: MYSQL_ROOT_PASSWORD is not set in .env\n"
            "Fill in the root credentials and try again."
        )
    if not MYSQL_PASSWORD:
        raise SystemExit(
            "ERROR: MYSQL_PASSWORD is not set in .env\n"
            "Choose a password for the strava_user account."
        )

    setup_database()

    print(f"[db] Connecting as {MYSQL_USER} to {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}...")
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )

    create_table(conn)

    run_json = _latest_run_json()
    import_runs(conn, run_json)

    conn.close()
    print(f"\n[done] Import complete. Database: {MYSQL_DATABASE} @ {MYSQL_HOST}")


if __name__ == "__main__":
    main()
