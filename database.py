import json
import os
from datetime import datetime

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

# ── Setup ─────────────────────────────────────────────────────────────────────

def setup_database():
    print(f"[setup] Verbinden als root met {MYSQL_HOST}:{MYSQL_PORT}...")
    conn = mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=ROOT_USER, password=ROOT_PASSWORD,
    )
    cur = conn.cursor()
    cur.execute(
        f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` "
        f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    )
    print(f"[setup] Database `{MYSQL_DATABASE}` gereed.")
    cur.execute(
        f"CREATE USER IF NOT EXISTS '{MYSQL_USER}'@'%' IDENTIFIED BY '{MYSQL_PASSWORD}';"
    )
    cur.execute(
        f"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, INDEX "
        f"ON `{MYSQL_DATABASE}`.* TO '{MYSQL_USER}'@'%';"
    )
    cur.execute("FLUSH PRIVILEGES;")
    print(f"[setup] Gebruiker `{MYSQL_USER}` aangemaakt met rechten op `{MYSQL_DATABASE}`.")
    cur.close()
    conn.close()


def connect() -> mysql.connector.MySQLConnection:
    return mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )


def create_table(conn):
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    cur.close()
    print("[setup] Tabel `runs` gereed.")

# ── Upsert ────────────────────────────────────────────────────────────────────

def _parse_datetime(value: str | None) -> str | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _coerce(col: str, value):
    if value is None:
        return None
    if col in ("start_date", "start_date_local"):
        return _parse_datetime(value)
    if col in ("start_latlng", "end_latlng"):
        return json.dumps(value) if isinstance(value, list) else value
    if col in ("commute", "trainer", "manual", "private", "flagged"):
        return int(bool(value))
    return value


def upsert_runs(conn, runs: list[dict]):
    all_cols     = ["id"] + COLUMNS
    col_list     = ", ".join(f"`{c}`" for c in all_cols)
    placeholders = ", ".join(["%s"] * len(all_cols))
    updates      = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in COLUMNS)
    sql = (
        f"INSERT INTO runs ({col_list}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {updates};"
    )

    cur = conn.cursor()
    inserted = updated = skipped = 0
    for act in runs:
        row = [act.get("id")] + [_coerce(col, act.get(col)) for col in COLUMNS]
        cur.execute(sql, row)
        if cur.rowcount == 1:
            inserted += 1
        elif cur.rowcount == 2:
            updated += 1
        else:
            skipped += 1
    conn.commit()
    cur.close()
    print(f"[mysql] {inserted} nieuw  |  {updated} bijgewerkt  |  {skipped} ongewijzigd")
