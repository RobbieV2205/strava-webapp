"""
Strava Sync
-----------

Creates database and user (if needed) and sync's all runs to database.
"""


import time
import logging
import mysql.connector
import logging.handlers
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv, set_key
from database import connect, upsert_runs, setup_database
from strava_api import get_access_token, fetch_all_runs
import os

# Configure the logging system.
_LOG_PATH = Path(__file__).parent / "strava_sync.log"
_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

_handler_console = logging.StreamHandler()
_handler_console.setFormatter(_formatter)

_handler_file = logging.handlers.RotatingFileHandler(
    _LOG_PATH, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
)
_handler_file.setFormatter(_formatter)

logging.basicConfig(level=logging.INFO, handlers=[_handler_console, _handler_file])
log = logging.getLogger(__name__)

# Set env path and interval loop time.
_ENV_PATH = Path(__file__).parent / ".env"
SYNC_INTERVAL_SECONDS = 6 * 60 * 60  # 6 uur


def ensure_env():
    """ ensures .env is available and if necessary data is available. raises error and stops if not."""

    if not _ENV_PATH.exists():
        raise SystemExit(
            f".env file not found."
        )
    load_dotenv(_ENV_PATH, override=True)
    
    from database import ROOT_PASSWORD, MYSQL_PASSWORD, ROOT_PASSWORD, MYSQL_PASSWORD
    from strava_api import CLIENT_ID, CLIENT_SECRET

    if not CLIENT_ID or not CLIENT_SECRET:
        raise RuntimeError("STRAVA_CLIENT_ID or STRAVA_CLIENT_SECRET is missing in .env")
    if not ROOT_PASSWORD:
        raise RuntimeError("MYSQL_ROOT_PASSWORD missing in .env")
    if not MYSQL_PASSWORD:
        raise RuntimeError("MYSQL_PASSWORD missing in .env")
    

def ensure_database():
    """Ensures database is available. set up database if not and sets database crated flag to treu
       If datbase not available and flag is set to true it raises error.
    """

    already_initialized = os.getenv("DB_INITIALIZED", "").lower() == "true"

    try:
        conn = connect()
        conn.close()
    except mysql.connector.Error as exc:
        if already_initialized:
            raise SystemExit(
                "Database not available after creation please check database."
                f"System raised error: {exc}"
            )
        setup_database()
        log.info("Database created.")
        set_key(_ENV_PATH, "DB_INITIALIZED", "true")
        load_dotenv(_ENV_PATH, override=True)


def sync_once():
    """funtion used in the loop the sync the strava data to the database. """

    load_dotenv(_ENV_PATH, override=True)

    token = get_access_token()
    conn = connect()
    runs = fetch_all_runs(token)
    
    if runs:
        upsert_runs(conn, runs)
    else:
        log.info("No runs found.")

    conn.close()


def main():
    ensure_env()
    ensure_database()

    log.info("=" * 60)
    log.info("Server started — sync each %d uur. Logfile: %s", SYNC_INTERVAL_SECONDS // 3600, _LOG_PATH)
    log.info("=" * 60)

    cycle = 0
    while True:
        cycle += 1
        log.info("--- Loop #%d started at %s ---", cycle, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        try:
            sync_once()
        except Exception as exc:
            log.exception("Sync failed: %s", exc)

        next_run = datetime.fromtimestamp(time.time() + SYNC_INTERVAL_SECONDS).strftime("%Y-%m-%d %H:%M:%S")
        time.sleep(SYNC_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
