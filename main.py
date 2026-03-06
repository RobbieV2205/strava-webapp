"""
Strava Sync
-----------
Maakt de database en gebruiker aan (indien nodig) en synchroniseert
alle runs van Strava naar MySQL.

Gebruik:
    1. Voer eenmalig uit: python auth.py   (Strava token ophalen)
    2. Vul .env in met database- en Strava-gegevens
    3. python main.py

Server-modus: draait een sync elke 6 uur in een loop.
"""

import time
import logging
import logging.handlers
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

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

_ENV_PATH = Path(__file__).parent / ".env"
SYNC_INTERVAL_SECONDS = 6 * 60 * 60  # 6 uur

_ENV_TEMPLATE = """\
# Strava credentials — haal op via https://www.strava.com/settings/api
STRAVA_CLIENT_ID=
STRAVA_CLIENT_SECRET=
STRAVA_REDIRECT_URI=http://localhost:8080/callback

# MySQL
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=strava_data
MYSQL_USER=strava_user
MYSQL_PASSWORD=
MYSQL_ROOT_USER=root
MYSQL_ROOT_PASSWORD=
"""


def ensure_env():
    if not _ENV_PATH.exists():
        _ENV_PATH.write_text(_ENV_TEMPLATE)
        raise SystemExit(
            f"[setup] .env aangemaakt in {_ENV_PATH.resolve()}\n"
            "[setup] Vul de waarden in en start opnieuw."
        )
    load_dotenv(_ENV_PATH)


def sync_once():
    """Voert één sync-cyclus uit. Gooit een exception bij fouten."""
    from database import (
        ROOT_PASSWORD, MYSQL_PASSWORD, MYSQL_USER, MYSQL_DATABASE, MYSQL_HOST,
        setup_database, connect, create_table, upsert_runs,
    )
    from strava_api import CLIENT_ID, CLIENT_SECRET, get_access_token, fetch_all_runs

    if not CLIENT_ID or not CLIENT_SECRET:
        raise RuntimeError("Vul STRAVA_CLIENT_ID en STRAVA_CLIENT_SECRET in in .env")
    if not ROOT_PASSWORD:
        raise RuntimeError("Vul MYSQL_ROOT_PASSWORD in in .env")
    if not MYSQL_PASSWORD:
        raise RuntimeError("Vul MYSQL_PASSWORD in in .env")

    setup_database()

    log.info("Verbinden met MySQL als %s...", MYSQL_USER)
    conn = connect()
    create_table(conn)

    token = get_access_token()

    runs = fetch_all_runs(token)
    if runs:
        log.info("Data schrijven — %d runs worden opgeslagen naar %s @ %s...", len(runs), MYSQL_DATABASE, MYSQL_HOST)
        upsert_runs(conn, runs)
        log.info("Data geschreven op %s — %d runs opgeslagen.", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), len(runs))
    else:
        log.info("Geen nieuwe runs gevonden — niets geschreven.")

    conn.close()


def main():
    ensure_env()

    # Imports na load_dotenv zodat os.getenv() de juiste waarden leest
    from database import ROOT_PASSWORD, MYSQL_PASSWORD
    from strava_api import CLIENT_ID, CLIENT_SECRET

    if not CLIENT_ID or not CLIENT_SECRET:
        log.critical("STRAVA_CLIENT_ID of STRAVA_CLIENT_SECRET ontbreekt in .env")
        raise SystemExit("ERROR: Vul STRAVA_CLIENT_ID en STRAVA_CLIENT_SECRET in in .env")
    if not ROOT_PASSWORD:
        log.critical("MYSQL_ROOT_PASSWORD ontbreekt in .env")
        raise SystemExit("ERROR: Vul MYSQL_ROOT_PASSWORD in in .env")
    if not MYSQL_PASSWORD:
        log.critical("MYSQL_PASSWORD ontbreekt in .env")
        raise SystemExit("ERROR: Vul MYSQL_PASSWORD in in .env")

    log.info("=" * 60)
    log.info("Server gestart — sync elke %d uur. Logbestand: %s", SYNC_INTERVAL_SECONDS // 3600, _LOG_PATH)
    log.info("=" * 60)

    cycle = 0
    while True:
        cycle += 1
        log.info("--- Loop #%d gestart op %s ---", cycle, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        try:
            sync_once()
        except Exception as exc:
            log.exception("Sync mislukt: %s", exc)

        next_run = datetime.fromtimestamp(time.time() + SYNC_INTERVAL_SECONDS).strftime("%Y-%m-%d %H:%M:%S")
        log.info("--- Loop #%d klaar. Volgende sync om %s ---", cycle, next_run)
        time.sleep(SYNC_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
