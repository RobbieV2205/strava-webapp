"""
Strava Sync
-----------
Maakt de database en gebruiker aan (indien nodig) en synchroniseert
alle runs van Strava naar MySQL.

Gebruik:
    1. Voer eenmalig uit: python auth.py   (Strava token ophalen)
    2. Vul .env in met database- en Strava-gegevens
    3. python main.py
"""

from pathlib import Path
from dotenv import load_dotenv

_ENV_PATH = Path(".env")

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
        print(f"[setup] .env aangemaakt in {_ENV_PATH.resolve()}")
        print("[setup] Vul de waarden in en druk daarna op Enter om door te gaan.")
        input("[setup] Druk op Enter als .env is ingevuld: ")
    load_dotenv(_ENV_PATH)


def main():
    ensure_env()

    # Imports na load_dotenv zodat os.getenv() de juiste waarden leest
    from database import (
        ROOT_PASSWORD, MYSQL_PASSWORD, MYSQL_USER, MYSQL_DATABASE, MYSQL_HOST,
        setup_database, connect, create_table, upsert_runs,
    )
    from strava_api import CLIENT_ID, CLIENT_SECRET, get_access_token, fetch_all_runs

    if not CLIENT_ID or not CLIENT_SECRET:
        raise SystemExit("ERROR: Vul STRAVA_CLIENT_ID en STRAVA_CLIENT_SECRET in in .env")
    if not ROOT_PASSWORD:
        raise SystemExit("ERROR: Vul MYSQL_ROOT_PASSWORD in in .env")
    if not MYSQL_PASSWORD:
        raise SystemExit("ERROR: Vul MYSQL_PASSWORD in in .env")

    # Stap 1 — Database en gebruiker aanmaken
    setup_database()

    # Stap 2 — Verbinden en tabel aanmaken
    print(f"[mysql] Verbinden als {MYSQL_USER}...")
    conn = connect()
    create_table(conn)

    # Stap 3 — Token ophalen (auto-refresh indien verlopen)
    token = get_access_token()

    # Stap 4 — Runs ophalen en wegschrijven
    runs = fetch_all_runs(token)
    if runs:
        upsert_runs(conn, runs)
    else:
        print("[mysql] Geen runs gevonden.")

    conn.close()
    print(f"\n[klaar] Sync voltooid. Database: {MYSQL_DATABASE} @ {MYSQL_HOST}")


if __name__ == "__main__":
    main()
