"""
Strava → MySQL Sync
-------------------
Haalt runs op van de Strava API en schrijft ze direct naar MySQL.
Vervangt het apart draaien van strava_export.py + strava_to_mysql.py.

Vereisten:
  - Database en gebruiker moeten al bestaan.
  - Alleen nieuwe of gewijzigde runs worden weggeschreven (upsert).

Gebruik:
    python strava_sync.py
"""

import json
import os
import time
import webbrowser
from datetime import datetime
from pathlib import Path
from threading import Thread
from urllib.parse import urlencode, urlparse, parse_qs
from http.server import HTTPServer, BaseHTTPRequestHandler

import requests
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

CLIENT_ID      = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET  = os.getenv("STRAVA_CLIENT_SECRET")
REDIRECT_URI   = os.getenv("STRAVA_REDIRECT_URI", "http://localhost:8080/callback")
TOKEN_FILE     = Path(os.getenv("STRAVA_TOKEN_FILE", "strava_tokens.json"))

MYSQL_HOST     = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "strava_data")
MYSQL_USER     = os.getenv("MYSQL_USER", "strava_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")

BASE_URL  = "https://www.strava.com/api/v3"
AUTH_URL  = "https://www.strava.com/oauth/authorize"
TOKEN_URL = "https://www.strava.com/oauth/token"

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

# ── Strava auth ───────────────────────────────────────────────────────────────

_auth_code: str | None = None


class _CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global _auth_code
        params = parse_qs(urlparse(self.path).query)
        if "code" in params:
            _auth_code = params["code"][0]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"<h2>Authorized! You can close this tab.</h2>")
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"<h2>Authorization failed.</h2>")

    def log_message(self, *args):
        pass


def _authorize() -> dict:
    port = int(urlparse(REDIRECT_URI).port or 8080)
    params = {
        "client_id":       CLIENT_ID,
        "redirect_uri":    REDIRECT_URI,
        "response_type":   "code",
        "approval_prompt": "auto",
        "scope":           "read,activity:read_all",
    }
    url = f"{AUTH_URL}?{urlencode(params)}"
    print(f"[auth] Browser openen voor Strava autorisatie...")
    print(f"[auth] Werkt de browser niet? Ga naar:\n  {url}\n")
    Thread(target=lambda: HTTPServer(("localhost", port), _CallbackHandler).handle_request(), daemon=True).start()
    webbrowser.open(url)
    for _ in range(120):
        if _auth_code:
            break
        time.sleep(1)
    if not _auth_code:
        raise RuntimeError("Autorisatie timeout — geen code ontvangen.")
    resp = requests.post(TOKEN_URL, data={
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code":          _auth_code,
        "grant_type":    "authorization_code",
    })
    resp.raise_for_status()
    tokens = resp.json()
    TOKEN_FILE.write_text(json.dumps(tokens, indent=2))
    print("[auth] Token opgeslagen.")
    return tokens


def _get_access_token() -> str:
    tokens = json.loads(TOKEN_FILE.read_text()) if TOKEN_FILE.exists() else None
    if tokens is None:
        tokens = _authorize()
    if tokens.get("expires_at", 0) < time.time() + 60:
        print("[auth] Token verlopen — vernieuwen...")
        resp = requests.post(TOKEN_URL, data={
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": tokens["refresh_token"],
            "grant_type":    "refresh_token",
        })
        resp.raise_for_status()
        tokens = resp.json()
        TOKEN_FILE.write_text(json.dumps(tokens, indent=2))
        print("[auth] Token vernieuwd.")
    return tokens["access_token"]


# ── Strava API ────────────────────────────────────────────────────────────────

def _api_get(endpoint: str, token: str, params: dict = {}) -> list | dict:
    resp = requests.get(f"{BASE_URL}{endpoint}", headers={"Authorization": f"Bearer {token}"}, params=params)
    if resp.status_code == 429:
        wait = max(int(resp.headers.get("X-RateLimit-Reset", 0)) - int(time.time()), 60)
        print(f"[api] Rate limit — wachten {wait}s...")
        time.sleep(wait)
        return _api_get(endpoint, token, params)
    resp.raise_for_status()
    return resp.json()


def _fetch_runs(token: str) -> list[dict]:
    """Haal alle runs op van Strava (gepagineerd)."""
    runs, page = [], 1
    print("[strava] Activiteiten ophalen...")
    while True:
        batch = _api_get("/athlete/activities", token, {"per_page": 200, "page": page})
        if not batch:
            break
        # Filter op runs
        run_batch = [a for a in batch if a.get("sport_type") in ("Run", "TrailRun", "VirtualRun")
                                      or a.get("type") == "Run"]
        runs.extend(run_batch)
        print(f"[strava]   pagina {page}: {len(batch)} activiteiten, {len(run_batch)} runs (totaal runs: {len(runs)})")
        page += 1
    print(f"[strava] Klaar — {len(runs)} runs opgehaald.")
    return runs


# ── MySQL setup ───────────────────────────────────────────────────────────────

def _connect() -> mysql.connector.MySQLConnection:
    return mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )


def _ensure_table(conn):
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    cur.close()


# ── Import ────────────────────────────────────────────────────────────────────

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


def _upsert_runs(conn, runs: list[dict]):
    all_cols     = ["id"] + COLUMNS
    col_list     = ", ".join(f"`{c}`" for c in all_cols)
    placeholders = ", ".join(["%s"] * len(all_cols))
    updates      = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in COLUMNS)
    sql = f"INSERT INTO runs ({col_list}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates};"

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


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        raise SystemExit("ERROR: Vul STRAVA_CLIENT_ID en STRAVA_CLIENT_SECRET in in .env")
    if not MYSQL_PASSWORD:
        raise SystemExit("ERROR: Vul MYSQL_PASSWORD in in .env")

    # Stap 1 — Strava token ophalen
    token = _get_access_token()

    # Stap 2 — Verbinden en tabel aanmaken
    print(f"[mysql] Verbinden als {MYSQL_USER}...")
    conn = _connect()
    _ensure_table(conn)

    # Stap 3 — Runs ophalen en wegschrijven
    runs = _fetch_runs(token)
    if runs:
        _upsert_runs(conn, runs)
    else:
        print("[mysql] Geen runs gevonden.")

    conn.close()
    print(f"\n[klaar] Sync voltooid. Database: {MYSQL_DATABASE} @ {MYSQL_HOST}")


if __name__ == "__main__":
    main()
