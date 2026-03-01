"""
Strava API Export Tool
----------------------
Fetches all activities from the Strava API and exports them to JSON and CSV,
sorted per activity type.

Usage:
    1. Copy .env.example to .env and fill in your Strava API credentials
    2. Run: python strava_export.py
    3. On first run, follow the OAuth2 authorization flow in your browser

Requirements:
    pip install -r requirements.txt
"""

import os
import json
import csv
import time
import webbrowser
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode, urlparse, parse_qs
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread

import requests
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

CLIENT_ID     = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
REDIRECT_URI  = os.getenv("STRAVA_REDIRECT_URI", "http://localhost:8080/callback")
TOKEN_FILE    = Path(os.getenv("STRAVA_TOKEN_FILE", "strava_tokens.json"))
OUTPUT_DIR    = Path(os.getenv("OUTPUT_DIR", "output"))

BASE_URL      = "https://www.strava.com/api/v3"
AUTH_URL      = "https://www.strava.com/oauth/authorize"
TOKEN_URL     = "https://www.strava.com/oauth/token"

# CSV columns — add/remove fields as needed
CSV_FIELDS = [
    "id", "name", "sport_type", "type", "distance", "moving_time",
    "elapsed_time", "total_elevation_gain", "elev_high", "elev_low",
    "start_date", "start_date_local", "timezone", "average_speed",
    "max_speed", "average_cadence", "average_watts", "max_watts",
    "weighted_average_watts", "kilojoules", "average_heartrate",
    "max_heartrate", "suffer_score", "calories", "kudos_count",
    "comment_count", "achievement_count", "pr_count",
    "start_latlng", "end_latlng", "map_summary_polyline",
    "gear_id", "commute", "trainer", "manual", "private",
    "flagged", "workout_type", "description",
]

# ── OAuth2 helpers ────────────────────────────────────────────────────────────

_auth_code: str | None = None


class _CallbackHandler(BaseHTTPRequestHandler):
    """Tiny HTTP server that captures the OAuth2 callback code."""

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
        pass  # suppress access log


def _run_callback_server(port: int = 8080):
    server = HTTPServer(("localhost", port), _CallbackHandler)
    server.handle_request()  # serve exactly one request then stop


def authorize() -> dict:
    """Run the full OAuth2 authorization code flow and return token data."""
    port = int(urlparse(REDIRECT_URI).port or 8080)

    params = {
        "client_id":     CLIENT_ID,
        "redirect_uri":  REDIRECT_URI,
        "response_type": "code",
        "approval_prompt": "auto",
        "scope":         "read,activity:read_all",
    }
    url = f"{AUTH_URL}?{urlencode(params)}"

    print(f"\n[auth] Opening browser for Strava authorization...")
    print(f"[auth] If the browser does not open automatically, go to:\n  {url}\n")

    thread = Thread(target=_run_callback_server, args=(port,), daemon=True)
    thread.start()
    webbrowser.open(url)
    thread.join(timeout=120)

    if _auth_code is None:
        raise RuntimeError("Authorization timed out. No code received.")

    resp = requests.post(TOKEN_URL, data={
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code":          _auth_code,
        "grant_type":    "authorization_code",
    })
    resp.raise_for_status()
    token_data = resp.json()
    _save_tokens(token_data)
    print("[auth] Tokens saved successfully.")
    return token_data


def _save_tokens(token_data: dict):
    TOKEN_FILE.write_text(json.dumps(token_data, indent=2))


def _load_tokens() -> dict | None:
    if TOKEN_FILE.exists():
        return json.loads(TOKEN_FILE.read_text())
    return None


def get_access_token() -> str:
    """Return a valid access token, refreshing if necessary."""
    tokens = _load_tokens()

    if tokens is None:
        tokens = authorize()

    # Refresh if expired (with 60 s buffer)
    if tokens.get("expires_at", 0) < time.time() + 60:
        print("[auth] Access token expired — refreshing...")
        resp = requests.post(TOKEN_URL, data={
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": tokens["refresh_token"],
            "grant_type":    "refresh_token",
        })
        resp.raise_for_status()
        tokens = resp.json()
        _save_tokens(tokens)
        print("[auth] Token refreshed.")

    return tokens["access_token"]


# ── API helpers ───────────────────────────────────────────────────────────────

def _get(endpoint: str, token: str, params: dict | None = None) -> dict | list:
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(f"{BASE_URL}{endpoint}", headers=headers, params=params or {})

    # Handle rate limiting
    if resp.status_code == 429:
        retry_after = int(resp.headers.get("X-RateLimit-Reset", 900))
        wait = max(retry_after - int(time.time()), 60)
        print(f"[api] Rate limited — waiting {wait}s...")
        time.sleep(wait)
        return _get(endpoint, token, params)

    resp.raise_for_status()
    return resp.json()


def fetch_all_activities(token: str) -> list[dict]:
    """Fetch every activity for the authenticated athlete (handles pagination)."""
    activities = []
    page = 1

    print("[fetch] Downloading activities...")
    while True:
        batch = _get("/athlete/activities", token, {
            "per_page": 200,
            "page":     page,
        })
        if not batch:
            break
        activities.extend(batch)
        print(f"[fetch]   page {page}: {len(batch)} activities (total: {len(activities)})")
        page += 1

    print(f"[fetch] Done — {len(activities)} activities fetched.")
    return activities


def fetch_activity_detail(activity_id: int, token: str) -> dict:
    """Fetch detailed data for a single activity (includes segments, laps, etc.)."""
    return _get(f"/activities/{activity_id}", token, {"include_all_efforts": True})


# ── Export helpers ────────────────────────────────────────────────────────────

def _flat_value(v) -> str:
    """Flatten nested values for CSV output."""
    if isinstance(v, (list, dict)):
        return json.dumps(v)
    return str(v) if v is not None else ""


def export_json(activities_by_type: dict[str, list], out_dir: Path):
    """Write one JSON file per activity type + one combined file."""
    out_dir.mkdir(parents=True, exist_ok=True)

    all_activities = []
    for sport_type, acts in sorted(activities_by_type.items()):
        all_activities.extend(acts)
        path = out_dir / f"{sport_type.lower()}.json"
        path.write_text(json.dumps(acts, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"[json] {path} ({len(acts)} activities)")

    combined_path = out_dir / "all_activities.json"
    combined_path.write_text(json.dumps(all_activities, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[json] {combined_path} ({len(all_activities)} total)")


def export_csv(activities_by_type: dict[str, list], out_dir: Path):
    """Write one CSV file per activity type + one combined file."""
    out_dir.mkdir(parents=True, exist_ok=True)

    all_rows = []
    for sport_type, acts in sorted(activities_by_type.items()):
        rows = []
        for act in acts:
            row = {field: _flat_value(act.get(field)) for field in CSV_FIELDS}
            # Add any extra fields not in the predefined list
            for k, v in act.items():
                if k not in row:
                    row[k] = _flat_value(v)
            rows.append(row)

        all_rows.extend(rows)
        path = out_dir / f"{sport_type.lower()}.csv"
        all_keys = list({k for r in rows for k in r})
        _write_csv(path, rows, all_keys)
        print(f"[csv]  {path} ({len(rows)} activities)")

    if all_rows:
        combined_path = out_dir / "all_activities.csv"
        all_keys = list({k for r in all_rows for k in r})
        _write_csv(combined_path, all_rows, all_keys)
        print(f"[csv]  {combined_path} ({len(all_rows)} total)")


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]):
    # Put the predefined columns first, then any extras alphabetically
    ordered = [f for f in CSV_FIELDS if f in fieldnames]
    extras  = sorted(f for f in fieldnames if f not in ordered)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ordered + extras, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        raise SystemExit(
            "ERROR: STRAVA_CLIENT_ID and STRAVA_CLIENT_SECRET must be set.\n"
            "Copy .env.example to .env and fill in your credentials."
        )

    token = get_access_token()

    # Fetch summary list of all activities
    activities = fetch_all_activities(token)

    # Optionally fetch detailed data (uncomment to enable — uses more API calls)
    # print("[fetch] Fetching detailed data for each activity...")
    # detailed = []
    # for i, act in enumerate(activities, 1):
    #     print(f"[fetch]   {i}/{len(activities)} — {act['name']}")
    #     detailed.append(fetch_activity_detail(act["id"], token))
    #     time.sleep(0.5)  # be polite to the API
    # activities = detailed

    # Group by sport_type (falls back to 'type' for older activities)
    activities_by_type: dict[str, list] = {}
    for act in activities:
        sport = act.get("sport_type") or act.get("type") or "Unknown"
        activities_by_type.setdefault(sport, []).append(act)

    # Sort each group by start date (newest first)
    for sport in activities_by_type:
        activities_by_type[sport].sort(
            key=lambda a: a.get("start_date", ""), reverse=True
        )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = OUTPUT_DIR / timestamp

    json_dir = run_dir / "json"
    csv_dir  = run_dir / "csv"

    print(f"\n[export] Writing output to {run_dir}/")
    export_json(activities_by_type, json_dir)
    export_csv(activities_by_type, csv_dir)

    print(f"\n[done] Export complete.")
    print(f"  Activity types found: {', '.join(sorted(activities_by_type.keys()))}")
    print(f"  Output directory:     {run_dir.resolve()}")


if __name__ == "__main__":
    main()
