import json
import os
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

CLIENT_ID     = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
TOKEN_FILE    = Path(os.getenv("STRAVA_TOKEN_FILE", "strava_tokens.json"))

BASE_URL  = "https://www.strava.com/api/v3"
TOKEN_URL = "https://www.strava.com/oauth/token"

# ── Auth ──────────────────────────────────────────────────────────────────────

def get_access_token() -> str:
    if not TOKEN_FILE.exists():
        raise SystemExit("ERROR: Geen token gevonden. Voer eerst auth.py uit.")
    tokens = json.loads(TOKEN_FILE.read_text())
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

# ── API ───────────────────────────────────────────────────────────────────────

def _api_get(endpoint: str, token: str, params: dict = {}) -> list | dict:
    resp = requests.get(
        f"{BASE_URL}{endpoint}",
        headers={"Authorization": f"Bearer {token}"},
        params=params,
    )
    if resp.status_code == 429:
        wait = max(int(resp.headers.get("X-RateLimit-Reset", 0)) - int(time.time()), 60)
        print(f"[api] Rate limit — wachten {wait}s...")
        time.sleep(wait)
        return _api_get(endpoint, token, params)
    resp.raise_for_status()
    return resp.json()


def fetch_all_runs(token: str) -> list[dict]:
    runs, page = [], 1
    print("[strava] Activiteiten ophalen...")
    while True:
        batch = _api_get("/athlete/activities", token, {"per_page": 200, "page": page})
        if not batch:
            break
        run_batch = [
            a for a in batch
            if a.get("sport_type") in ("Run", "TrailRun", "VirtualRun")
            or a.get("type") == "Run"
        ]
        runs.extend(run_batch)
        print(f"[strava]   pagina {page}: {len(batch)} activiteiten, {len(run_batch)} runs (totaal: {len(runs)})")
        page += 1
    print(f"[strava] Klaar — {len(runs)} runs opgehaald.")
    return runs
