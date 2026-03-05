"""
Strava OAuth2 autorisatie — eenmalig uitvoeren
-----------------------------------------------
Opent de browser voor Strava-autorisatie en slaat het token op in
strava_tokens.json. Daarna vernieuwt main.py het token automatisch.

Gebruik:
    python auth.py
"""

import json
import os
import time
import webbrowser
from pathlib import Path
from threading import Thread
from urllib.parse import urlencode, urlparse, parse_qs
from http.server import HTTPServer, BaseHTTPRequestHandler

import requests
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID     = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
REDIRECT_URI  = os.getenv("STRAVA_REDIRECT_URI", "http://localhost:8080/callback")
TOKEN_FILE    = Path(os.getenv("STRAVA_TOKEN_FILE", "strava_tokens.json"))

AUTH_URL  = "https://www.strava.com/oauth/authorize"
TOKEN_URL = "https://www.strava.com/oauth/token"

_auth_code: str | None = None


class _CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global _auth_code
        params = parse_qs(urlparse(self.path).query)
        if "code" in params:
            _auth_code = params["code"][0]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"<h2>Geautoriseerd! Je kunt dit tabblad sluiten.</h2>")
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"<h2>Autorisatie mislukt.</h2>")

    def log_message(self, *args):
        pass


def authorize():
    port = int(urlparse(REDIRECT_URI).port or 8080)
    params = {
        "client_id":       CLIENT_ID,
        "redirect_uri":    REDIRECT_URI,
        "response_type":   "code",
        "approval_prompt": "auto",
        "scope":           "read,activity:read_all",
    }
    url = f"{AUTH_URL}?{urlencode(params)}"
    print("[auth] Browser openen voor Strava autorisatie...")
    print(f"[auth] Werkt de browser niet? Ga naar:\n  {url}\n")
    Thread(
        target=lambda: HTTPServer(("localhost", port), _CallbackHandler).handle_request(),
        daemon=True,
    ).start()
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
    TOKEN_FILE.write_text(json.dumps(resp.json(), indent=2))
    print(f"[auth] Token opgeslagen in {TOKEN_FILE}.")


if __name__ == "__main__":
    if not CLIENT_ID or not CLIENT_SECRET:
        raise SystemExit("ERROR: Vul STRAVA_CLIENT_ID en STRAVA_CLIENT_SECRET in in .env")
    authorize()
