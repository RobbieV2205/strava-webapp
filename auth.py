"""
Strava OAuth2 authentication
-----------------------------------------------

opens the browser to authenticate and get the right tokens needed for the application.
Tokens will be stored in a file called: strava_tokens.json.

if the tokens arre collected succesfully this script is no longer needed.
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
            self.wfile.write(b"<h2>authorised! you can close this tab.</h2>")
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"<h2>Autorisatie failed.</h2>")

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
    print("[auth] Browser opens for Strava authentication...")
    print(f"[auth] Does the browser not work? Go to:\n  {url}\n")
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
        raise RuntimeError("Authenctication timeout — no code recieved.")
    resp = requests.post(TOKEN_URL, data={
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code":          _auth_code,
        "grant_type":    "authorization_code",
    })
    resp.raise_for_status()
    TOKEN_FILE.write_text(json.dumps(resp.json(), indent=2))
    print(f"[auth] Token stored in: {TOKEN_FILE}.")


if __name__ == "__main__":
    if not CLIENT_ID or not CLIENT_SECRET:
        raise SystemExit("ERROR: STRAVA_CLIENT_ID and STRAVA_CLIENT_SECRET missing in .env")
    authorize()
