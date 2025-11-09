# routes/google_oauth.py
import os, urllib.parse, secrets
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse, JSONResponse, HTMLResponse

router = APIRouter()

def _env(k: str) -> str:
    return os.getenv(k, "").strip()

@router.get("/api/google/oauth/start")
def google_oauth_start():
    cid = _env("GOOGLE_CLIENT_ID")
    red = _env("GOOGLE_REDIRECT_URI")
    scopes = _env("GOOGLE_SCOPES")
    missing = [k for k, v in [
        ("GOOGLE_CLIENT_ID", cid),
        ("GOOGLE_REDIRECT_URI", red),
        ("GOOGLE_SCOPES", scopes),
    ] if not v]
    if missing:
        return JSONResponse({"error": "Missing env", "missing": missing}, status_code=500)

    # minimal state (PKCE omitted for now; goal is to reach consent page)
    state = secrets.token_urlsafe(24)

    params = {
        "response_type": "code",
        "client_id": cid,
        "redirect_uri": red,  # MUST match a Google Authorized redirect URI
        "scope": scopes,      # space-separated
        "access_type": "offline",
        "include_granted_scopes": "true",
        "prompt": "consent",
        "state": state,
        "code_challenge": "TEST",
        "code_challenge_method": "plain",
    }
    auth_url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)
    return RedirectResponse(url=auth_url, status_code=307)

# TEMP callback so you can see that Google can return here successfully.
# Use the redirect you configured in Google: /auth/google/callback  (based on your screenshot)
@router.get("/auth/google/callback")
def google_oauth_callback(request: Request):
    # show what Google sent back so we know it worked
    params = dict(request.query_params)
    html = "<h2>Google callback hit ✅</h2><pre>" + \
           "".join(f"{k}: {v}\n" for k, v in params.items()) + "</pre>"
    return HTMLResponse(content=html, status_code=200)