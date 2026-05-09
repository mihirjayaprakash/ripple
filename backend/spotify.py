import asyncio
import base64
import json as _json
import logging
import os
import time
from urllib.parse import urlencode

import httpx
from dotenv import load_dotenv

_log = logging.getLogger("ripple.spotify")

load_dotenv()

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "")
REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI", "http://localhost:8000/spotify/callback")
_APP_REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN", "")
_APP_USER_ID = os.getenv("SPOTIFY_USER_ID", "")

_AUTH_URL = "https://accounts.spotify.com/authorize"
_TOKEN_URL = "https://accounts.spotify.com/api/token"
_API_BASE = "https://api.spotify.com/v1"

_cached: dict = {}
_app_token: dict = {}


def _basic_header() -> str:
    creds = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    return f"Basic {creds}"


async def client_token() -> str:
    """App-level token for search (client credentials, no user needed)."""
    if _cached.get("expires_at", 0) > time.time():
        return _cached["access_token"]
    async with httpx.AsyncClient() as c:
        r = await c.post(
            _TOKEN_URL,
            headers={"Authorization": _basic_header()},
            data={"grant_type": "client_credentials"},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
    _cached["access_token"] = data["access_token"]
    _cached["expires_at"] = time.time() + data["expires_in"] - 30
    return data["access_token"]


async def app_user_token() -> tuple[str, str]:
    """Get access token + user_id for the pre-authorized app account."""
    if not _APP_REFRESH_TOKEN:
        raise RuntimeError("SPOTIFY_REFRESH_TOKEN not configured")
    if _app_token.get("expires_at", 0) > time.time():
        return _app_token["access_token"], _APP_USER_ID
    async with httpx.AsyncClient() as c:
        r = await c.post(
            _TOKEN_URL,
            headers={"Authorization": _basic_header()},
            data={"grant_type": "refresh_token", "refresh_token": _APP_REFRESH_TOKEN},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
    _log.warning("Token refreshed — scopes: %s", data.get("scope", "NOT IN RESPONSE"))
    _app_token["access_token"] = data["access_token"]
    _app_token["expires_at"] = time.time() + data["expires_in"] - 30
    return data["access_token"], _APP_USER_ID


def auth_url(state: str, force: bool = False) -> str:
    params = {
        "client_id": CLIENT_ID,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "state": state,
        "scope": "user-read-private playlist-modify-public playlist-modify-private",
    }
    if force:
        params["show_dialog"] = "true"
    return f"{_AUTH_URL}?{urlencode(params)}"


async def exchange_code(code: str) -> dict:
    async with httpx.AsyncClient() as c:
        r = await c.post(
            _TOKEN_URL,
            headers={"Authorization": _basic_header()},
            data={"grant_type": "authorization_code", "code": code, "redirect_uri": REDIRECT_URI},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()


async def search_tracks(q: str, limit: int = 8) -> list[dict]:
    if not CLIENT_ID:
        raise RuntimeError("SPOTIFY_CLIENT_ID not configured")
    token = await client_token()
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{_API_BASE}/search",
            headers={"Authorization": f"Bearer {token}"},
            params={"q": q, "type": "track", "limit": limit},
            timeout=10,
        )
        r.raise_for_status()
    return [
        {
            "id": t["id"],
            "name": t["name"],
            "artist": ", ".join(a["name"] for a in t["artists"]),
            "album": t["album"]["name"],
            "album_art": t["album"]["images"][0]["url"] if t["album"]["images"] else None,
            "uri": t["uri"],
            "preview_url": t.get("preview_url"),
            "external_url": t["external_urls"]["spotify"],
        }
        for t in r.json()["tracks"]["items"]
    ]


async def user_profile(access_token: str) -> dict:
    async with httpx.AsyncClient() as c:
        r = await c.get(
            f"{_API_BASE}/me",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()


async def create_playlist(name: str, uris: list[str]) -> str:
    """Create a public playlist under the pre-authorized app account."""
    token, user_id = await app_user_token()
    if not user_id:
        # fetch it once if not set
        profile = await user_profile(token)
        user_id = profile["id"]
    async with httpx.AsyncClient() as c:
        r = await c.post(
            f"{_API_BASE}/me/playlists",
            headers={"Authorization": f"Bearer {token}"},
            json={"name": name, "description": "Created with Ripple 🎵", "public": True},
            timeout=10,
        )
        r.raise_for_status()
        pl = r.json()
    _log.warning("Playlist created: id=%s owner=%s url=%s",
                 pl["id"], pl.get("owner", {}).get("id"), pl["external_urls"]["spotify"])
    if uris:
        await asyncio.sleep(1)
        _log.warning("Adding %d track(s) to playlist %s: %s", len(uris), pl["id"], uris)
        async with httpx.AsyncClient() as c2:
            # Try query-param style (alternative to JSON body) to work around 403
            r2 = await c2.post(
                f"{_API_BASE}/playlists/{pl['id']}/tracks",
                headers={"Authorization": f"Bearer {token}"},
                params={"uris": ",".join(uris)},
                timeout=10,
            )
            if not r2.is_success:
                _log.warning("Failed to add tracks (query params): %s %s", r2.status_code, r2.text)
            else:
                _log.warning("Tracks added successfully")
    return pl["external_urls"]["spotify"]
