# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run the dev server (from project root)
uvicorn backend.main:app --reload

# App is served at http://localhost:8000
```

The SQLite database (`ripple.db`) is created automatically on first run in the project root. Delete it to reset all state.

## Architecture

Ripple is a Music League clone. Single FastAPI process serves the REST API, WebSocket hub, and the frontend HTML file.

### Backend (`backend/`)

| File | Role |
|------|------|
| `main.py` | FastAPI app — all REST routes, WebSocket endpoint, Spotify OAuth callback |
| `database.py` | SQLite schema init + `get_conn()` async context manager |
| `spotify.py` | Spotify client — client credentials token cache, user OAuth, search, playlist creation |

**Data model:** `rooms → players`, `rooms → rounds → submissions → votes`. All foreign keys cascade on delete. WAL mode is enabled for concurrent reads.

**Phase state machine** lives on `rooms.phase`:
```
waiting → submit → vote → results → submit (next round) → …
```
Only the host can advance phases via `POST /api/rooms/{code}/advance`. All connected WebSocket clients receive a `phase_change` broadcast with the full new room state and re-render.

**Spotify integration has two modes:**
- Track search: server-side proxy using client credentials (no user login). Requires `SPOTIFY_CLIENT_ID` + `SPOTIFY_CLIENT_SECRET` in `.env`.
- Playlist creation: host-only OAuth (Authorization Code flow). The OAuth popup posts a `message` to the opener window on success/failure, then closes itself.

**WebSocket** (`/ws/{code}`) is broadcast-only from the server. Clients reconnect with exponential backoff. The server ignores all incoming WS messages (used only as keepalive).

### Frontend (`frontend/index.html`)

Single self-contained HTML/CSS/JS file, no build step, no dependencies. Served by FastAPI at `GET /`. All state lives in the `S` object in JS. Views (`home`, `lobby`, `submit`, `vote`, `results`) are toggled with `hidden` class. Player identity is persisted in `localStorage` and auto-restored on page load.

**Vote phase submissions** are shuffled deterministically using `random.Random(round_id)` so all clients see the same order without revealing authorship. Player names are stripped from submission objects in vote phase and only returned in the results endpoint.

### Environment variables

```
SPOTIFY_CLIENT_ID=
SPOTIFY_CLIENT_SECRET=
SPOTIFY_REDIRECT_URI=http://localhost:8000/spotify/callback
```

Add these to `.env` (copied from `.env.example`). The app runs without them but search and playlist features will return 503/502.
