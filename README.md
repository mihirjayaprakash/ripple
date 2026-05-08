# Ripple

A Music League clone — submit songs, vote anonymously, see the results.

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Spotify

1. Go to [Spotify Developer Dashboard](https://developer.spotify.com/dashboard) and create an app.
2. Set the Redirect URI to `http://localhost:8000/spotify/callback`.
3. Copy your Client ID and Client Secret.

```bash
cp .env.example .env
# edit .env and fill in your credentials
```

> **Scopes required:** `user-read-private`, `playlist-modify-public`, `playlist-modify-private`

### 3. Run the server

```bash
uvicorn backend.main:app --reload
```

Open [http://localhost:8000](http://localhost:8000).

---

## How to play

| Step | Who | What |
|------|-----|-------|
| Create room | Host | Enter a name and room name — get a 6-char code |
| Join | Players | Enter name + room code |
| Start | Host | Enter a round theme, click **Start** |
| Submit | Everyone | Search Spotify, pick a song, submit |
| Vote | Everyone | Rate every track (except your own) 1–5 ★ |
| Results | Everyone | See the leaderboard with vote breakdown |
| Playlist | Host | Connect Spotify → **Create Playlist** to export the round |
| Next round | Host | Enter a new theme to play again |

---

## Architecture

```
backend/main.py      FastAPI app — REST API + WebSocket broadcast
backend/database.py  SQLite schema (rooms, players, rounds, submissions, votes)
backend/spotify.py   Spotify client — client credentials search + OAuth playlist creation
frontend/index.html  Single-file vanilla JS/CSS SPA served by FastAPI
ripple.db            SQLite database (created on first run)
```

### Phase flow

```
waiting → submit → vote → results → submit (next round) → …
```

- Phase transitions are triggered by the host via `POST /api/rooms/{code}/advance`.
- All connected clients receive a WebSocket broadcast and re-render immediately.
- Search uses Spotify **Client Credentials** (no user login needed).
- Playlist creation uses the host's **OAuth token** (Authorization Code flow).

### Key API endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/rooms` | Create room |
| `POST` | `/api/rooms/{code}/join` | Join room |
| `POST` | `/api/rooms/{code}/advance` | Advance phase (host) |
| `POST` | `/api/submissions` | Submit a track |
| `POST` | `/api/votes` | Submit votes (batch) |
| `GET`  | `/api/rounds/{id}/results` | Full results with vote breakdown |
| `GET`  | `/spotify/search?q=` | Track search (server-side proxy) |
| `GET`  | `/spotify/login?player_id=` | Start OAuth flow |
| `WS`   | `/ws/{code}` | Real-time room updates |

### WebSocket messages (server → client)

| Type | Payload | Trigger |
|------|---------|---------|
| `phase_change` | `{state: <full room state>}` | Host advances phase |
| `player_joined` | `{player: {id, name}}` | New player joins |
| `submission_update` | `{submitted, total}` | Any player submits |
| `vote_update` | `{voted, total}` | Any player submits votes |

### Voting rules

- Each player rates every other player's submission **1–5 ★**.
- You cannot vote on your own submission.
- Votes can be updated until the batch is submitted.
- Highest total points wins the round.
