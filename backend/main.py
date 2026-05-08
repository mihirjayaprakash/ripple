import random
import string
from collections import defaultdict
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import aiosqlite
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from pydantic import BaseModel

from backend.database import init_db, get_conn
import backend.spotify as sp


# ── WebSocket manager ─────────────────────────────────────────────────────────

class Manager:
    def __init__(self):
        self._rooms: dict[str, list[WebSocket]] = defaultdict(list)

    async def connect(self, ws: WebSocket, code: str):
        await ws.accept()
        self._rooms[code].append(ws)

    def disconnect(self, ws: WebSocket, code: str):
        self._rooms[code] = [w for w in self._rooms[code] if w is not ws]

    async def broadcast(self, code: str, msg: dict):
        dead = []
        for ws in list(self._rooms[code]):
            try:
                await ws.send_json(msg)
            except Exception:
                dead.append(ws)
        self._rooms[code] = [w for w in self._rooms[code] if w not in dead]


manager = Manager()


# ── App ───────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(title="Ripple", lifespan=lifespan)


# ── DB helpers ────────────────────────────────────────────────────────────────

async def fetch_room(conn: aiosqlite.Connection, code: str) -> aiosqlite.Row:
    async with conn.execute("SELECT * FROM rooms WHERE code = ?", (code,)) as cur:
        row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "Room not found")
    return row


async def fetch_player(conn: aiosqlite.Connection, player_id: int, room_id: int) -> aiosqlite.Row:
    async with conn.execute(
        "SELECT * FROM players WHERE id = ? AND room_id = ?", (player_id, room_id)
    ) as cur:
        row = await cur.fetchone()
    if not row:
        raise HTTPException(404, "Player not in this room")
    return row


async def latest_round(conn: aiosqlite.Connection, room_id: int) -> Optional[aiosqlite.Row]:
    async with conn.execute(
        "SELECT * FROM rounds WHERE room_id = ? ORDER BY round_number DESC LIMIT 1",
        (room_id,),
    ) as cur:
        return await cur.fetchone()


async def build_round_payload(
    conn: aiosqlite.Connection,
    rnd: aiosqlite.Row,
    phase: str,
    viewer_id: Optional[int],
) -> dict:
    async with conn.execute(
        """SELECT s.id, s.player_id, p.name AS player_name,
                  s.track_name, s.artist, s.album, s.album_art, s.spotify_uri, s.preview_url
           FROM submissions s JOIN players p ON s.player_id = p.id
           WHERE s.round_id = ? ORDER BY s.id""",
        (rnd["id"],),
    ) as cur:
        subs = [dict(r) for r in await cur.fetchall()]

    if phase == "vote":
        # Deterministic shuffle by round id so every client sees the same order
        import random as _r
        rng = _r.Random(rnd["id"])
        rng.shuffle(subs)
        for s in subs:
            s["is_own"] = s["player_id"] == viewer_id
            del s["player_name"]

    return {
        "id": rnd["id"],
        "round_number": rnd["round_number"],
        "theme": rnd["theme"],
        "submissions": subs,
        "submission_count": len(subs),
    }


async def build_room_state(
    conn: aiosqlite.Connection,
    room: aiosqlite.Row,
    viewer_id: Optional[int] = None,
) -> dict:
    async with conn.execute(
        "SELECT id, name, is_host FROM players WHERE room_id = ? ORDER BY id",
        (room["id"],),
    ) as cur:
        players = [dict(r) for r in await cur.fetchall()]

    rnd_payload = None
    if room["phase"] != "waiting":
        rnd = await latest_round(conn, room["id"])
        if rnd:
            rnd_payload = await build_round_payload(conn, rnd, room["phase"], viewer_id)

    return {
        "id": room["id"],
        "code": room["code"],
        "name": room["name"],
        "phase": room["phase"],
        "players": players,
        "round": rnd_payload,
    }


def gen_code() -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=6))


# ── Pydantic request bodies ───────────────────────────────────────────────────

class CreateRoomBody(BaseModel):
    player_name: str
    room_name: str


class JoinBody(BaseModel):
    player_name: str


class AdvanceBody(BaseModel):
    player_id: int
    theme: Optional[str] = None


class SubmitBody(BaseModel):
    round_id: int
    player_id: int
    track_id: str
    track_name: str
    artist: str
    album: Optional[str] = None
    album_art: Optional[str] = None
    spotify_uri: Optional[str] = None
    preview_url: Optional[str] = None


class VoteEntry(BaseModel):
    submission_id: int
    points: int


class VotesBody(BaseModel):
    round_id: int
    player_id: int
    votes: list[VoteEntry]


class PlaylistBody(BaseModel):
    player_id: int


# ── Room endpoints ────────────────────────────────────────────────────────────

@app.post("/api/rooms", status_code=201)
async def create_room(body: CreateRoomBody):
    async with get_conn() as conn:
        code = gen_code()
        for _ in range(10):
            async with conn.execute("SELECT id FROM rooms WHERE code = ?", (code,)) as cur:
                if not await cur.fetchone():
                    break
            code = gen_code()

        async with conn.execute(
            "INSERT INTO rooms (code, name) VALUES (?, ?)", (code, body.room_name)
        ) as cur:
            room_id = cur.lastrowid

        async with conn.execute(
            "INSERT INTO players (room_id, name, is_host) VALUES (?, ?, 1)",
            (room_id, body.player_name),
        ) as cur:
            player_id = cur.lastrowid

        await conn.commit()

    return {"room_code": code, "room_id": room_id, "player_id": player_id}


@app.get("/api/rooms/{code}")
async def get_room(code: str, player_id: Optional[int] = Query(None)):
    async with get_conn() as conn:
        room = await fetch_room(conn, code)
        return await build_room_state(conn, room, player_id)


@app.post("/api/rooms/{code}/join", status_code=201)
async def join_room(code: str, body: JoinBody):
    async with get_conn() as conn:
        room = await fetch_room(conn, code)
        if room["phase"] != "waiting":
            raise HTTPException(400, "Game is already in progress")

        async with conn.execute(
            "INSERT INTO players (room_id, name) VALUES (?, ?)",
            (room["id"], body.player_name),
        ) as cur:
            player_id = cur.lastrowid

        await conn.commit()

        async with conn.execute(
            "SELECT id, name, is_host FROM players WHERE room_id = ? ORDER BY id",
            (room["id"],),
        ) as cur:
            players = [dict(r) for r in await cur.fetchall()]

    await manager.broadcast(code, {
        "type": "player_joined",
        "player": {"id": player_id, "name": body.player_name, "is_host": 0},
    })
    return {"player_id": player_id, "players": players}


@app.post("/api/rooms/{code}/advance")
async def advance_phase(code: str, body: AdvanceBody):
    async with get_conn() as conn:
        room = await fetch_room(conn, code)
        player = await fetch_player(conn, body.player_id, room["id"])

        if not player["is_host"]:
            raise HTTPException(403, "Only the host can advance phases")

        phase = room["phase"]

        if phase == "waiting":
            if not body.theme:
                raise HTTPException(400, "Theme is required to start the game")
            await conn.execute(
                "INSERT INTO rounds (room_id, round_number, theme) VALUES (?, 1, ?)",
                (room["id"], body.theme),
            )
            await conn.execute("UPDATE rooms SET phase='submit' WHERE id=?", (room["id"],))

        elif phase == "submit":
            await conn.execute("UPDATE rooms SET phase='vote' WHERE id=?", (room["id"],))

        elif phase == "vote":
            await conn.execute("UPDATE rooms SET phase='results' WHERE id=?", (room["id"],))

        elif phase == "results":
            if not body.theme:
                raise HTTPException(400, "Theme is required for the next round")
            rnd = await latest_round(conn, room["id"])
            next_num = (rnd["round_number"] + 1) if rnd else 1
            await conn.execute(
                "INSERT INTO rounds (room_id, round_number, theme) VALUES (?, ?, ?)",
                (room["id"], next_num, body.theme),
            )
            await conn.execute("UPDATE rooms SET phase='submit' WHERE id=?", (room["id"],))

        else:
            raise HTTPException(400, f"Cannot advance from phase: {phase}")

        await conn.commit()
        room = await fetch_room(conn, code)
        state = await build_room_state(conn, room)

    await manager.broadcast(code, {"type": "phase_change", "state": state})
    return state


# ── Submission endpoint ───────────────────────────────────────────────────────

@app.post("/api/submissions", status_code=201)
async def submit_track(body: SubmitBody):
    async with get_conn() as conn:
        async with conn.execute("SELECT * FROM rounds WHERE id=?", (body.round_id,)) as cur:
            rnd = await cur.fetchone()
        if not rnd:
            raise HTTPException(404, "Round not found")

        await fetch_player(conn, body.player_id, rnd["room_id"])

        async with conn.execute(
            "SELECT phase, code FROM rooms WHERE id=?", (rnd["room_id"],)
        ) as cur:
            room_row = await cur.fetchone()
        if room_row["phase"] != "submit":
            raise HTTPException(400, "Room is not in the submission phase")

        await conn.execute(
            """INSERT INTO submissions
               (round_id, player_id, track_id, track_name, artist, album, album_art, spotify_uri, preview_url)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(round_id, player_id) DO UPDATE SET
                 track_id=excluded.track_id, track_name=excluded.track_name,
                 artist=excluded.artist, album=excluded.album,
                 album_art=excluded.album_art, spotify_uri=excluded.spotify_uri,
                 preview_url=excluded.preview_url""",
            (body.round_id, body.player_id, body.track_id, body.track_name,
             body.artist, body.album, body.album_art, body.spotify_uri, body.preview_url),
        )
        await conn.commit()

        async with conn.execute(
            "SELECT COUNT(*) AS n FROM submissions WHERE round_id=?", (body.round_id,)
        ) as cur:
            submitted = (await cur.fetchone())["n"]

        async with conn.execute(
            "SELECT COUNT(*) AS n FROM players WHERE room_id=?", (rnd["room_id"],)
        ) as cur:
            total = (await cur.fetchone())["n"]

        room_code = room_row["code"]

    await manager.broadcast(room_code, {
        "type": "submission_update",
        "submitted": submitted,
        "total": total,
    })
    return {"submitted": submitted, "total": total}


# ── Vote endpoints ────────────────────────────────────────────────────────────

@app.post("/api/votes", status_code=201)
async def submit_votes(body: VotesBody):
    async with get_conn() as conn:
        async with conn.execute("SELECT * FROM rounds WHERE id=?", (body.round_id,)) as cur:
            rnd = await cur.fetchone()
        if not rnd:
            raise HTTPException(404, "Round not found")

        await fetch_player(conn, body.player_id, rnd["room_id"])

        async with conn.execute(
            "SELECT phase, code FROM rooms WHERE id=?", (rnd["room_id"],)
        ) as cur:
            room_row = await cur.fetchone()
        if room_row["phase"] != "vote":
            raise HTTPException(400, "Room is not in the voting phase")

        async with conn.execute(
            "SELECT id FROM submissions WHERE round_id=? AND player_id=?",
            (body.round_id, body.player_id),
        ) as cur:
            own_sub = await cur.fetchone()
        own_sub_id = own_sub["id"] if own_sub else None

        for vote in body.votes:
            if not (1 <= vote.points <= 5):
                raise HTTPException(400, "Points must be between 1 and 5")
            if vote.submission_id == own_sub_id:
                raise HTTPException(400, "Cannot vote for your own submission")
            await conn.execute(
                """INSERT INTO votes (submission_id, voter_id, points) VALUES (?, ?, ?)
                   ON CONFLICT(submission_id, voter_id) DO UPDATE SET points=excluded.points""",
                (vote.submission_id, body.player_id, vote.points),
            )

        await conn.commit()

        async with conn.execute(
            """SELECT COUNT(DISTINCT voter_id) AS n FROM votes v
               JOIN submissions s ON v.submission_id = s.id
               WHERE s.round_id=?""",
            (body.round_id,),
        ) as cur:
            voted = (await cur.fetchone())["n"]

        async with conn.execute(
            "SELECT COUNT(*) AS n FROM players WHERE room_id=?", (rnd["room_id"],)
        ) as cur:
            total = (await cur.fetchone())["n"]

        room_code = room_row["code"]

    await manager.broadcast(room_code, {"type": "vote_update", "voted": voted, "total": total})
    return {"voted": voted, "total": total}


@app.get("/api/votes/{round_id}")
async def get_my_votes(round_id: int, player_id: int = Query(...)):
    async with get_conn() as conn:
        async with conn.execute(
            """SELECT v.submission_id, v.points FROM votes v
               JOIN submissions s ON v.submission_id = s.id
               WHERE s.round_id=? AND v.voter_id=?""",
            (round_id, player_id),
        ) as cur:
            votes = [dict(r) for r in await cur.fetchall()]
    return {"votes": votes}


# ── Results endpoint ──────────────────────────────────────────────────────────

@app.get("/api/rounds/{round_id}/results")
async def get_results(round_id: int):
    async with get_conn() as conn:
        async with conn.execute(
            """SELECT s.id, s.player_id, p.name AS player_name,
                      s.track_name, s.artist, s.album, s.album_art,
                      s.spotify_uri, s.preview_url,
                      COALESCE(SUM(v.points), 0) AS total_points
               FROM submissions s
               JOIN players p ON s.player_id = p.id
               LEFT JOIN votes v ON v.submission_id = s.id
               WHERE s.round_id=?
               GROUP BY s.id
               ORDER BY total_points DESC""",
            (round_id,),
        ) as cur:
            rows = [dict(r) for r in await cur.fetchall()]

        for row in rows:
            async with conn.execute(
                """SELECT p.name AS voter_name, v.points FROM votes v
                   JOIN players p ON v.voter_id = p.id
                   WHERE v.submission_id=? ORDER BY v.points DESC""",
                (row["id"],),
            ) as cur:
                row["votes"] = [dict(r) for r in await cur.fetchall()]

    return {"results": rows}


# ── Spotify endpoints ─────────────────────────────────────────────────────────

@app.get("/spotify/search")
async def spotify_search(q: str = Query(..., min_length=1)):
    try:
        tracks = await sp.search_tracks(q)
    except RuntimeError as e:
        raise HTTPException(503, str(e))
    except Exception as e:
        raise HTTPException(502, f"Spotify search failed: {e}")
    return {"tracks": tracks}


@app.get("/spotify/login")
async def spotify_login(player_id: int = Query(...)):
    if not sp.CLIENT_ID:
        raise HTTPException(503, "Spotify not configured")
    return RedirectResponse(sp.auth_url(state=str(player_id)))


@app.get("/spotify/callback")
async def spotify_callback(
    code: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    error: Optional[str] = Query(None),
):
    def close_script(msg: str, ok: bool, detail: str = "") -> HTMLResponse:
        status = "spotify_ok" if ok else "spotify_error"
        label = "Connected! You can close this window." if ok else f"Auth failed: {detail}"
        return HTMLResponse(
            f"<html><body style='font-family:sans-serif;padding:2rem;background:#0b0b10;color:#f0f0f5'>"
            f"<script>window.opener&&window.opener.postMessage({{type:'{status}',{msg}}}, '*');window.close();</script>"
            f"<p>{label}</p></body></html>"
        )

    if error or not code or not state:
        detail = error or "cancelled"
        return close_script(f"error:'{detail}'", False, detail)

    try:
        player_id = int(state)
        tokens = await sp.exchange_code(code)
        profile = await sp.user_profile(tokens["access_token"])
    except Exception as e:
        return close_script(f"error:'Auth failed: {e}'", False, str(e))

    async with get_conn() as conn:
        await conn.execute(
            """UPDATE players SET spotify_access_token=?, spotify_refresh_token=?,
               spotify_user_id=? WHERE id=?""",
            (tokens["access_token"], tokens.get("refresh_token"), profile["id"], player_id),
        )
        await conn.commit()

    display = profile.get("display_name") or profile["id"]
    return close_script(f"user:'{display}'", True)


@app.post("/api/rooms/{code}/playlist")
async def create_playlist(code: str, body: PlaylistBody):
    async with get_conn() as conn:
        room = await fetch_room(conn, code)
        player = await fetch_player(conn, body.player_id, room["id"])

        if not player["is_host"]:
            raise HTTPException(403, "Only the host can create playlists")
        if not player["spotify_access_token"]:
            raise HTTPException(400, "Connect Spotify first")

        rnd = await latest_round(conn, room["id"])
        if not rnd:
            raise HTTPException(400, "No active round")

        async with conn.execute(
            "SELECT spotify_uri FROM submissions WHERE round_id=? AND spotify_uri IS NOT NULL",
            (rnd["id"],),
        ) as cur:
            uris = [r["spotify_uri"] for r in await cur.fetchall()]

        access_token = player["spotify_access_token"]
        refresh = player["spotify_refresh_token"]
        user_id = player["spotify_user_id"]
        pl_name = f"{room['name']} — Round {rnd['round_number']}: {rnd['theme']}"

    async def _make_playlist(token: str) -> str:
        return await sp.create_playlist(token, user_id, pl_name, uris)

    try:
        url = await _make_playlist(access_token)
    except Exception:
        try:
            new_tokens = await sp.refresh_token(refresh)
            async with get_conn() as conn:
                await conn.execute(
                    "UPDATE players SET spotify_access_token=? WHERE id=?",
                    (new_tokens["access_token"], body.player_id),
                )
                await conn.commit()
            url = await _make_playlist(new_tokens["access_token"])
        except Exception as e:
            raise HTTPException(502, f"Playlist creation failed: {e}")

    return {"playlist_url": url}


# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws/{code}")
async def ws_endpoint(ws: WebSocket, code: str):
    await manager.connect(ws, code)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws, code)


# ── Frontend ──────────────────────────────────────────────────────────────────

@app.get("/")
async def index():
    return FileResponse(Path("frontend") / "index.html")
