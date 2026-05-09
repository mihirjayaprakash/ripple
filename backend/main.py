import asyncio
import json as _json
import logging
import random
import string
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger("ripple")

THEMES = [
    "Songs that have a weird beat or irregular time signature",
    "Songs with summertime vibes and sunny energy",
    "Songs that have a person's name in the title",
    "Songs that feature a prominent whistle section",
    "Songs you would play during a high-stakes heist",
    "Songs that are exactly 4 minutes and 20 seconds long",
    "Songs that mention a specific color in the first 10 seconds",
    "Songs that sound better at night",
    "Songs that feature an instrument you can't name",
    "Songs that make you want to drive slightly over the speed limit",
    "Songs with a key change that gives you goosebumps",
    "Songs that mention a day of the week",
    "Songs that start with a long spoken word intro",
    "Songs that feel like they belong in a 90s teen movie",
    "Songs that use a non-musical object as an instrument",
    "Songs about a city that isn't New York, London, or LA",
    "Songs that change genres halfway through",
    "Songs that would be played at a villain's garden party",
    "Songs that have a title longer than five words",
    "Songs that feature a 'hidden' track or a long silence",
    "Songs that remind you of a specific ex (without naming them)",
    "Songs that make you feel like you're floating in space",
    "Songs with a bassline that carries the entire track",
    "Songs that mention a type of food or drink",
    "Songs that were better as a cover version",
    "Songs that sound like a 'Level 1' video game theme",
    "Songs that feature a choir or a group of children singing",
    "Songs about outer space or aliens",
    "Songs that would play over the end credits of your biopic",
    "Songs that are technically 'Christmas songs' but don't sound like it",
    "Songs that mention a brand name",
    "Songs that have an animal in the title",
    "Songs that make you want to start a revolution",
    "Songs that feature a phone ringing or a busy signal",
    "Songs that sound like they were recorded in a bathroom",
    "Songs with a one-word title",
    "Songs that mention the weather (not just 'sun')",
    "Songs that were featured in a famous movie montage",
    "Songs that sound like the 1970s",
    "Songs that mention a specific year",
    "Songs that feature a heavy use of cowbell",
    "Songs that you would play at a 5-year-old's birthday party",
    "Songs that make you feel like a detective in a noir film",
    "Songs with a title that is a question",
    "Songs that mention a family member (Mom, Brother, etc.)",
    "Songs that feature a prominent saxophone solo",
    "Songs about being broke or having no money",
    "Songs that sound like a rainy Tuesday afternoon",
    "Songs that mention a planet",
    "Songs that use 'La La La' as a major part of the chorus",
    "Songs that were huge hits but the artist is a 'one-hit wonder'",
    "Songs that mention a clothing item",
    "Songs that sound like a 1980s workout video",
    "Songs that mention a mode of transportation (Bus, Train, etc.)",
    "Songs with a lyric about the moon",
    "Songs that feature a clap-along section",
    "Songs that make you want to dance, but only in your kitchen",
    "Songs that mention a fruit",
    "Songs with a repetitive lyric that gets stuck in your head",
    "Songs that sound like they belong in a Western movie",
    "Songs that mention a specific US state",
    "Songs that have a 'count-in' (1, 2, 3, 4!) at the start",
    "Songs about a fictional character",
    "Songs that feature a siren or emergency vehicle sound",
    "Songs that sound like a 'Boss Battle' theme",
    "Songs that mention a body part in the title",
    "Songs that you're embarrassed to admit you like",
    "Songs that mention a specific time of day",
    "Songs that feature a heavy amount of autotune used stylistically",
    "Songs that sound like a carnival or circus",
    "Songs that mention a flower",
    "Songs with a tempo of over 150 BPM",
    "Songs that mention a specific number",
    "Songs that feature a recording of a nature sound (Rain, Birds, etc.)",
    "Songs that sound like a spy movie theme",
    "Songs that mention a specific alcoholic drink",
    "Songs that were released the year you were born",
    "Songs that feature a guest rapper who steals the show",
    "Songs that sound like they were recorded in the 1920s",
    "Songs that mention a specific hobby",
    "Songs that make you feel like you're in a high-speed chase",
    "Songs that have a 'false ending' and then start up again",
    "Songs that mention a shape (Circle, Square, etc.)",
    "Songs that feature a prominent flute part",
    "Songs that sound like a dream or a hallucination",
    "Songs that mention a specific historical event",
    "Songs that mention a cardinal direction (North, South, etc.)",
    "Songs that have a parenthesis in the title",
    "Songs that feature a prominent banjo or mandolin",
    "Songs that sound like a summer camp bonfire",
    "Songs that mention a specific gemstone or metal",
    "Songs that have a lyric about 'dancing in the rain'",
    "Songs that sound like they belong in a futuristic neon city",
    "Songs that mention a specific type of bird",
    "Songs that were originally written for a musical or play",
    "Songs that feature a 'call and response' section",
    "Songs that sound like a crisp autumn morning",
    "Songs that mention a specific sport",
    "Songs that have a title that doesn't appear in the lyrics",
    "Songs that make you want to go for a long walk alone",
    "Songs that feature a heavy use of the wah-wah pedal",
    "Songs that sound like they were written for a commercial",
    "Songs that mention a specific holiday (not Christmas)",
    "Songs that have a 'heavy' riff but aren't metal songs",
    "Songs that sound like a montage of someone falling in love",
]


async def pick_theme(conn, room_id: int) -> str:
    async with conn.execute("SELECT theme FROM rounds WHERE room_id = ?", (room_id,)) as cur:
        used = {r["theme"] for r in await cur.fetchall()}
    available = [t for t in THEMES if t not in used]
    pool = available if available else THEMES
    return random.choice(pool)

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


@dataclass
class VotekickState:
    target_id: int
    target_name: str
    initiator_name: str
    yes_voters: set = field(default_factory=set)
    no_voters: set = field(default_factory=set)
    task: object = None  # asyncio.Task


_votekicks: dict[str, VotekickState] = {}


# ── App ───────────────────────────────────────────────────────────────────────

async def _cleanup_finished_rooms():
    while True:
        await asyncio.sleep(10 * 60)  # run every 10 minutes
        try:
            async with get_conn() as conn:
                await conn.execute(
                    "DELETE FROM rooms WHERE phase='finished' AND finished_at < datetime('now', '-2 hours')"
                )
                await conn.commit()
        except Exception:
            logger.exception("Room cleanup failed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    task = asyncio.create_task(_cleanup_finished_rooms())
    yield
    task.cancel()


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
                  s.track_name, s.artist, s.album, s.album_art, s.spotify_uri, s.preview_url, s.note
           FROM submissions s JOIN players p ON s.player_id = p.id
           WHERE s.round_id = ? ORDER BY s.id""",
        (rnd["id"],),
    ) as cur:
        subs = [dict(r) for r in await cur.fetchall()]

    if phase == "submit":
        async with conn.execute(
            """SELECT p.name FROM players p
               WHERE p.room_id = ? AND p.left = 0
               AND p.id NOT IN (SELECT player_id FROM submissions WHERE round_id = ?)
               ORDER BY p.id""",
            (rnd["room_id"], rnd["id"]),
        ) as cur:
            pending_submitters = [r[0] for r in await cur.fetchall()]
    else:
        pending_submitters = []

    if phase == "vote":
        # Deterministic shuffle by round id so every client sees the same order
        import random as _r
        rng = _r.Random(rnd["id"])
        rng.shuffle(subs)
        for s in subs:
            s["is_own"] = s["player_id"] == viewer_id
            del s["player_name"]

        async with conn.execute(
            """SELECT p.name FROM players p
               WHERE p.room_id = ? AND p.left = 0
               AND p.id NOT IN (
                   SELECT DISTINCT v.voter_id FROM votes v
                   JOIN submissions s ON v.submission_id = s.id
                   WHERE s.round_id = ?
               )
               ORDER BY p.id""",
            (rnd["room_id"], rnd["id"]),
        ) as cur:
            pending_voters = [r[0] for r in await cur.fetchall()]
    else:
        pending_voters = []

    return {
        "id": rnd["id"],
        "round_number": rnd["round_number"],
        "theme": rnd["theme"],
        "playlist_url": rnd["playlist_url"],
        "submissions": subs,
        "submission_count": len(subs),
        "pending_submitters": pending_submitters,
        "pending_voters": pending_voters,
    }


async def build_room_state(
    conn: aiosqlite.Connection,
    room: aiosqlite.Row,
    viewer_id: Optional[int] = None,
) -> dict:
    async with conn.execute(
        "SELECT id, name, is_host FROM players WHERE room_id = ? AND left = 0 ORDER BY id",
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
        "max_rounds": room["max_rounds"],
        "players": players,
        "round": rnd_payload,
    }


def gen_code() -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=6))


# ── Pydantic request bodies ───────────────────────────────────────────────────

class CreateRoomBody(BaseModel):
    player_name: str
    room_name: str
    max_rounds: int = 5


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
    note: Optional[str] = None


class VoteEntry(BaseModel):
    submission_id: int
    points: int


class VotesBody(BaseModel):
    round_id: int
    player_id: int
    votes: list[VoteEntry]


class PlaylistBody(BaseModel):
    player_id: int


class LeaveBody(BaseModel):
    player_id: int


class TransferBody(BaseModel):
    player_id: int
    new_host_id: int
    stay: bool = False


class KickBody(BaseModel):
    player_id: int
    target_id: int


class VotekickBody(BaseModel):
    player_id: int
    target_id: int
    vote: str = "yes"  # "yes" or "no"


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

        max_rounds = body.max_rounds if body.max_rounds in (3, 5, 7) else 5
        async with conn.execute(
            "INSERT INTO rooms (code, name, max_rounds) VALUES (?, ?, ?)",
            (code, body.room_name, max_rounds),
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

        async with conn.execute(
            "SELECT id FROM players WHERE room_id=? AND name=? AND left=0",
            (room["id"], body.player_name),
        ) as cur:
            if await cur.fetchone():
                raise HTTPException(409, "A player with that name is already in the room")

        # Rejoin if previously left, otherwise insert as new player
        async with conn.execute(
            "SELECT id FROM players WHERE room_id=? AND name=? AND left=1",
            (room["id"], body.player_name),
        ) as cur:
            existing = await cur.fetchone()

        if existing:
            player_id = existing["id"]
            await conn.execute("UPDATE players SET left=0 WHERE id=?", (player_id,))
        else:
            async with conn.execute(
                "INSERT INTO players (room_id, name) VALUES (?, ?)",
                (room["id"], body.player_name),
            ) as cur:
                player_id = cur.lastrowid

        await conn.commit()

        async with conn.execute(
            "SELECT id, name, is_host FROM players WHERE room_id = ? AND left = 0 ORDER BY id",
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
            theme = body.theme or await pick_theme(conn, room["id"])
            await conn.execute(
                "INSERT INTO rounds (room_id, round_number, theme) VALUES (?, 1, ?)",
                (room["id"], theme),
            )
            await conn.execute("UPDATE rooms SET phase='submit' WHERE id=?", (room["id"],))

        elif phase == "submit":
            await conn.execute("UPDATE rooms SET phase='vote' WHERE id=?", (room["id"],))
            # Auto-create playlist using the pre-authorized app account
            rnd = await latest_round(conn, room["id"])
            if rnd:
                async with conn.execute(
                    "SELECT spotify_uri FROM submissions WHERE round_id=? AND spotify_uri IS NOT NULL",
                    (rnd["id"],),
                ) as cur:
                    uris = [r["spotify_uri"] for r in await cur.fetchall()]
                if uris:
                    try:
                        pl_name = f"{room['name']} — Round {rnd['round_number']}: {rnd['theme']}"
                        playlist_url = await sp.create_playlist(pl_name, uris)
                    except Exception:
                        logger.exception("Playlist creation failed")
                        playlist_url = None
                    if playlist_url:
                        await conn.execute(
                            "UPDATE rounds SET playlist_url=? WHERE id=?",
                            (playlist_url, rnd["id"]),
                        )

        elif phase == "vote":
            await conn.execute("UPDATE rooms SET phase='results' WHERE id=?", (room["id"],))

        elif phase == "results":
            rnd = await latest_round(conn, room["id"])
            if rnd and rnd["round_number"] >= room["max_rounds"]:
                # Last round finished — end the game
                await conn.execute(
                    "UPDATE rooms SET phase='finished', finished_at=datetime('now') WHERE id=?",
                    (room["id"],),
                )
            else:
                theme = body.theme or await pick_theme(conn, room["id"])
                next_num = (rnd["round_number"] + 1) if rnd else 1
                await conn.execute(
                    "INSERT INTO rounds (room_id, round_number, theme) VALUES (?, ?, ?)",
                    (room["id"], next_num, theme),
                )
                await conn.execute("UPDATE rooms SET phase='submit' WHERE id=?", (room["id"],))

        else:
            raise HTTPException(400, f"Cannot advance from phase: {phase}")

        await conn.commit()
        room = await fetch_room(conn, code)
        state = await build_room_state(conn, room)

    await manager.broadcast(code, {"type": "phase_change", "state": state})
    return state


@app.post("/api/rooms/{code}/leave")
async def leave_room(code: str, body: LeaveBody):
    async with get_conn() as conn:
        room = await fetch_room(conn, code)
        player = await fetch_player(conn, body.player_id, room["id"])

        if player["is_host"]:
            raise HTTPException(400, "Host must transfer or end the game instead of leaving")

        name = player["name"]
        await conn.execute("UPDATE players SET left=1 WHERE id=?", (body.player_id,))
        await conn.commit()

    await manager.broadcast(code, {"type": "player_left", "player_id": body.player_id, "player_name": name})
    return {"ok": True}


@app.post("/api/rooms/{code}/transfer")
async def transfer_host(code: str, body: TransferBody):
    async with get_conn() as conn:
        room = await fetch_room(conn, code)
        player = await fetch_player(conn, body.player_id, room["id"])

        if not player["is_host"]:
            raise HTTPException(403, "Only the host can transfer")

        new_host = await fetch_player(conn, body.new_host_id, room["id"])
        await conn.execute("UPDATE players SET is_host=1 WHERE id=?", (body.new_host_id,))
        if body.stay:
            await conn.execute("UPDATE players SET is_host=0 WHERE id=?", (body.player_id,))
        else:
            await conn.execute("DELETE FROM players WHERE id=?", (body.player_id,))
        await conn.commit()

        room = await fetch_room(conn, code)
        state = await build_room_state(conn, room)

    await manager.broadcast(code, {
        "type": "host_transferred",
        "state": state,
        "new_host_name": new_host["name"],
    })
    return {"ok": True}


@app.post("/api/rooms/{code}/forfeit")
async def forfeit_game(code: str, body: LeaveBody):
    async with get_conn() as conn:
        room = await fetch_room(conn, code)
        player = await fetch_player(conn, body.player_id, room["id"])

        if not player["is_host"]:
            raise HTTPException(403, "Only the host can end the game")

        await conn.execute("DELETE FROM rooms WHERE id=?", (room["id"],))
        await conn.commit()

    await manager.broadcast(code, {"type": "game_over"})
    return {"ok": True}


@app.post("/api/rooms/{code}/restart")
async def restart_game(code: str, body: LeaveBody):
    async with get_conn() as conn:
        room = await fetch_room(conn, code)
        player = await fetch_player(conn, body.player_id, room["id"])

        if not player["is_host"]:
            raise HTTPException(403, "Only the host can restart the game")

        await conn.execute("DELETE FROM rounds WHERE room_id=?", (room["id"],))
        await conn.execute("UPDATE rooms SET phase='waiting' WHERE id=?", (room["id"],))
        await conn.commit()

        room = await fetch_room(conn, code)
        state = await build_room_state(conn, room)

    await manager.broadcast(code, {"type": "phase_change", "state": state})
    return {"ok": True}


# ── Kick / votekick ───────────────────────────────────────────────────────────

@app.post("/api/rooms/{code}/kick")
async def kick_player(code: str, body: KickBody):
    async with get_conn() as conn:
        room = await fetch_room(conn, code)
        kicker = await fetch_player(conn, body.player_id, room["id"])
        if not kicker["is_host"]:
            raise HTTPException(403, "Only the host can kick players")
        target = await fetch_player(conn, body.target_id, room["id"])
        if target["is_host"]:
            raise HTTPException(400, "Cannot kick the host")
        name = target["name"]
        await conn.execute("UPDATE players SET left=1 WHERE id=?", (body.target_id,))
        await conn.commit()

    _votekicks.pop(code, None)
    await manager.broadcast(code, {"type": "player_left", "player_id": body.target_id, "player_name": name})
    await manager.broadcast(code, {"type": "player_kicked", "target_name": name})
    return {"ok": True}


async def _resolve_votekick(code: str, target_id: int, target_name: str, passed: bool):
    _votekicks.pop(code, None)
    if passed:
        async with get_conn() as conn:
            await conn.execute("UPDATE players SET left=1 WHERE id=?", (target_id,))
            await conn.commit()
        await manager.broadcast(code, {"type": "player_left", "player_id": target_id, "player_name": target_name})
    await manager.broadcast(code, {"type": "votekick_result", "target_name": target_name, "passed": passed})


async def _votekick_timeout(code: str, target_id: int, target_name: str):
    await asyncio.sleep(60)
    if code in _votekicks and _votekicks[code].target_id == target_id:
        await _resolve_votekick(code, target_id, target_name, False)


@app.post("/api/rooms/{code}/votekick")
async def start_or_vote_votekick(code: str, body: VotekickBody):
    async with get_conn() as conn:
        room = await fetch_room(conn, code)
        voter = await fetch_player(conn, body.player_id, room["id"])
        target = await fetch_player(conn, body.target_id, room["id"])

        if target["is_host"]:
            raise HTTPException(400, "Cannot votekick the host")

        async with conn.execute(
            "SELECT id FROM players WHERE room_id=? AND left=0",
            (room["id"],),
        ) as cur:
            total_count = len(await cur.fetchall())

    needed = total_count // 2 + 1
    target_name = target["name"]
    voter_name = voter["name"]

    state = _votekicks.get(code)
    if state is None or state.target_id != body.target_id:
        if body.player_id == body.target_id:
            raise HTTPException(400, "Cannot start a votekick against yourself")
        if state is not None:
            state.task.cancel()
        task = asyncio.create_task(_votekick_timeout(code, body.target_id, target_name))
        state = VotekickState(
            target_id=body.target_id,
            target_name=target_name,
            initiator_name=voter_name,
            yes_voters={body.player_id},
            task=task,
        )
        _votekicks[code] = state
        await manager.broadcast(code, {
            "type": "votekick_started",
            "target_id": body.target_id,
            "target_name": target_name,
            "initiated_by": voter_name,
            "yes": len(state.yes_voters),
            "no": len(state.no_voters),
            "needed": needed,
        })
    else:
        if body.vote == "yes":
            state.yes_voters.add(body.player_id)
            state.no_voters.discard(body.player_id)
        else:
            state.no_voters.add(body.player_id)
            state.yes_voters.discard(body.player_id)
        await manager.broadcast(code, {
            "type": "votekick_update",
            "target_id": body.target_id,
            "target_name": target_name,
            "yes": len(state.yes_voters),
            "no": len(state.no_voters),
            "needed": needed,
        })

    all_voted = (len(state.yes_voters) + len(state.no_voters)) >= total_count
    passed = len(state.yes_voters) >= needed
    if passed or all_voted:
        state.task.cancel()
        await _resolve_votekick(code, body.target_id, target_name, passed)

    return {"ok": True}


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

        async with conn.execute(
            "SELECT player_id FROM submissions WHERE round_id=? AND track_id=? AND player_id!=?",
            (body.round_id, body.track_id, body.player_id),
        ) as cur:
            conflict = await cur.fetchone()
        if conflict:
            raise HTTPException(409, "Another player has already submitted that track")

        await conn.execute(
            """INSERT INTO submissions
               (round_id, player_id, track_id, track_name, artist, album, album_art, spotify_uri, preview_url, note)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(round_id, player_id) DO UPDATE SET
                 track_id=excluded.track_id, track_name=excluded.track_name,
                 artist=excluded.artist, album=excluded.album,
                 album_art=excluded.album_art, spotify_uri=excluded.spotify_uri,
                 preview_url=excluded.preview_url, note=excluded.note""",
            (body.round_id, body.player_id, body.track_id, body.track_name,
             body.artist, body.album, body.album_art, body.spotify_uri, body.preview_url,
             body.note or None),
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

        async with conn.execute(
            """SELECT p.name FROM players p
               WHERE p.room_id = ? AND p.left = 0
               AND p.id NOT IN (SELECT player_id FROM submissions WHERE round_id = ?)
               ORDER BY p.id""",
            (rnd["room_id"], body.round_id),
        ) as cur:
            pending = [r[0] for r in await cur.fetchall()]

        room_code = room_row["code"]

    await manager.broadcast(room_code, {
        "type": "submission_update",
        "submitted": submitted,
        "total": total,
        "pending": pending,
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
            if not (0 <= vote.points <= 5):
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

        async with conn.execute(
            """SELECT p.name FROM players p
               WHERE p.room_id = ? AND p.left = 0
               AND p.id NOT IN (
                   SELECT DISTINCT v.voter_id FROM votes v
                   JOIN submissions s ON v.submission_id = s.id
                   WHERE s.round_id = ?
               )
               ORDER BY p.id""",
            (rnd["room_id"], body.round_id),
        ) as cur:
            pending = [r[0] for r in await cur.fetchall()]

        room_code = room_row["code"]

    await manager.broadcast(room_code, {"type": "vote_update", "voted": voted, "total": total, "pending": pending})
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
                      s.spotify_uri, s.preview_url, s.note,
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


@app.get("/api/rooms/{code}/leaderboard")
async def get_leaderboard(code: str):
    async with get_conn() as conn:
        room = await fetch_room(conn, code)
        async with conn.execute(
            """SELECT p.id, p.name, p.is_host,
                      COALESCE(SUM(v.points), 0) AS total_points
               FROM players p
               LEFT JOIN submissions s ON s.player_id = p.id
               LEFT JOIN votes v ON v.submission_id = s.id
               WHERE p.room_id = ? AND p.left = 0
               GROUP BY p.id, p.name, p.is_host
               ORDER BY total_points DESC""",
            (room["id"],),
        ) as cur:
            rows = [dict(r) for r in await cur.fetchall()]
    return {"leaderboard": rows}


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



@app.get("/spotify/setup")
async def spotify_setup():
    """One-time page to authorize the app account and retrieve tokens for env vars."""
    if not sp.CLIENT_ID:
        raise HTTPException(503, "Spotify not configured")
    url = sp.auth_url(state="setup", force=True)
    return HTMLResponse(f"""
<html><body style='font-family:sans-serif;padding:2rem;background:#0b0b10;color:#f0f0f5;max-width:600px;margin:auto'>
<h2>Ripple — Spotify Setup</h2>
<p>Click the button below to authorize Ripple to create playlists on your Spotify account.
You only need to do this once. After authorizing, copy the values shown and add them to Railway.</p>
<a href="{url}" style='display:inline-block;background:#1db954;color:#fff;padding:.75rem 1.5rem;
border-radius:8px;text-decoration:none;font-weight:700;margin-top:1rem'>Authorize with Spotify</a>
</body></html>""")


@app.get("/spotify/callback")
async def spotify_callback(
    code: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    error: Optional[str] = Query(None),
):
    if error or not code or not state:
        return HTMLResponse(f"<p>Error: {error or 'cancelled'}</p>")

    try:
        tokens = await sp.exchange_code(code)
        profile = await sp.user_profile(tokens["access_token"])
    except Exception as e:
        return HTMLResponse(f"<p>Auth failed: {e}</p>")

    if state == "setup":
        rt = tokens.get("refresh_token", "n/a")
        uid = profile["id"]
        return HTMLResponse(f"""
<html><body style='font-family:sans-serif;padding:2rem;background:#0b0b10;color:#f0f0f5;max-width:600px;margin:auto'>
<h2>✓ Authorized as {profile.get('display_name') or uid}</h2>
<p>Add these two variables to Railway (Settings → Variables):</p>
<pre style='background:#1c1c2a;padding:1rem;border-radius:8px;overflow-x:auto'>
SPOTIFY_REFRESH_TOKEN={rt}
SPOTIFY_USER_ID={uid}
</pre>
<p>Then redeploy. Playlists will be created automatically — no player needs to connect Spotify.</p>
</body></html>""")


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
