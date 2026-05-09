import os
import aiosqlite
from contextlib import asynccontextmanager
from pathlib import Path

DB_PATH = Path(os.getenv("DB_PATH", "ripple.db"))

_SCHEMA = """\
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS rooms (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    code       TEXT UNIQUE NOT NULL,
    name       TEXT NOT NULL,
    phase      TEXT NOT NULL DEFAULT 'waiting',
    max_rounds INTEGER NOT NULL DEFAULT 5,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS players (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id               INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    name                  TEXT NOT NULL,
    is_host               INTEGER NOT NULL DEFAULT 0,
    left                  INTEGER NOT NULL DEFAULT 0,
    spotify_access_token  TEXT,
    spotify_refresh_token TEXT,
    spotify_user_id       TEXT,
    joined_at             TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS rounds (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id      INTEGER NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    round_number INTEGER NOT NULL,
    theme        TEXT NOT NULL,
    playlist_url TEXT,
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS submissions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id     INTEGER NOT NULL REFERENCES rounds(id) ON DELETE CASCADE,
    player_id    INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    track_id     TEXT NOT NULL,
    track_name   TEXT NOT NULL,
    artist       TEXT NOT NULL,
    album        TEXT,
    album_art    TEXT,
    spotify_uri  TEXT,
    preview_url  TEXT,
    submitted_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(round_id, player_id)
);

CREATE TABLE IF NOT EXISTS votes (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    submission_id INTEGER NOT NULL REFERENCES submissions(id) ON DELETE CASCADE,
    voter_id      INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    points        INTEGER NOT NULL CHECK(points BETWEEN 1 AND 5),
    voted_at      TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(submission_id, voter_id)
);
"""


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.executescript(_SCHEMA)
        # migrate existing DBs that predate the playlist_url column
        for migration in [
            "ALTER TABLE rounds ADD COLUMN playlist_url TEXT",
            "ALTER TABLE rooms ADD COLUMN max_rounds INTEGER NOT NULL DEFAULT 5",
            "ALTER TABLE players ADD COLUMN left INTEGER NOT NULL DEFAULT 0",
        ]:
            try:
                await conn.execute(migration)
                await conn.commit()
            except Exception:
                pass


@asynccontextmanager
async def get_conn():
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA foreign_keys=ON")
        yield conn
