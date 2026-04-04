from __future__ import annotations

import logging
from datetime import datetime, timezone
from enum import Enum

import aiosqlite

logger = logging.getLogger(__name__)


class FileState(str, Enum):
    PENDING = "Pending"
    UPLOADING = "Uploading"
    COMMITTED = "Committed"
    FAILED = "Failed"


class BlockState(str, Enum):
    PENDING = "Pending"
    UPLOADED = "Uploaded"
    FAILED = "Failed"


_SCHEMA = """\
CREATE TABLE IF NOT EXISTS files (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    path        TEXT    NOT NULL,
    file_hash   TEXT    NOT NULL,
    total_blocks INTEGER NOT NULL,
    state       TEXT    NOT NULL DEFAULT 'Pending',
    error_msg   TEXT,
    created_at  TEXT    NOT NULL,
    updated_at  TEXT    NOT NULL,
    UNIQUE(path, file_hash)
);

CREATE TABLE IF NOT EXISTS blocks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id     INTEGER NOT NULL REFERENCES files(id),
    block_index INTEGER NOT NULL,
    block_id    TEXT    NOT NULL,
    state       TEXT    NOT NULL DEFAULT 'Pending',
    updated_at  TEXT    NOT NULL,
    UNIQUE(file_id, block_index)
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Database:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def open(self) -> None:
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(_SCHEMA)
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.commit()
        logger.info("Database opened at %s", self._db_path)

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None

    @property
    def _conn(self) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError("Database is not open")
        return self._db

    # ── File operations ──────────────────────────────────────────────

    async def upsert_file(
        self, path: str, file_hash: str, total_blocks: int, block_ids: list[str]
    ) -> int:
        """Insert or update a file record, returning its id.

        If the same path+hash already exists, resets it to Pending.
        Also inserts block rows for any that don't exist yet.
        """
        now = _now()
        cursor = await self._conn.execute(
            """INSERT INTO files (path, file_hash, total_blocks, state, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(path, file_hash) DO UPDATE SET
                   state = 'Pending', total_blocks = excluded.total_blocks,
                   updated_at = excluded.updated_at
               RETURNING id""",
            (path, file_hash, total_blocks, FileState.PENDING.value, now, now),
        )
        row = await cursor.fetchone()
        file_id: int = row[0]

        # Ensure block rows exist
        for idx, block_id in enumerate(block_ids):
            await self._conn.execute(
                """INSERT INTO blocks (file_id, block_index, block_id, state, updated_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(file_id, block_index) DO NOTHING""",
                (file_id, idx, block_id, BlockState.PENDING.value, now),
            )

        await self._conn.commit()
        logger.debug(
            "Upserted file id=%d path=%s blocks=%d", file_id, path, total_blocks
        )
        return file_id

    async def get_pending_files(self) -> list[dict]:
        """Return files in Pending or Uploading state."""
        cursor = await self._conn.execute(
            "SELECT id, path, file_hash, total_blocks, state FROM files WHERE state IN (?, ?)",
            (FileState.PENDING.value, FileState.UPLOADING.value),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_uploaded_block_indices(self, file_id: int) -> set[int]:
        """Return set of block indices already uploaded for a file."""
        cursor = await self._conn.execute(
            "SELECT block_index FROM blocks WHERE file_id = ? AND state = ?",
            (file_id, BlockState.UPLOADED.value),
        )
        rows = await cursor.fetchall()
        return {r[0] for r in rows}

    async def get_block_ids(self, file_id: int) -> list[str]:
        """Return ordered list of block IDs for a file."""
        cursor = await self._conn.execute(
            "SELECT block_id FROM blocks WHERE file_id = ? ORDER BY block_index",
            (file_id,),
        )
        rows = await cursor.fetchall()
        return [r[0] for r in rows]

    async def mark_file_uploading(self, file_id: int) -> None:
        await self._conn.execute(
            "UPDATE files SET state = ?, updated_at = ? WHERE id = ?",
            (FileState.UPLOADING.value, _now(), file_id),
        )
        await self._conn.commit()

    async def mark_block_uploaded(self, file_id: int, block_index: int) -> None:
        await self._conn.execute(
            "UPDATE blocks SET state = ?, updated_at = ? WHERE file_id = ? AND block_index = ?",
            (BlockState.UPLOADED.value, _now(), file_id, block_index),
        )
        await self._conn.commit()

    async def mark_file_committed(self, file_id: int) -> None:
        await self._conn.execute(
            "UPDATE files SET state = ?, updated_at = ? WHERE id = ?",
            (FileState.COMMITTED.value, _now(), file_id),
        )
        await self._conn.commit()

    async def mark_file_failed(self, file_id: int, error_msg: str) -> None:
        await self._conn.execute(
            "UPDATE files SET state = ?, error_msg = ?, updated_at = ? WHERE id = ?",
            (FileState.FAILED.value, error_msg, _now(), file_id),
        )
        await self._conn.commit()

    async def is_already_committed(self, path: str, file_hash: str) -> bool:
        """Return True if this exact path+hash is already committed."""
        cursor = await self._conn.execute(
            "SELECT 1 FROM files WHERE path = ? AND file_hash = ? AND state = ?",
            (path, file_hash, FileState.COMMITTED.value),
        )
        return await cursor.fetchone() is not None
