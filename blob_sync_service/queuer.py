from __future__ import annotations

import asyncio
import hashlib
import logging
from pathlib import Path

from blob_sync_service.chunker import calculate_total_blocks, generate_block_ids
from blob_sync_service.db import Database

logger = logging.getLogger(__name__)

_BUF_SIZE = 256 * 1024  # 256 KB read buffer for hashing


async def _file_hash(path: Path) -> str:
    """Compute SHA-256 hash of a file in a thread to avoid blocking the loop."""

    def _hash() -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(_BUF_SIZE):
                h.update(chunk)
        return h.hexdigest()

    return await asyncio.to_thread(_hash)


async def _wait_for_stable(path: Path, debounce: float) -> bool:
    """Wait until the file's size/mtime stops changing.

    Returns True if the file stabilised, False if it disappeared.
    """
    try:
        prev = path.stat()
    except FileNotFoundError:
        return False

    await asyncio.sleep(debounce)

    try:
        curr = path.stat()
    except FileNotFoundError:
        return False

    if (curr.st_size, curr.st_mtime_ns) != (prev.st_size, prev.st_mtime_ns):
        # Still changing — wait one more round
        return await _wait_for_stable(path, debounce)

    return True


async def run_queuer(
    queue: asyncio.Queue[Path],
    db: Database,
    block_size: int,
    debounce: float,
    stop_event: asyncio.Event,
) -> None:
    """Consume file paths from the watcher queue, debounce, hash, and register in DB."""
    logger.info("Queuer started (debounce=%.1fs, block_size=%d)", debounce, block_size)
    while not stop_event.is_set():
        try:
            path = await asyncio.wait_for(queue.get(), timeout=1.0)
        except TimeoutError:
            continue

        logger.info("Queuer received: %s", path)

        if not await _wait_for_stable(path, debounce):
            logger.warning("File disappeared before stabilising: %s", path)
            continue

        # Verify file is readable
        try:
            with open(path, "rb"):
                pass
        except OSError:
            logger.warning("File is locked or unreadable: %s", path)
            continue

        file_hash = await _file_hash(path)

        if await db.is_already_committed(str(path), file_hash):
            logger.info("Skipping already-committed file: %s", path)
            continue

        total_blocks = calculate_total_blocks(path, block_size)
        block_ids = generate_block_ids(total_blocks)

        file_id = await db.upsert_file(str(path), file_hash, total_blocks, block_ids)
        logger.info(
            "Registered file id=%d hash=%s blocks=%d",
            file_id,
            file_hash[:12],
            total_blocks,
        )
