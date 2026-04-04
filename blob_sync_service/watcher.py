from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from watchfiles import Change, awatch

logger = logging.getLogger(__name__)


async def watch_directory(
    watch_dir: Path,
    queue: asyncio.Queue[Path],
    stop_event: asyncio.Event,
) -> None:
    """Watch *watch_dir* for new / modified files and push paths into *queue*.

    On startup, enqueues all existing files in the directory.
    Then watches for new/modified files until *stop_event* is set.
    """
    logger.info("Watching %s for new/modified files", watch_dir)

    # Scan existing files at startup
    existing = sorted(p for p in watch_dir.rglob("*") if p.is_file())
    if existing:
        logger.info("Initial scan: found %d existing file(s)", len(existing))
        for path in existing:
            await queue.put(path)

    async for changes in awatch(watch_dir, stop_event=stop_event):
        for change_type, path_str in changes:
            if change_type in (Change.added, Change.modified):
                path = Path(path_str)
                if path.is_file():
                    logger.debug("Detected %s: %s", change_type.name, path)
                    await queue.put(path)
