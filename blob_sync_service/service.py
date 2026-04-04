from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path

from blob_sync_service.config import Config
from blob_sync_service.db import Database
from blob_sync_service.manager import run_manager
from blob_sync_service.queuer import run_queuer
from blob_sync_service.watcher import watch_directory

logger = logging.getLogger(__name__)


async def run_service(config: Config) -> None:
    """Wire up all components and run until interrupted."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    )

    logger.info("Starting blob-sync-service")
    logger.info("  watch_dir          = %s", config.watch_dir)
    logger.info("  db_path            = %s", config.db_path)
    logger.info("  block_size         = %d bytes", config.block_size_bytes)
    logger.info("  max_parallel       = %d", config.max_parallel_uploads)
    logger.info("  poll_interval      = %ds", config.poll_interval_seconds)
    logger.info("  storage_account    = %s", config.storage_account_url)
    logger.info("  container          = %s", config.container_name)

    config.watch_dir.mkdir(parents=True, exist_ok=True)

    db = Database(str(config.db_path))
    await db.open()

    stop_event = asyncio.Event()
    queue: asyncio.Queue[Path] = asyncio.Queue()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: _handle_shutdown(stop_event))

    tasks = [
        asyncio.create_task(watch_directory(config.watch_dir, queue, stop_event)),
        asyncio.create_task(
            run_queuer(
                queue, db, config.block_size_bytes, config.debounce_seconds, stop_event
            )
        ),
        asyncio.create_task(run_manager(config, db, stop_event)),
    ]

    try:
        await asyncio.gather(*tasks)
    except (asyncio.CancelledError, OSError):
        pass
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await db.close()
        logger.info("blob-sync-service stopped")


def _handle_shutdown(stop_event: asyncio.Event) -> None:
    logger.info("Shutdown signal received, draining in-flight uploads…")
    stop_event.set()
