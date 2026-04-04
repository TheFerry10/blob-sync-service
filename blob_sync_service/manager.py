from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from azure.identity.aio import DefaultAzureCredential

from blob_sync_service.chunker import read_block
from blob_sync_service.config import Config
from blob_sync_service.db import Database
from blob_sync_service.uploader import _make_blob_client, commit_blob, upload_block

logger = logging.getLogger(__name__)


async def run_manager(
    config: Config,
    db: Database,
    stop_event: asyncio.Event,
) -> None:
    """Poll the DB for pending files and upload missing blocks."""
    credential = DefaultAzureCredential()
    sem = asyncio.Semaphore(config.max_parallel_uploads)

    logger.info(
        "Upload Manager started (poll=%ds, parallel=%d)",
        config.poll_interval_seconds,
        config.max_parallel_uploads,
    )

    try:
        while not stop_event.is_set():
            try:
                await _poll_once(config, db, credential, sem)
            except Exception:
                logger.exception("Error in upload manager poll")

            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=config.poll_interval_seconds
                )
                break  # stop_event was set
            except TimeoutError:
                pass  # poll again
    finally:
        await credential.close()


async def _poll_once(
    config: Config,
    db: Database,
    credential: DefaultAzureCredential,
    sem: asyncio.Semaphore,
) -> None:
    pending = await db.get_pending_files()
    if not pending:
        return

    for file_row in pending:
        file_id = file_row["id"]
        file_path = Path(file_row["path"])
        total_blocks = file_row["total_blocks"]

        if not file_path.exists():
            await db.mark_file_failed(file_id, "Source file no longer exists")
            logger.warning("File disappeared: %s", file_path)
            continue

        await db.mark_file_uploading(file_id)

        uploaded = await db.get_uploaded_block_indices(file_id)
        missing = [i for i in range(total_blocks) if i not in uploaded]

        blob_name = file_path.relative_to(config.watch_dir).as_posix()
        block_ids = await db.get_block_ids(file_id)

        blob_client = _make_blob_client(
            config.storage_account_url,
            config.container_name,
            blob_name,
            credential,
        )

        async with blob_client:
            if not missing:
                # All blocks already uploaded — just commit
                await _commit(db, blob_client, file_id, file_path)
                continue

            tasks = [
                asyncio.create_task(
                    _upload_one_block(
                        sem,
                        blob_client,
                        db,
                        config,
                        file_path,
                        file_id,
                        block_ids,
                        i,
                        total_blocks,
                    )
                )
                for i in missing
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            errors = [r for r in results if isinstance(r, Exception)]
            if errors:
                await db.mark_file_failed(
                    file_id, f"{len(errors)} block(s) failed: {errors[0]}"
                )
                logger.error(
                    "Upload failed for %s: %d block errors",
                    blob_name,
                    len(errors),
                )
                continue

            await _commit(db, blob_client, file_id, file_path)


async def _upload_one_block(
    sem,
    blob_client,
    db,
    config,
    file_path,
    file_id,
    block_ids,
    idx,
    total_blocks,
):
    async with sem:
        data = await read_block(file_path, idx, config.block_size_bytes)
        await upload_block(blob_client, block_ids[idx], data)
        await db.mark_block_uploaded(file_id, idx)
        logger.debug(
            "Block %d/%d uploaded for %s", idx + 1, total_blocks, file_path.name
        )


async def _commit(db, blob_client, file_id, file_path):
    block_ids = await db.get_block_ids(file_id)
    await commit_blob(blob_client, block_ids)
    await db.mark_file_committed(file_id)
    logger.info("File committed: %s", file_path.name)
