import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from blob_sync_service.config import Config
from blob_sync_service.db import Database
from blob_sync_service.manager import _poll_once


@pytest_asyncio.fixture
async def db(tmp_path):
    d = Database(str(tmp_path / "test.db"))
    await d.open()
    yield d
    await d.close()


def _make_config(tmp_path: Path, **overrides) -> Config:
    defaults = dict(
        watch_dir=tmp_path / "watch",
        db_path=tmp_path / "test.db",
        storage_account_url="https://account.blob.core.windows.net",
        container_name="landing-zone",
        block_size_bytes=100,
        max_parallel_uploads=2,
        poll_interval_seconds=1,
        debounce_seconds=0.1,
    )
    defaults.update(overrides)
    return Config(**defaults)


@pytest.mark.asyncio
async def test_poll_uploads_and_commits(tmp_path, db):
    """Full flow: file with 2 blocks → upload both → commit."""
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    test_file = watch_dir / "video.mp4"
    test_file.write_bytes(b"A" * 100 + b"B" * 50)

    config = _make_config(tmp_path)

    block_ids = ["MDAwMDA=", "MDAwMDE="]
    file_id = await db.upsert_file(str(test_file), "fakehash", 2, block_ids)

    mock_blob_client = AsyncMock()
    mock_blob_client.stage_block = AsyncMock()
    mock_blob_client.commit_block_list = AsyncMock()
    mock_blob_client.blob_name = "video.mp4"
    mock_blob_client.__aenter__ = AsyncMock(return_value=mock_blob_client)
    mock_blob_client.__aexit__ = AsyncMock(return_value=False)

    credential = MagicMock()
    sem = asyncio.Semaphore(config.max_parallel_uploads)

    with patch(
        "blob_sync_service.manager._make_blob_client",
        return_value=mock_blob_client,
    ):
        await _poll_once(config, db, credential, sem)

    # Verify SDK calls
    assert mock_blob_client.stage_block.await_count == 2
    mock_blob_client.commit_block_list.assert_awaited_once()

    # File should be committed
    pending = await db.get_pending_files()
    assert len(pending) == 0

    uploaded = await db.get_uploaded_block_indices(file_id)
    assert uploaded == {0, 1}
