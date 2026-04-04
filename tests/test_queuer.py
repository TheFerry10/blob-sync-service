import asyncio
from pathlib import Path

import pytest
import pytest_asyncio

from blob_sync_service.db import Database
from blob_sync_service.queuer import run_queuer


@pytest_asyncio.fixture
async def db(tmp_path):
    d = Database(str(tmp_path / "test.db"))
    await d.open()
    yield d
    await d.close()


@pytest.mark.asyncio
async def test_queuer_registers_file(tmp_path, db):
    """A file placed on the queue should end up in the DB after debounce."""
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    test_file = watch_dir / "video.mp4"
    test_file.write_bytes(b"\x00" * 1000)

    queue: asyncio.Queue[Path] = asyncio.Queue()
    stop_event = asyncio.Event()

    await queue.put(test_file)

    # Run queuer briefly and then stop
    async def _stop_after_processing():
        await asyncio.sleep(0.5)  # Let it process the item
        stop_event.set()

    await asyncio.gather(
        run_queuer(queue, db, block_size=500, debounce=0.1, stop_event=stop_event),
        _stop_after_processing(),
    )

    pending = await db.get_pending_files()
    assert len(pending) == 1
    assert pending[0]["path"] == str(test_file)
    assert pending[0]["total_blocks"] == 2  # 1000 bytes / 500 byte blocks


@pytest.mark.asyncio
async def test_queuer_skips_committed_file(tmp_path, db):
    """A file that is already committed with the same hash should be skipped."""
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    test_file = watch_dir / "video.mp4"
    test_file.write_bytes(b"\x00" * 1000)

    queue: asyncio.Queue[Path] = asyncio.Queue()
    stop_event = asyncio.Event()

    # First pass: register and commit the file
    await queue.put(test_file)

    async def _stop_after_processing():
        await asyncio.sleep(0.5)
        stop_event.set()

    await asyncio.gather(
        run_queuer(queue, db, block_size=500, debounce=0.1, stop_event=stop_event),
        _stop_after_processing(),
    )

    pending = await db.get_pending_files()
    assert len(pending) == 1
    file_id = pending[0]["id"]
    await db.mark_file_committed(file_id)

    # Second pass: same file should be skipped
    stop_event.clear()
    await queue.put(test_file)

    async def _stop_again():
        await asyncio.sleep(0.5)
        stop_event.set()

    await asyncio.gather(
        run_queuer(queue, db, block_size=500, debounce=0.1, stop_event=stop_event),
        _stop_again(),
    )

    # Still no pending files — the duplicate was skipped
    pending = await db.get_pending_files()
    assert len(pending) == 0
