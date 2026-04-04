import asyncio
from pathlib import Path

import pytest

from blob_sync_service.watcher import watch_directory


@pytest.mark.asyncio
async def test_initial_scan_enqueues_existing_files(tmp_path):
    """Existing files in the watch dir should be enqueued at startup."""
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()

    # Create files before the watcher starts
    (watch_dir / "a.txt").write_text("aaa")
    sub = watch_dir / "sub"
    sub.mkdir()
    (sub / "b.jpg").write_bytes(b"\xff\xd8" * 10)
    (sub / "c.mp4").write_bytes(b"\x00" * 100)

    queue: asyncio.Queue[Path] = asyncio.Queue()
    stop_event = asyncio.Event()

    # Stop immediately so we only get the initial scan, not the awatch loop
    stop_event.set()

    await watch_directory(watch_dir, queue, stop_event)

    enqueued = set()
    while not queue.empty():
        enqueued.add(await queue.get())

    assert enqueued == {
        watch_dir / "a.txt",
        sub / "b.jpg",
        sub / "c.mp4",
    }


@pytest.mark.asyncio
async def test_initial_scan_skips_directories(tmp_path):
    """Directories themselves should not be enqueued, only files."""
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    (watch_dir / "subdir").mkdir()
    (watch_dir / "file.txt").write_text("data")

    queue: asyncio.Queue[Path] = asyncio.Queue()
    stop_event = asyncio.Event()
    stop_event.set()

    await watch_directory(watch_dir, queue, stop_event)

    enqueued = []
    while not queue.empty():
        enqueued.append(await queue.get())

    assert enqueued == [watch_dir / "file.txt"]


@pytest.mark.asyncio
async def test_initial_scan_empty_directory(tmp_path):
    """An empty watch dir should enqueue nothing."""
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()

    queue: asyncio.Queue[Path] = asyncio.Queue()
    stop_event = asyncio.Event()
    stop_event.set()

    await watch_directory(watch_dir, queue, stop_event)

    assert queue.empty()
