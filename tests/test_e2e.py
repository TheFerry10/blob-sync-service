"""End-to-end test against Azurite.

Requires:
  - Azurite blob service running on 127.0.0.1:10000

The test exercises the full pipeline:
    write file → queuer (hash + DB register) → manager (stage blocks → commit)
and then verifies the blob content in Azurite matches the original file.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from unittest.mock import patch

import pytest
import pytest_asyncio
from azure.storage.blob.aio import BlobClient, BlobServiceClient

from blob_sync_service.config import Config
from blob_sync_service.db import Database
from blob_sync_service.manager import run_manager
from blob_sync_service.queuer import run_queuer

# ── Azurite well-known credentials ──────────────────────────────────

_AZURITE_ACCOUNT_NAME = "devstoreaccount1"
_AZURITE_ACCOUNT_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq"
    "/K1SZFPTOtr/KBHBeksoGMGw=="
)
_AZURITE_BLOB_URL = f"http://127.0.0.1:10000/{_AZURITE_ACCOUNT_NAME}"
_AZURITE_CONN_STR = (
    f"AccountName={_AZURITE_ACCOUNT_NAME};"
    f"AccountKey={_AZURITE_ACCOUNT_KEY};"
    f"DefaultEndpointsProtocol=http;"
    f"BlobEndpoint={_AZURITE_BLOB_URL};"
)

_CONTAINER = "landing-zone"

# Pin API version to one that Azurite supports
_AZURITE_API_VERSION = "2024-08-04"


# ── Helpers ─────────────────────────────────────────────────────────


def _make_azurite_blob_client(
    storage_account_url: str,
    container: str,
    blob_name: str,
    credential,
) -> BlobClient:
    """Create a BlobClient using the Azurite connection string directly."""
    svc = BlobServiceClient.from_connection_string(
        _AZURITE_CONN_STR, api_version=_AZURITE_API_VERSION
    )
    return svc.get_blob_client(container, blob_name)


async def _ensure_container_exists() -> None:
    """Create the landing-zone container in Azurite if it doesn't exist."""
    client = BlobServiceClient.from_connection_string(
        _AZURITE_CONN_STR, api_version=_AZURITE_API_VERSION
    )
    async with client:
        try:
            await client.create_container(_CONTAINER)
        except Exception:
            pass  # already exists


async def _download_blob(blob_name: str) -> bytes:
    """Download a blob from Azurite and return its content."""
    client = BlobServiceClient.from_connection_string(
        _AZURITE_CONN_STR, api_version=_AZURITE_API_VERSION
    )
    async with client:
        blob = client.get_blob_client(_CONTAINER, blob_name)
        stream = await blob.download_blob()
        return await stream.readall()


async def _delete_blob(blob_name: str) -> None:
    """Clean up a blob after test."""
    client = BlobServiceClient.from_connection_string(
        _AZURITE_CONN_STR, api_version=_AZURITE_API_VERSION
    )
    async with client:
        blob = client.get_blob_client(_CONTAINER, blob_name)
        try:
            await blob.delete_blob()
        except Exception:
            pass


async def _run_manager_patched(config, db, stop_event):
    """Run the manager with _make_blob_client patched for Azurite."""
    with (
        patch(
            "blob_sync_service.manager._make_blob_client",
            side_effect=_make_azurite_blob_client,
        ),
        patch(
            "blob_sync_service.manager.DefaultAzureCredential",
        ),
    ):
        await run_manager(config, db, stop_event)


# ── Fixtures ────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def db(tmp_path):
    d = Database(str(tmp_path / "e2e.db"))
    await d.open()
    yield d
    await d.close()


# ── Tests ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_e2e_file_upload_to_azurite(tmp_path, db):
    """Drop a file, run queuer+manager pipeline, verify blob in Azurite."""
    await _ensure_container_exists()

    # -- 1. Prepare a test file -----------------------------------------
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()

    file_content = b"Hello Azurite! " * 100  # 1500 bytes
    test_file = watch_dir / "test-upload.bin"
    test_file.write_bytes(file_content)

    block_size = 512  # small blocks to exercise multi-block logic

    config = Config(
        watch_dir=watch_dir,
        db_path=tmp_path / "e2e.db",
        storage_account_url=_AZURITE_BLOB_URL,
        container_name=_CONTAINER,
        block_size_bytes=block_size,
        max_parallel_uploads=2,
        poll_interval_seconds=1,
        debounce_seconds=0.1,
    )

    # -- 2. Run the pipeline --------------------------------------------
    queue: asyncio.Queue[Path] = asyncio.Queue()
    await queue.put(test_file)

    stop_event = asyncio.Event()

    # Let the queuer process exactly one file then stop
    async def _run_queuer_once():
        await run_queuer(queue, db, block_size, 0.1, stop_event)

    queuer_task = asyncio.create_task(_run_queuer_once())

    # Wait for the file to appear in the DB
    for _ in range(30):
        pending = await db.get_pending_files()
        if pending:
            break
        await asyncio.sleep(0.1)

    assert pending, "Queuer did not register the file in time"

    # Run manager (patched for Azurite)
    manager_task = asyncio.create_task(_run_manager_patched(config, db, stop_event))

    # Wait for the file to reach Committed state
    for _ in range(60):
        rows = await db.get_pending_files()
        if not rows:
            cursor = await db._conn.execute(
                "SELECT state FROM files WHERE path = ?",
                (str(test_file),),
            )
            row = await cursor.fetchone()
            if row and row[0] == "Committed":
                break
        await asyncio.sleep(0.5)

    # Signal everything to stop
    stop_event.set()
    await asyncio.gather(queuer_task, manager_task, return_exceptions=True)

    # -- 3. Verify blob in Azurite --------------------------------------
    downloaded = await _download_blob("test-upload.bin")
    assert downloaded == file_content, (
        f"Blob content mismatch: expected {len(file_content)} bytes, "
        f"got {len(downloaded)} bytes"
    )

    # -- 4. Verify DB state ---------------------------------------------
    cursor = await db._conn.execute(
        "SELECT state FROM files WHERE path = ?", (str(test_file),)
    )
    row = await cursor.fetchone()
    assert row[0] == "Committed"

    # Cleanup
    await _delete_blob("test-upload.bin")


@pytest.mark.asyncio
async def test_e2e_multiblock_file(tmp_path, db):
    """Verify a file larger than one block is correctly reassembled."""
    await _ensure_container_exists()

    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()

    # 3 full blocks + 1 partial = 4 blocks total
    block_size = 256
    file_content = os.urandom(block_size * 3 + 100)
    test_file = watch_dir / "multi-block.dat"
    test_file.write_bytes(file_content)

    config = Config(
        watch_dir=watch_dir,
        db_path=tmp_path / "e2e.db",
        storage_account_url=_AZURITE_BLOB_URL,
        container_name=_CONTAINER,
        block_size_bytes=block_size,
        max_parallel_uploads=4,
        poll_interval_seconds=1,
        debounce_seconds=0.1,
    )

    queue: asyncio.Queue[Path] = asyncio.Queue()
    await queue.put(test_file)

    stop_event = asyncio.Event()

    queuer_task = asyncio.create_task(
        run_queuer(queue, db, block_size, 0.1, stop_event)
    )

    for _ in range(30):
        pending = await db.get_pending_files()
        if pending:
            break
        await asyncio.sleep(0.1)

    manager_task = asyncio.create_task(_run_manager_patched(config, db, stop_event))

    for _ in range(60):
        rows = await db.get_pending_files()
        if not rows:
            cursor = await db._conn.execute(
                "SELECT state FROM files WHERE path = ?",
                (str(test_file),),
            )
            row = await cursor.fetchone()
            if row and row[0] == "Committed":
                break
        await asyncio.sleep(0.5)

    stop_event.set()
    await asyncio.gather(queuer_task, manager_task, return_exceptions=True)

    downloaded = await _download_blob("multi-block.dat")
    assert downloaded == file_content
    assert len(downloaded) == block_size * 3 + 100

    # Verify all 4 blocks were tracked
    cursor = await db._conn.execute(
        "SELECT id FROM files WHERE path = ?", (str(test_file),)
    )
    file_row = await cursor.fetchone()
    uploaded = await db.get_uploaded_block_indices(file_row[0])
    assert uploaded == {0, 1, 2, 3}

    await _delete_blob("multi-block.dat")
