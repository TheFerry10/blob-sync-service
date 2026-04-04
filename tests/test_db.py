import pytest
import pytest_asyncio

from blob_sync_service.db import Database, FileState


@pytest_asyncio.fixture
async def db(tmp_path):
    d = Database(str(tmp_path / "test.db"))
    await d.open()
    yield d
    await d.close()


@pytest.mark.asyncio
async def test_upsert_and_get_pending(db):
    file_id = await db.upsert_file("/tmp/a.mp4", "abc123", 3, ["AA==", "AQ==", "Ag=="])
    assert file_id >= 1

    pending = await db.get_pending_files()
    assert len(pending) == 1
    assert pending[0]["path"] == "/tmp/a.mp4"
    assert pending[0]["state"] == FileState.PENDING.value


@pytest.mark.asyncio
async def test_upsert_idempotent(db):
    id1 = await db.upsert_file("/tmp/a.mp4", "abc123", 3, ["AA==", "AQ==", "Ag=="])
    id2 = await db.upsert_file("/tmp/a.mp4", "abc123", 3, ["AA==", "AQ==", "Ag=="])
    assert id1 == id2


@pytest.mark.asyncio
async def test_block_upload_tracking(db):
    file_id = await db.upsert_file(
        "/tmp/b.mp4", "def456", 4, ["AA==", "AQ==", "Ag==", "Aw=="]
    )
    uploaded = await db.get_uploaded_block_indices(file_id)
    assert uploaded == set()

    await db.mark_block_uploaded(file_id, 0)
    await db.mark_block_uploaded(file_id, 2)
    uploaded = await db.get_uploaded_block_indices(file_id)
    assert uploaded == {0, 2}


@pytest.mark.asyncio
async def test_file_state_transitions(db):
    file_id = await db.upsert_file("/tmp/c.mp4", "ghi789", 2, ["AA==", "AQ=="])

    await db.mark_file_uploading(file_id)
    pending = await db.get_pending_files()
    assert pending[0]["state"] == FileState.UPLOADING.value

    await db.mark_file_committed(file_id)
    pending = await db.get_pending_files()
    assert len(pending) == 0


@pytest.mark.asyncio
async def test_mark_file_failed(db):
    file_id = await db.upsert_file("/tmp/d.mp4", "jkl012", 1, ["AA=="])

    await db.mark_file_failed(file_id, "network timeout")
    pending = await db.get_pending_files()
    assert len(pending) == 0


@pytest.mark.asyncio
async def test_get_block_ids(db):
    ids = ["AAAA", "BBBB", "CCCC"]
    file_id = await db.upsert_file("/tmp/e.mp4", "mno345", 3, ids)
    result = await db.get_block_ids(file_id)
    assert result == ids
