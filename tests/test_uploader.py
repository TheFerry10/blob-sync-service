from unittest.mock import AsyncMock

import pytest

from blob_sync_service.uploader import commit_blob, upload_block


@pytest.mark.asyncio
async def test_upload_block_success():
    blob_client = AsyncMock()
    blob_client.stage_block = AsyncMock()

    await upload_block(blob_client, "AA==", b"hello")

    blob_client.stage_block.assert_awaited_once_with(
        block_id="AA==", data=b"hello", length=5
    )


@pytest.mark.asyncio
async def test_upload_block_propagates_sdk_error():
    blob_client = AsyncMock()
    blob_client.stage_block = AsyncMock(side_effect=RuntimeError("boom"))

    with pytest.raises(RuntimeError, match="boom"):
        await upload_block(blob_client, "AA==", b"hello")


@pytest.mark.asyncio
async def test_commit_blob_success():
    blob_client = AsyncMock()
    blob_client.blob_name = "test.bin"
    blob_client.commit_block_list = AsyncMock()

    await commit_blob(blob_client, ["AA==", "AQ=="])

    blob_client.commit_block_list.assert_awaited_once()
    block_list = blob_client.commit_block_list.call_args[0][0]
    assert [b.id for b in block_list] == ["AA==", "AQ=="]


@pytest.mark.asyncio
async def test_make_blob_client():
    from unittest.mock import MagicMock

    from blob_sync_service.uploader import _make_blob_client

    credential = MagicMock()
    client = _make_blob_client(
        "https://account.blob.core.windows.net",
        "container",
        "blob.bin",
        credential,
    )
    assert client.container_name == "container"
    assert client.blob_name == "blob.bin"
    await client.close()
