from __future__ import annotations

import logging
from typing import Any

from azure.storage.blob import BlobBlock
from azure.storage.blob.aio import BlobClient

logger = logging.getLogger(__name__)


def _make_blob_client(
    storage_account_url: str,
    container: str,
    blob_name: str,
    credential: Any,
) -> BlobClient:
    """Create an async BlobClient with the given credential."""
    return BlobClient(
        account_url=storage_account_url,
        container_name=container,
        blob_name=blob_name,
        credential=credential,
    )


async def upload_block(
    blob_client: BlobClient,
    block_id: str,
    data: bytes,
) -> None:
    """Stage a single block via the Azure SDK (retries handled by the SDK)."""
    await blob_client.stage_block(block_id=block_id, data=data, length=len(data))


async def commit_blob(
    blob_client: BlobClient,
    block_ids: list[str],
) -> None:
    """Commit previously staged blocks via the Azure SDK."""
    block_list = [BlobBlock(block_id=bid) for bid in block_ids]
    await blob_client.commit_block_list(block_list)
    logger.info("Blob committed: %s", blob_client.blob_name)
