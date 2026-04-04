import base64

import pytest

from blob_sync_service.chunker import (
    calculate_total_blocks,
    generate_block_id,
    generate_block_ids,
    read_block,
)


def test_total_blocks_exact(tmp_path):
    f = tmp_path / "exact.bin"
    f.write_bytes(b"\x00" * (8 * 1024 * 1024))  # exactly 8 MB
    assert calculate_total_blocks(f, 8 * 1024 * 1024) == 1


def test_total_blocks_remainder(tmp_path):
    f = tmp_path / "remainder.bin"
    f.write_bytes(b"\x00" * (8 * 1024 * 1024 + 1))
    assert calculate_total_blocks(f, 8 * 1024 * 1024) == 2


def test_total_blocks_empty(tmp_path):
    f = tmp_path / "empty.bin"
    f.write_bytes(b"")
    assert calculate_total_blocks(f, 8 * 1024 * 1024) == 1


def test_block_id_deterministic():
    assert generate_block_id(0) == base64.b64encode(b"00000").decode()
    assert generate_block_id(42) == base64.b64encode(b"00042").decode()


def test_block_ids_length():
    ids = generate_block_ids(5)
    assert len(ids) == 5
    # All same length (Azure requirement)
    lengths = {len(i) for i in ids}
    assert len(lengths) == 1


@pytest.mark.asyncio
async def test_read_block(tmp_path):
    data = b"A" * 100 + b"B" * 100 + b"C" * 50
    f = tmp_path / "test.bin"
    f.write_bytes(data)

    block0 = await read_block(f, 0, 100)
    assert block0 == b"A" * 100

    block1 = await read_block(f, 1, 100)
    assert block1 == b"B" * 100

    block2 = await read_block(f, 2, 100)
    assert block2 == b"C" * 50
