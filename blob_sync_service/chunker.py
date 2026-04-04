from __future__ import annotations

import asyncio
import base64
import math
from pathlib import Path


def calculate_total_blocks(path: Path, block_size: int) -> int:
    """Return the number of blocks needed for a file."""
    size = path.stat().st_size
    if size == 0:
        return 1  # Upload a single empty block
    return math.ceil(size / block_size)


def generate_block_id(block_index: int) -> str:
    """Return a deterministic, Azure-compatible block ID for a given index.

    Azure requires all block IDs within a blob to be the same length and
    be valid base64 strings.
    """
    return base64.b64encode(f"{block_index:05d}".encode()).decode()


def generate_block_ids(total_blocks: int) -> list[str]:
    """Return ordered list of block IDs for *total_blocks*."""
    return [generate_block_id(i) for i in range(total_blocks)]


async def read_block(path: Path, block_index: int, block_size: int) -> bytes:
    """Read a single block from disk asynchronously."""

    def _read() -> bytes:
        offset = block_index * block_size
        with open(path, "rb") as f:
            f.seek(offset)
            return f.read(block_size)

    return await asyncio.to_thread(_read)
