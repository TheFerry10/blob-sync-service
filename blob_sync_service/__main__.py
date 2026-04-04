"""CLI entry point: python -m blob_sync_service [--config path/to/config.yaml]"""

from __future__ import annotations

import asyncio
import sys

from blob_sync_service.config import load_config
from blob_sync_service.service import run_service


def main() -> None:
    config_path = None
    if "--config" in sys.argv:
        idx = sys.argv.index("--config")
        if idx + 1 < len(sys.argv):
            config_path = sys.argv[idx + 1]

    config = load_config(config_path)
    asyncio.run(run_service(config))


if __name__ == "__main__":
    main()
