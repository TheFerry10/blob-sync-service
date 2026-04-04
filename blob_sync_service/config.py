from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

_DEFAULT_BLOCK_SIZE = 8 * 1024 * 1024  # 8 MB
_DEFAULT_MAX_PARALLEL = 4
_DEFAULT_POLL_INTERVAL = 5
_DEFAULT_DEBOUNCE_SECONDS = 2.0


@dataclass(frozen=True)
class Config:
    watch_dir: Path
    db_path: Path
    storage_account_url: str
    container_name: str = "landing-zone"
    block_size_bytes: int = _DEFAULT_BLOCK_SIZE
    max_parallel_uploads: int = _DEFAULT_MAX_PARALLEL
    poll_interval_seconds: int = _DEFAULT_POLL_INTERVAL
    debounce_seconds: float = _DEFAULT_DEBOUNCE_SECONDS


def load_config(config_path: str | Path | None = None) -> Config:
    """Load configuration from environment variables, falling back to a YAML file."""
    # Start with YAML defaults if a config file exists
    file_values: dict = {}
    path = Path(config_path) if config_path else Path("config.yaml")
    if path.exists():
        with open(path) as f:
            file_values = yaml.safe_load(f) or {}

    def _get(key: str, default: str | None = None) -> str | None:
        return os.environ.get(key) or file_values.get(key.lower()) or default

    watch_dir = _get("WATCH_DIR")
    if not watch_dir:
        raise ValueError(
            "WATCH_DIR must be set via environment variable or config.yaml"
        )

    storage_account_url = _get("STORAGE_ACCOUNT_URL")
    if not storage_account_url:
        raise ValueError(
            "STORAGE_ACCOUNT_URL must be set via environment variable or config.yaml"
        )

    return Config(
        watch_dir=Path(watch_dir),
        db_path=Path(_get("DB_PATH", "blob_sync.db")),
        storage_account_url=storage_account_url,
        container_name=_get("CONTAINER_NAME", "landing-zone"),
        block_size_bytes=int(_get("BLOCK_SIZE_BYTES", str(_DEFAULT_BLOCK_SIZE))),
        max_parallel_uploads=int(
            _get("MAX_PARALLEL_UPLOADS", str(_DEFAULT_MAX_PARALLEL))
        ),
        poll_interval_seconds=int(
            _get("POLL_INTERVAL_SECONDS", str(_DEFAULT_POLL_INTERVAL))
        ),
        debounce_seconds=float(
            _get("DEBOUNCE_SECONDS", str(_DEFAULT_DEBOUNCE_SECONDS))
        ),
    )
