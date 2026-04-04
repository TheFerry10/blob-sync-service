import pytest


@pytest.fixture
def tmp_watch_dir(tmp_path):
    d = tmp_path / "watch"
    d.mkdir()
    return d


@pytest.fixture
def tmp_db_path(tmp_path):
    return tmp_path / "test.db"
