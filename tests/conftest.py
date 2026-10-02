"""Test setup: point every service at throwaway local backends."""

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

TMP_DB = "/tmp/traffic_test.db"
if os.path.exists(TMP_DB):
    os.remove(TMP_DB)

os.environ["DATABASE_URL"] = f"sqlite:///{TMP_DB}"
os.environ["STREAM_BACKEND"] = "memory"
os.environ["CACHE_BACKEND"] = "memory"
os.environ["MODEL_DIR"] = str(ROOT / "ml" / "artifacts")


@pytest.fixture()
def test_db(tmp_path, monkeypatch):
    """Isolated sqlite DB per test: re-point settings and rebuild schema."""
    from common.config import get_settings
    from common.db import init_db

    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{tmp_path}/test.db")
    get_settings.cache_clear()
    init_db()
    yield tmp_path / "test.db"
    get_settings.cache_clear()
