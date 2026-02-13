"""Shared fixtures for SAS Mapper tests."""

import sys
from pathlib import Path

import pytest

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from parser import load_patterns


@pytest.fixture(scope="session")
def patterns():
    """Compiled regex patterns from patterns.yaml."""
    return load_patterns(Path(__file__).parent.parent / "patterns.yaml")


@pytest.fixture
def db_conn(tmp_path):
    """Fresh SQLite database connection with schema initialized."""
    from db import init_db
    db_path = tmp_path / "test.db"
    conn = init_db(str(db_path))
    yield conn
    conn.close()
