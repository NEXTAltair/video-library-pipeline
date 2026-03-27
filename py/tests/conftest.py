"""Shared pytest fixtures for video-library-pipeline tests."""
import sqlite3
import sys
import os

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture
def make_db():
    """Factory fixture that creates an in-memory SQLite DB with path_metadata and programs tables.

    Usage:
        def test_something(make_db):
            con = make_db(["タイトルA", "タイトルB"], human_reviewed=["タイトルA"])
    """
    def _factory(
        titles: list,
        *,
        human_reviewed: list | None = None,
        programs: list | None = None,
    ) -> sqlite3.Connection:
        con = sqlite3.connect(":memory:")
        con.row_factory = sqlite3.Row
        con.execute(
            "CREATE TABLE path_metadata ("
            "  path_id TEXT PRIMARY KEY,"
            "  source TEXT NOT NULL,"
            "  data_json TEXT NOT NULL DEFAULT '{}',"
            "  updated_at TEXT NOT NULL DEFAULT '',"
            "  program_title TEXT,"
            "  human_reviewed INTEGER NOT NULL DEFAULT 0"
            ")"
        )
        con.execute(
            "CREATE TABLE programs ("
            "  program_id TEXT PRIMARY KEY,"
            "  program_key TEXT NOT NULL UNIQUE,"
            "  canonical_title TEXT NOT NULL,"
            "  created_at TEXT NOT NULL DEFAULT '',"
            "  franchise_id TEXT"
            ")"
        )
        for i, t in enumerate(titles):
            is_hr = human_reviewed and t in human_reviewed
            con.execute(
                "INSERT INTO path_metadata (path_id, source, program_title, human_reviewed) "
                "VALUES (?, ?, ?, ?)",
                (f"p{i}", "human_reviewed" if is_hr else "epg", t, 1 if is_hr else 0),
            )
        for i, t in enumerate(programs or []):
            con.execute(
                "INSERT INTO programs (program_id, program_key, canonical_title) "
                "VALUES (?, ?, ?)",
                (f"prog{i}", f"key{i}", t),
            )
        con.commit()
        return con

    return _factory
