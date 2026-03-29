"""Tests for detect_folder_contamination.py — Issue #94 regression."""
import json
import sqlite3
import subprocess
import sys
import tempfile
import os
from pathlib import Path

import pytest

SCRIPT = str(Path(__file__).parent.parent / "detect_folder_contamination.py")


def _make_test_db(path: str, *, rows: list[dict], programs: list[str] | None = None) -> None:
    """Create a minimal test DB with paths + path_metadata + programs tables.

    rows: list of dicts with keys: path_id, path, program_title, human_reviewed (0/1)
    """
    con = sqlite3.connect(path)
    con.execute(
        "CREATE TABLE paths ("
        "  path_id TEXT PRIMARY KEY,"
        "  path TEXT NOT NULL UNIQUE,"
        "  drive TEXT NULL, dir TEXT NULL, name TEXT NULL, ext TEXT NULL,"
        "  created_at TEXT NOT NULL DEFAULT '',"
        "  updated_at TEXT NOT NULL DEFAULT ''"
        ")"
    )
    con.execute(
        "CREATE TABLE path_metadata ("
        "  path_id TEXT PRIMARY KEY,"
        "  source TEXT NOT NULL DEFAULT 'epg',"
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
    for r in rows:
        con.execute(
            "INSERT INTO paths (path_id, path) VALUES (?, ?)",
            (r["path_id"], r["path"]),
        )
        con.execute(
            "INSERT INTO path_metadata (path_id, source, program_title, human_reviewed) "
            "VALUES (?, ?, ?, ?)",
            (r["path_id"], "human_reviewed" if r.get("human_reviewed") else "epg",
             r["program_title"], 1 if r.get("human_reviewed") else 0),
        )
    for i, t in enumerate(programs or []):
        con.execute(
            "INSERT INTO programs (program_id, program_key, canonical_title) VALUES (?, ?, ?)",
            (f"prog{i}", f"key{i}", t),
        )
    con.commit()
    con.close()


def _run(db: str, extra_args: list[str]) -> dict:
    result = subprocess.run(
        [sys.executable, SCRIPT, "--db", db, "--dry-run"] + extra_args,
        capture_output=True, text=True,
    )
    assert result.returncode == 0, f"script failed:\n{result.stderr}"
    return json.loads(result.stdout)


class TestIssue94Regression:
    """Regression tests for Issue #94: broad-scan protection blocking operator-forced correction."""

    def test_path_like_with_canonical_title_returns_resolved_targets(self, tmp_path):
        """--path-like + --canonical-title must not suppress the row from resolvedTargets.

        Repro from Issue #94 comment: pathLike-only targeted call returns
        scannedRows=1 but resolvedTargets=[] when the title is human_reviewed
        and auto-detection finds no suggestion.
        """
        db = str(tmp_path / "test.db")
        path = r"\BS11ガンダムアワー\機動戦士ガンダム THE ORIGIN 前夜 赤い彗星 第1話 「ジオンの子」\ep.mkv"
        _make_test_db(db, rows=[{
            "path_id": "p1",
            "path": path,
            "program_title": "機動戦士ガンダム THE ORIGIN 前夜 赤い彗星 第1話 「ジオンの子」",
            "human_reviewed": 1,
        }])

        result = _run(db, [
            "--path-like", "%BS11ガンダムアワー%機動戦士ガンダム THE ORIGIN 前夜 赤い彗星%",
            "--canonical-title", "機動戦士ガンダム THE ORIGIN 前夜 赤い彗星",
        ])

        assert result["scannedRows"] == 1
        assert len(result.get("resolvedTargets", [])) == 1, (
            "resolvedTargets must contain the row even though it is human_reviewed "
            "with no auto-suggestion, because canonicalTitle was supplied"
        )
        assert result["totalContaminatedTitles"] == 1
        assert result["updateInstructions"][0]["path_id"] == "p1"
        assert result["updateInstructions"][0]["new_title"] == "機動戦士ガンダム THE ORIGIN 前夜 赤い彗星"

    def test_path_like_without_canonical_title_suppresses_clean_human_reviewed(self, tmp_path):
        """Broad targeted (path-like only) must still suppress clean human_reviewed rows
        when no canonicalTitle is given — the original Issue #94 protection must hold."""
        db = str(tmp_path / "test.db")
        path = r"\NHK\NHKスペシャル\ep.mkv"
        _make_test_db(db, rows=[{
            "path_id": "p1",
            "path": path,
            "program_title": "NHKスペシャル",
            "human_reviewed": 1,
        }])

        result = _run(db, ["--path-like", "%NHKスペシャル%"])

        assert result["scannedRows"] == 1
        # Clean human_reviewed with no suggestion and no canonicalTitle → suppressed
        assert len(result.get("resolvedTargets", [])) == 0
        assert result["totalContaminatedTitles"] == 0

    def test_program_title_narrow_targeted_always_includes_human_reviewed(self, tmp_path):
        """--program-title (narrow targeted) must always include human_reviewed rows."""
        db = str(tmp_path / "test.db")
        path = r"\NHK\NHKスペシャル\ep.mkv"
        _make_test_db(db, rows=[{
            "path_id": "p1",
            "path": path,
            "program_title": "NHKスペシャル",
            "human_reviewed": 1,
        }])

        result = _run(db, [
            "--program-title", "NHKスペシャル",
            "--canonical-title", "NHKスペシャル改",
        ])

        assert result["scannedRows"] == 1
        assert len(result.get("resolvedTargets", [])) == 1
        assert result["totalContaminatedTitles"] == 1
