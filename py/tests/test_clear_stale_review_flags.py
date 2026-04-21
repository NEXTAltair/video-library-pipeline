import json
import sqlite3

from clear_stale_review_flags import apply_clear_stale_review_flags, find_stale_review_flag_candidates


def make_connection() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute(
        "CREATE TABLE paths ("
        "  path_id TEXT PRIMARY KEY,"
        "  path TEXT NOT NULL"
        ")"
    )
    con.execute(
        "CREATE TABLE path_metadata ("
        "  path_id TEXT PRIMARY KEY,"
        "  source TEXT NOT NULL,"
        "  data_json TEXT NOT NULL DEFAULT '{}',"
        "  updated_at TEXT NOT NULL DEFAULT '',"
        "  program_title TEXT,"
        "  needs_review INTEGER NOT NULL DEFAULT 0"
        ")"
    )
    return con


def insert_metadata(
    con: sqlite3.Connection,
    *,
    path_id: str,
    path: str,
    program_title: str,
    needs_review_reason: str,
    source: str = "human_reviewed",
    needs_review: int = 1,
) -> None:
    con.execute("INSERT INTO paths (path_id, path) VALUES (?, ?)", (path_id, path))
    con.execute(
        """INSERT INTO path_metadata (path_id, source, data_json, program_title, needs_review)
           VALUES (?, ?, ?, ?, ?)""",
        (
            path_id,
            source,
            json.dumps({
                "needs_review": bool(needs_review),
                "needs_review_reason": needs_review_reason,
            }, ensure_ascii=False),
            program_title,
            needs_review,
        ),
    )
    con.commit()


def test_find_candidates_only_returns_title_clean_human_reviewed_rows() -> None:
    con = make_connection()
    insert_metadata(
        con,
        path_id="p1",
        path=r"B:\VideoLibrary\Show\2026\04\episode.ts",
        program_title="Show",
        needs_review_reason="suspicious_program_title",
    )
    insert_metadata(
        con,
        path_id="p2",
        path=r"B:\VideoLibrary\Show\2026\04\episode.ts",
        program_title="Show",
        needs_review_reason="suspicious_program_title,missing_air_date",
    )
    insert_metadata(
        con,
        path_id="p3",
        path=r"B:\VideoLibrary\Show\2026\04\episode.ts",
        program_title="Show▽Episode",
        needs_review_reason="subtitle_separator_in_program_title",
    )
    insert_metadata(
        con,
        path_id="p4",
        path=r"B:\VideoLibrary\VeryLongProgram\2026\04\episode.ts",
        program_title="Very",
        needs_review_reason="suspicious_program_title_shortened",
    )

    candidates = find_stale_review_flag_candidates(con)

    assert [row["path_id"] for row in candidates] == ["p1"]


def test_apply_clear_stale_review_flags_resets_db_state() -> None:
    con = make_connection()
    insert_metadata(
        con,
        path_id="p1",
        path=r"B:\VideoLibrary\Show\2026\04\episode.ts",
        program_title="Show",
        needs_review_reason="suspicious_program_title",
    )

    candidates = find_stale_review_flag_candidates(con)
    updated_rows = apply_clear_stale_review_flags(con, candidates)
    con.commit()

    row = con.execute(
        "SELECT needs_review, data_json FROM path_metadata WHERE path_id = ?",
        ("p1",),
    ).fetchone()
    data = json.loads(row["data_json"])

    assert updated_rows == 1
    assert row["needs_review"] == 0
    assert data["needs_review"] is False
    assert data["needs_review_reason"] == ""
