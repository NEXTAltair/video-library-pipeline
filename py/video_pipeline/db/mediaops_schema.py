"""SQLite schema helpers for mediaops.sqlite (v3).

This module intentionally uses only the Python standard library.
It preserves the existing table/index/constraint shapes used by the pipeline.
"""

from __future__ import annotations

import sqlite3

from epg_common import normalize_program_key


DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS runs (
      run_id TEXT PRIMARY KEY,
      kind TEXT NOT NULL,
      target_root TEXT NULL,
      started_at TEXT NOT NULL,
      finished_at TEXT NULL,
      tool_version TEXT NULL,
      notes TEXT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_runs_kind_started ON runs(kind, started_at)",
    """
    CREATE TABLE IF NOT EXISTS files (
      file_id TEXT PRIMARY KEY,
      size_bytes INTEGER NOT NULL,
      content_hash TEXT NULL,
      hash_algo TEXT NULL,
      duration_sec REAL NULL,
      width INTEGER NULL,
      height INTEGER NULL,
      codec TEXT NULL,
      bitrate INTEGER NULL,
      fps TEXT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_files_hash ON files(content_hash)",
    """
    CREATE TABLE IF NOT EXISTS paths (
      path_id TEXT PRIMARY KEY,
      path TEXT NOT NULL UNIQUE,
      drive TEXT NULL,
      dir TEXT NULL,
      name TEXT NULL,
      ext TEXT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_paths_dir ON paths(dir)",
    "CREATE INDEX IF NOT EXISTS idx_paths_ext ON paths(ext)",
    """
    CREATE TABLE IF NOT EXISTS file_paths (
      file_id TEXT NOT NULL,
      path_id TEXT NOT NULL,
      is_current INTEGER NOT NULL DEFAULT 1,
      first_seen_run_id TEXT NULL,
      last_seen_run_id TEXT NULL,
      CONSTRAINT uq_file_paths_file_path UNIQUE (file_id, path_id),
      FOREIGN KEY (file_id) REFERENCES files(file_id),
      FOREIGN KEY (path_id) REFERENCES paths(path_id),
      FOREIGN KEY (first_seen_run_id) REFERENCES runs(run_id),
      FOREIGN KEY (last_seen_run_id) REFERENCES runs(run_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_file_paths_current ON file_paths(is_current)",
    """
    CREATE TABLE IF NOT EXISTS observations (
      run_id TEXT NOT NULL,
      path_id TEXT NOT NULL,
      size_bytes INTEGER NOT NULL,
      mtime_utc TEXT NULL,
      type TEXT NULL,
      name_flags TEXT NULL,
      CONSTRAINT uq_observations_run_path UNIQUE (run_id, path_id),
      FOREIGN KEY (run_id) REFERENCES runs(run_id),
      FOREIGN KEY (path_id) REFERENCES paths(path_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_obs_run ON observations(run_id)",
    """
    CREATE TABLE IF NOT EXISTS events (
      event_id INTEGER PRIMARY KEY AUTOINCREMENT,
      run_id TEXT NOT NULL,
      ts TEXT NOT NULL,
      kind TEXT NOT NULL,
      src_path_id TEXT NULL,
      dst_path_id TEXT NULL,
      detail_json TEXT NULL,
      ok INTEGER NOT NULL DEFAULT 1,
      error TEXT NULL,
      FOREIGN KEY (run_id) REFERENCES runs(run_id),
      FOREIGN KEY (src_path_id) REFERENCES paths(path_id),
      FOREIGN KEY (dst_path_id) REFERENCES paths(path_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_events_run ON events(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_events_kind_ts ON events(kind, ts)",
    """
    CREATE TABLE IF NOT EXISTS tags (
      tag_id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      namespace TEXT NOT NULL DEFAULT 'tablacus',
      CONSTRAINT uq_tags_namespace_name UNIQUE (namespace, name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS path_tags (
      path_id TEXT NOT NULL,
      tag_id INTEGER NOT NULL,
      source TEXT NOT NULL DEFAULT 'tablacus',
      updated_at TEXT NOT NULL,
      CONSTRAINT uq_path_tags_triplet UNIQUE (path_id, tag_id, source),
      FOREIGN KEY (path_id) REFERENCES paths(path_id),
      FOREIGN KEY (tag_id) REFERENCES tags(tag_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_path_tags_tag ON path_tags(tag_id)",
    """
    CREATE TABLE IF NOT EXISTS path_metadata (
      path_id TEXT PRIMARY KEY,
      source TEXT NOT NULL,
      data_json TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      program_title TEXT,
      air_date TEXT,
      needs_review INTEGER NOT NULL DEFAULT 0,
      episode_no TEXT,
      subtitle TEXT,
      broadcaster TEXT,
      human_reviewed INTEGER NOT NULL DEFAULT 0,
      FOREIGN KEY (path_id) REFERENCES paths(path_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_path_metadata_program_title ON path_metadata(program_title)",


    "CREATE INDEX IF NOT EXISTS idx_path_metadata_air_date ON path_metadata(air_date)",
    "CREATE INDEX IF NOT EXISTS idx_path_metadata_needs_review ON path_metadata(needs_review)",
    """
    CREATE TABLE IF NOT EXISTS franchises (
      franchise_id TEXT PRIMARY KEY,
      franchise_name TEXT NOT NULL UNIQUE,
      created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS programs (
      program_id TEXT PRIMARY KEY,
      program_key TEXT NOT NULL UNIQUE,
      canonical_title TEXT NOT NULL,
      franchise_id TEXT,
      created_at TEXT NOT NULL,
      FOREIGN KEY (franchise_id) REFERENCES franchises(franchise_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_programs_franchise ON programs(franchise_id)",
    """
    CREATE TABLE IF NOT EXISTS broadcasts (
      broadcast_id TEXT PRIMARY KEY,
      program_id TEXT NOT NULL,
      air_date TEXT,
      start_time TEXT,
      end_time TEXT,
      broadcaster TEXT,
      match_key TEXT UNIQUE,
      data_json TEXT,
      created_at TEXT NOT NULL,
      is_rebroadcast_flag INTEGER,
      epg_genres TEXT,
      description TEXT,
      official_title TEXT,
      annotations TEXT,
      FOREIGN KEY (program_id) REFERENCES programs(program_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_broadcasts_program ON broadcasts(program_id)",
    "CREATE INDEX IF NOT EXISTS idx_broadcasts_air_date ON broadcasts(air_date)",
    "CREATE INDEX IF NOT EXISTS idx_broadcasts_official_title ON broadcasts(official_title)",
    """
    CREATE TABLE IF NOT EXISTS path_programs (
      path_id TEXT NOT NULL,
      program_id TEXT NOT NULL,
      broadcast_id TEXT,
      source TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (path_id, program_id),
      FOREIGN KEY (path_id) REFERENCES paths(path_id),
      FOREIGN KEY (program_id) REFERENCES programs(program_id),
      FOREIGN KEY (broadcast_id) REFERENCES broadcasts(broadcast_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_path_programs_program ON path_programs(program_id)",
    "CREATE INDEX IF NOT EXISTS idx_path_programs_broadcast ON path_programs(broadcast_id)",
    """
    CREATE TABLE IF NOT EXISTS broadcast_groups (
      group_id TEXT PRIMARY KEY,
      program_title TEXT NOT NULL,
      episode_key TEXT,
      created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS broadcast_group_members (
      group_id TEXT NOT NULL,
      path_id TEXT NOT NULL,
      broadcast_type TEXT NOT NULL DEFAULT 'unknown',
      air_date TEXT,
      broadcaster TEXT,
      added_at TEXT NOT NULL,
      broadcast_id TEXT,
      CONSTRAINT uq_bgm UNIQUE (group_id, path_id),
      FOREIGN KEY (group_id) REFERENCES broadcast_groups(group_id),
      FOREIGN KEY (path_id) REFERENCES paths(path_id),
      FOREIGN KEY (broadcast_id) REFERENCES broadcasts(broadcast_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_bgm_group ON broadcast_group_members(group_id)",
    "CREATE INDEX IF NOT EXISTS idx_bgm_path ON broadcast_group_members(path_id)",
    "CREATE INDEX IF NOT EXISTS idx_bgm_broadcast ON broadcast_group_members(broadcast_id)",
    """
    CREATE TABLE IF NOT EXISTS schema_version (
      version INTEGER NOT NULL,
      migrated_at TEXT NOT NULL
    )
    """,
    # -- Views --
    """
    CREATE VIEW IF NOT EXISTS v_program_titles AS
    SELECT
      pm.program_title,
      COUNT(*) AS file_count,
      MIN(pm.air_date) AS first_air_date,
      MAX(pm.air_date) AS last_air_date,
      SUM(pm.human_reviewed) AS reviewed_count,
      SUM(CASE WHEN pm.human_reviewed = 0 THEN 1 ELSE 0 END) AS unreviewed_count
    FROM path_metadata pm
    WHERE pm.program_title IS NOT NULL AND pm.program_title != ''
    GROUP BY pm.program_title
    ORDER BY pm.program_title
    """,
    """
    CREATE VIEW IF NOT EXISTS v_titles_needs_review AS
    SELECT
      pm.path_id,
      pm.program_title,
      p.name AS filename,
      pm.needs_review
    FROM path_metadata pm
    JOIN paths p ON p.path_id = pm.path_id
    WHERE pm.needs_review = 1
    ORDER BY pm.program_title
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_v_titles_needs_review_update
    INSTEAD OF UPDATE ON v_titles_needs_review
    BEGIN
      UPDATE path_metadata
        SET program_title = NEW.program_title,
            needs_review  = NEW.needs_review
        WHERE path_id = NEW.path_id;
    END
    """,
]

_FILES_NEW_COLUMNS: list[tuple[str, str]] = [
    ("duration_sec", "REAL"),
    ("width", "INTEGER"),
    ("height", "INTEGER"),
    ("codec", "TEXT"),
    ("bitrate", "INTEGER"),
    ("fps", "TEXT"),
]

_PATH_METADATA_NEW_COLUMNS: list[tuple[str, str, str]] = [
    ("program_title", "TEXT", ""),
    ("air_date", "TEXT", ""),
    ("needs_review", "INTEGER NOT NULL DEFAULT 0", ""),


    ("episode_no", "TEXT", ""),
    ("subtitle", "TEXT", ""),
    ("broadcaster", "TEXT", ""),
    ("human_reviewed", "INTEGER NOT NULL DEFAULT 0", ""),
]

_BROADCASTS_NEW_COLUMNS: list[tuple[str, str, str]] = [
    ("is_rebroadcast_flag", "INTEGER", ""),
    ("epg_genres", "TEXT", ""),
    ("description", "TEXT", ""),
    ("official_title", "TEXT", ""),
    ("annotations", "TEXT", ""),
]

_PROGRAMS_NEW_COLUMNS: list[tuple[str, str, str]] = [
    ("franchise_id", "TEXT", ""),
]

_BGM_NEW_COLUMNS: list[tuple[str, str, str]] = [
    ("broadcast_id", "TEXT", ""),
]


def _add_columns_if_missing(con: sqlite3.Connection, table: str, columns: list[tuple[str, str, str]]) -> None:
    existing = {str(row[1]) for row in con.execute(f"PRAGMA table_info({table})").fetchall()}
    for col, typ, _default in columns:
        if col not in existing:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")


def _migrate_files_columns(con: sqlite3.Connection) -> None:
    existing = {str(row[1]) for row in con.execute("PRAGMA table_info(files)").fetchall()}
    for col, typ in _FILES_NEW_COLUMNS:
        if col not in existing:
            con.execute(f"ALTER TABLE files ADD COLUMN {col} {typ}")


def _table_exists(con: sqlite3.Connection, name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def _migrate_to_v3(con: sqlite3.Connection) -> None:
    """Add v3 columns to existing tables and create new tables."""
    # Check if already migrated
    if _table_exists(con, "schema_version"):
        row = con.execute("SELECT MAX(version) FROM schema_version").fetchone()
        if row and row[0] is not None and int(row[0]) >= 3:
            return

    # Create new tables (franchises, schema_version) if not exist
    if not _table_exists(con, "franchises"):
        con.execute("""
            CREATE TABLE franchises (
              franchise_id TEXT PRIMARY KEY,
              franchise_name TEXT NOT NULL UNIQUE,
              created_at TEXT NOT NULL
            )
        """)
    if not _table_exists(con, "schema_version"):
        con.execute("""
            CREATE TABLE schema_version (
              version INTEGER NOT NULL,
              migrated_at TEXT NOT NULL
            )
        """)

    # Add new columns to existing tables
    if _table_exists(con, "path_metadata"):
        _add_columns_if_missing(con, "path_metadata", _PATH_METADATA_NEW_COLUMNS)
    if _table_exists(con, "broadcasts"):
        _add_columns_if_missing(con, "broadcasts", _BROADCASTS_NEW_COLUMNS)
    if _table_exists(con, "programs"):
        _add_columns_if_missing(con, "programs", _PROGRAMS_NEW_COLUMNS)
    if _table_exists(con, "broadcast_group_members"):
        _add_columns_if_missing(con, "broadcast_group_members", _BGM_NEW_COLUMNS)

    # Create new indexes (IF NOT EXISTS is safe)
    _v3_indexes = [
        "CREATE INDEX IF NOT EXISTS idx_path_metadata_program_title ON path_metadata(program_title)",


        "CREATE INDEX IF NOT EXISTS idx_path_metadata_air_date ON path_metadata(air_date)",
        "CREATE INDEX IF NOT EXISTS idx_path_metadata_needs_review ON path_metadata(needs_review)",
        "CREATE INDEX IF NOT EXISTS idx_broadcasts_official_title ON broadcasts(official_title)",
        "CREATE INDEX IF NOT EXISTS idx_programs_franchise ON programs(franchise_id)",
        "CREATE INDEX IF NOT EXISTS idx_bgm_broadcast ON broadcast_group_members(broadcast_id)",
    ]
    for idx in _v3_indexes:
        con.execute(idx)


def register_custom_functions(con: sqlite3.Connection) -> None:
    """Register Python functions as SQLite custom functions.

    These are required by triggers (e.g. trg_path_metadata_npk_*).
    Only available in Python connections — external tools like DBeaver
    won't have them, so triggers will not fire there.
    """
    con.create_function("normalize_program_key", 1, normalize_program_key)


def connect_db(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    register_custom_functions(con)
    return con


def _ensure_triggers(con: sqlite3.Connection) -> None:
    """Create triggers if missing."""
    for stmt in DDL_STATEMENTS:
        if "CREATE TRIGGER" in stmt:
            con.execute(stmt)


def _drop_normalized_program_key(con: sqlite3.Connection) -> None:
    """Drop the deprecated normalized_program_key column if it still exists.

    依存オブジェクトを先に除去してからカラムを落とす。
    順序: views → triggers → index → column → views 再作成
    """
    cols = [r["name"] for r in con.execute("PRAGMA table_info(path_metadata)").fetchall()]
    if "normalized_program_key" not in cols:
        return
    # 1. 旧 view を除去 (旧定義が normalized_program_key を参照している場合)
    views = [
        r[0]
        for r in con.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='view' "
            "AND sql LIKE '%normalized_program_key%'"
        ).fetchall()
    ]
    for v in views:
        con.execute(f"DROP VIEW IF EXISTS {v}")
    # 2. 旧 trigger を除去 (trg_path_metadata_npk_* など)
    triggers = [
        r[0]
        for r in con.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='trigger' AND tbl_name='path_metadata' "
            "AND sql LIKE '%normalized_program_key%'"
        ).fetchall()
    ]
    for trg in triggers:
        con.execute(f"DROP TRIGGER IF EXISTS {trg}")
    # 3. 旧 index を除去
    con.execute("DROP INDEX IF EXISTS idx_path_metadata_npk")
    # 4. カラムを除去
    con.execute("ALTER TABLE path_metadata DROP COLUMN normalized_program_key")
    # 5. 落とした view を新定義で再作成
    for stmt in DDL_STATEMENTS:
        if "CREATE VIEW" in stmt:
            con.execute(stmt)


def create_schema_if_needed(con: sqlite3.Connection) -> None:
    for stmt in DDL_STATEMENTS:
        con.execute(stmt)
    _migrate_files_columns(con)
    _migrate_to_v3(con)
    _ensure_triggers(con)
    _drop_normalized_program_key(con)


def begin_immediate(con: sqlite3.Connection) -> None:
    con.execute("BEGIN IMMEDIATE")


def fetchone(con: sqlite3.Connection, sql: str, params: tuple = ()) -> sqlite3.Row | None:
    return con.execute(sql, params).fetchone()


def fetchall(con: sqlite3.Connection, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    return con.execute(sql, params).fetchall()
