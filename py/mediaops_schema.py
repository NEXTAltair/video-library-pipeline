"""SQLite schema helpers for mediaops.sqlite (v1).

This module intentionally uses only the Python standard library.
It preserves the existing table/index/constraint shapes used by the pipeline.
"""

from __future__ import annotations

import sqlite3


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
      FOREIGN KEY (path_id) REFERENCES paths(path_id)
    )
    """,
]


def connect_db(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    return con


def create_schema_if_needed(con: sqlite3.Connection) -> None:
    for stmt in DDL_STATEMENTS:
        con.execute(stmt)


def begin_immediate(con: sqlite3.Connection) -> None:
    con.execute("BEGIN IMMEDIATE")


def fetchone(con: sqlite3.Connection, sql: str, params: tuple = ()) -> sqlite3.Row | None:
    return con.execute(sql, params).fetchone()


def fetchall(con: sqlite3.Connection, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    return con.execute(sql, params).fetchall()

