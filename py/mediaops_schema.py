"""mediaops_schema.py

SQLAlchemy Core schema for mediaops.sqlite (v1).

Design goals:
- Separate *path* (location; aligns with Tablacus labels) from *file* (future stable identity).
- Keep run-scoped snapshots (observations) for reproducible reports.
- Keep an append-only audit trail (events).
- Tags are attached to paths (v1), imported from Tablacus label3.db.
- Arbitrary metadata attaches to paths as JSON (path_metadata).

DB path is provided by runtime config/CLI arguments.

NOTE: SQLite JSON is stored as TEXT; validate at the application layer.
"""

from __future__ import annotations

from sqlalchemy import (
    Table,
    Column,
    MetaData,
    ForeignKey,
    Integer,
    Text,
    String,
    UniqueConstraint,
    Index,
)

metadata = MetaData()

runs = Table(
    "runs",
    metadata,
    Column("run_id", String, primary_key=True),  # uuid
    Column("kind", String, nullable=False),
    Column("target_root", Text, nullable=True),
    Column("started_at", String, nullable=False),  # ISO8601
    Column("finished_at", String, nullable=True),
    Column("tool_version", String, nullable=True),
    Column("notes", Text, nullable=True),
    Index("idx_runs_kind_started", "kind", "started_at"),
)

files = Table(
    "files",
    metadata,
    Column("file_id", String, primary_key=True),  # uuid
    Column("size_bytes", Integer, nullable=False),
    Column("content_hash", String, nullable=True),
    Column("hash_algo", String, nullable=True),
    Column("created_at", String, nullable=False),
    Column("updated_at", String, nullable=False),
    Index("idx_files_hash", "content_hash"),
)

paths = Table(
    "paths",
    metadata,
    Column("path_id", String, primary_key=True),  # uuid
    Column("path", Text, nullable=False, unique=True),
    Column("drive", String(8), nullable=True),
    Column("dir", Text, nullable=True),
    Column("name", Text, nullable=True),
    Column("ext", String(32), nullable=True),
    Column("created_at", String, nullable=False),
    Column("updated_at", String, nullable=False),
    Index("idx_paths_dir", "dir"),
    Index("idx_paths_ext", "ext"),
)

file_paths = Table(
    "file_paths",
    metadata,
    Column("file_id", String, ForeignKey("files.file_id"), nullable=False),
    Column("path_id", String, ForeignKey("paths.path_id"), nullable=False),
    Column("is_current", Integer, nullable=False, server_default="1"),
    Column("first_seen_run_id", String, ForeignKey("runs.run_id"), nullable=True),
    Column("last_seen_run_id", String, ForeignKey("runs.run_id"), nullable=True),
    UniqueConstraint("file_id", "path_id", name="uq_file_paths_file_path"),
    Index("idx_file_paths_current", "is_current"),
)

observations = Table(
    "observations",
    metadata,
    Column("run_id", String, ForeignKey("runs.run_id"), nullable=False),
    Column("path_id", String, ForeignKey("paths.path_id"), nullable=False),
    Column("size_bytes", Integer, nullable=False),
    Column("mtime_utc", String, nullable=True),
    Column("type", String(32), nullable=True),
    Column("name_flags", Text, nullable=True),  # JSON text
    UniqueConstraint("run_id", "path_id", name="uq_observations_run_path"),
    Index("idx_obs_run", "run_id"),
)

events = Table(
    "events",
    metadata,
    Column("event_id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String, ForeignKey("runs.run_id"), nullable=False),
    Column("ts", String, nullable=False),
    Column("kind", String, nullable=False),
    Column("src_path_id", String, ForeignKey("paths.path_id"), nullable=True),
    Column("dst_path_id", String, ForeignKey("paths.path_id"), nullable=True),
    Column("detail_json", Text, nullable=True),
    Column("ok", Integer, nullable=False, server_default="1"),
    Column("error", Text, nullable=True),
    Index("idx_events_run", "run_id"),
    Index("idx_events_kind_ts", "kind", "ts"),
)

tags = Table(
    "tags",
    metadata,
    Column("tag_id", Integer, primary_key=True, autoincrement=True),
    Column("name", Text, nullable=False),
    Column("namespace", Text, nullable=False, server_default="'tablacus'"),
    UniqueConstraint("namespace", "name", name="uq_tags_namespace_name"),
)

path_tags = Table(
    "path_tags",
    metadata,
    Column("path_id", String, ForeignKey("paths.path_id"), nullable=False),
    Column("tag_id", Integer, ForeignKey("tags.tag_id"), nullable=False),
    Column("source", Text, nullable=False, server_default="'tablacus'"),
    Column("updated_at", String, nullable=False),
    UniqueConstraint("path_id", "tag_id", "source", name="uq_path_tags_triplet"),
    Index("idx_path_tags_tag", "tag_id"),
)

path_metadata = Table(
    "path_metadata",
    metadata,
    Column("path_id", String, ForeignKey("paths.path_id"), primary_key=True),
    Column("source", Text, nullable=False),
    Column("data_json", Text, nullable=False),
    Column("updated_at", String, nullable=False),
)
