"""Column separation helpers for v3 schema (promoted columns + data_json).

Provides split/reconstruct utilities so existing dict-based logic continues
to work while new columns are persisted to dedicated DB columns.
"""

from __future__ import annotations

import json
from typing import Any

# Keys promoted from path_metadata.data_json to dedicated columns
PROMOTED_PATH_METADATA_KEYS = {
    "program_title",
    "air_date",
    "needs_review",

    "episode_no",
    "subtitle",
    "broadcaster",
    "human_reviewed",
}

# Keys promoted from broadcasts.data_json to dedicated columns
PROMOTED_BROADCAST_KEYS = {
    "is_rebroadcast_flag",
    "epg_genres",
    "description",
    "official_title",
    "annotations",
}


def split_path_metadata(merged: dict[str, Any]) -> tuple[dict[str, Any], str]:
    """Split a merged metadata dict into promoted column values and residual data_json.

    Returns (promoted_columns, data_json_string).
    promoted_columns keys: program_title, air_date, needs_review,
      episode_no, subtitle, broadcaster, human_reviewed.
    """
    promoted: dict[str, Any] = {}
    residual: dict[str, Any] = {}
    for k, v in merged.items():
        if k in PROMOTED_PATH_METADATA_KEYS:
            promoted[k] = v
        else:
            residual[k] = v

    # Normalize types for DB columns
    promoted["needs_review"] = 1 if promoted.get("needs_review") else 0
    promoted["human_reviewed"] = 1 if promoted.get("human_reviewed") else 0

    return promoted, json.dumps(residual, ensure_ascii=False)


def reconstruct_path_metadata(row) -> dict[str, Any]:
    """Reconstruct a full metadata dict from a DB row with promoted columns + data_json.

    Works with sqlite3.Row or any dict-like object.
    """
    if isinstance(row, tuple) and not hasattr(row, 'keys'):
        # If it's a plain tuple, we can't reconstruct by name. Return empty.
        return {}

    try:
        data = json.loads(str(row["data_json"])) if row["data_json"] else {}
    except Exception:
        data = {}
    if not isinstance(data, dict):
        data = {}

    # Overlay promoted columns onto data dict
    for key in PROMOTED_PATH_METADATA_KEYS:
        try:
            if hasattr(row, 'keys'):
                val = row[key]
            elif hasattr(row, '__getitem__') and hasattr(row, 'keys'):
                 val = row[key]
            else:
                # If it's a tuple we need to know the index, but we can't do that generically here
                continue
        except (KeyError, IndexError, TypeError):
            continue
        if val is not None:
            if key == "needs_review":
                data[key] = bool(val)
            elif key == "human_reviewed":
                data[key] = bool(val)
            else:
                data[key] = val

    return data


def latest_path_metadata(con, path_id: str) -> tuple[dict[str, Any] | None, str | None]:
    """Get latest metadata for a path_id, returning (metadata_dict, source).

    Expects a connection with row_factory=sqlite3.Row (as returned by connect_db).
    """
    row = con.execute(
        """
        SELECT source, data_json, program_title, air_date, needs_review,
               episode_no, subtitle, broadcaster, human_reviewed
        FROM path_metadata
        WHERE path_id=?
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (path_id,),
    ).fetchone()
    if not row:
        return None, None
    md = reconstruct_path_metadata(row)
    return md if md else None, str(row["source"]) if row["source"] is not None else None


def latest_path_metadata_by_path(con, path: str) -> tuple[dict[str, Any] | None, str | None, str | None]:
    """Get latest metadata by resolving path->path_id first.

    Returns (metadata_dict, source, path_id).
    """
    path_row = con.execute("SELECT path_id FROM paths WHERE path=?", (path,)).fetchone()
    if not path_row:
        return None, None, None
    pid = str(path_row["path_id"])
    md, source = latest_path_metadata(con, pid)
    return md, source, pid


def split_broadcast_data(data: dict[str, Any]) -> tuple[dict[str, Any], str]:
    """Split broadcast data into promoted column values and residual data_json.

    Returns (promoted_columns, data_json_string).
    promoted_columns keys: is_rebroadcast_flag, epg_genres, description,
      official_title, annotations.
    """
    promoted: dict[str, Any] = {}
    residual: dict[str, Any] = {}
    for k, v in data.items():
        if k in PROMOTED_BROADCAST_KEYS:
            promoted[k] = v
        else:
            residual[k] = v

    # Serialize JSON array fields
    if "epg_genres" in promoted and not isinstance(promoted["epg_genres"], str):
        promoted["epg_genres"] = json.dumps(promoted["epg_genres"], ensure_ascii=False) if promoted["epg_genres"] is not None else None
    if "annotations" in promoted and not isinstance(promoted["annotations"], str):
        promoted["annotations"] = json.dumps(promoted["annotations"], ensure_ascii=False) if promoted["annotations"] is not None else None

    # is_rebroadcast_flag: bool -> int
    rbf = promoted.get("is_rebroadcast_flag")
    if isinstance(rbf, bool):
        promoted["is_rebroadcast_flag"] = 1 if rbf else 0
    elif rbf is not None:
        promoted["is_rebroadcast_flag"] = 1 if rbf else 0

    # description: truncate to 500 chars
    desc = promoted.get("description")
    if isinstance(desc, str) and len(desc) > 500:
        promoted["description"] = desc[:500]

    return promoted, json.dumps(residual, ensure_ascii=False)


def reconstruct_broadcast_data(row) -> dict[str, Any]:
    """Reconstruct a full broadcast data dict from a DB row with promoted columns + data_json."""
    try:
        data = json.loads(str(row["data_json"])) if row["data_json"] else {}
    except Exception:
        data = {}
    if not isinstance(data, dict):
        data = {}

    for key in PROMOTED_BROADCAST_KEYS:
        try:
            val = row[key]
        except (KeyError, IndexError):
            continue
        if val is not None:
            if key == "is_rebroadcast_flag":
                data[key] = bool(val)
            elif key in ("epg_genres", "annotations"):
                # Stored as JSON string in column
                if isinstance(val, str):
                    try:
                        data[key] = json.loads(val)
                    except Exception:
                        data[key] = val
                else:
                    data[key] = val
            else:
                data[key] = val

    return data
