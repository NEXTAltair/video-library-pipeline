"""Shared utilities for EPG program/broadcast ID generation.

Used by ingest_program_txt.py and migrate_epg_to_programs.py.
"""

from __future__ import annotations

import re
import unicodedata
import uuid

WS = re.compile(r"[\s\u3000]+")
BAD = re.compile(r'[<>:"/\\|?*]')
UND = re.compile(r"_+")
SUB_WS = re.compile(r"\s+")


def normalize_program_key(title: str) -> str:
    """Normalize title to a stable program_key (NFKC + lowercase + sanitize)."""
    t = unicodedata.normalize("NFKC", str(title or "")).strip().lower()
    t = WS.sub("_", t)
    t = BAD.sub("", t)
    t = UND.sub("_", t).strip("_")
    return t or "unknown"


def build_episode_group_key(
    program_title: str,
    episode_no: str | int | None,
    subtitle: str | None,
    *,
    air_date: str | None = None,
    include_air_date_fallback: bool = False,
) -> str | None:
    """Build a stable episode grouping key from title + episode/subtitle.

    Uses normalized program key derived on demand from program_title.
    """
    key = normalize_program_key(program_title)
    ep = str(episode_no or "").strip()
    if ep:
        return f"{key}::ep::{ep}"
    sub = SUB_WS.sub(" ", str(subtitle or "").strip().lower())
    if sub:
        return f"{key}::sub::{sub}"
    if include_air_date_fallback:
        air = str(air_date or "").strip()
        if air:
            return f"{key}::date::{air}"
    return None


def program_id_for(program_key: str) -> str:
    """Deterministic UUID5 for a program series."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"program_key:{program_key}"))


def broadcast_id_for(match_key: str, fallback_seed: str = "") -> str:
    """Deterministic UUID5 for a broadcast airing.

    Uses match_key when available (preferred). Falls back to a composite
    seed for legacy records that lack match_key.
    The fallback_seed should NOT include file paths — use only EPG metadata
    (air_date, start_time, broadcaster, title) to keep IDs stable.
    """
    if match_key:
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"broadcast_match_key:{match_key}"))
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"broadcast_fallback:{fallback_seed}"))
