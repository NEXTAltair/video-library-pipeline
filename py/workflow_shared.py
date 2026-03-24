#!/usr/bin/env python3
"""Shared workflow-state helpers used across review/apply/move stages."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from db_helpers import reconstruct_path_metadata
from path_placement_rules import normalize_title_for_comparison


@dataclass(frozen=True)
class EffectiveMetadata:
    metadata: dict
    source: str | None
    selected_from: str


@dataclass(frozen=True)
class CanonicalTitleSources:
    human_reviewed_titles: set[str]
    programs_titles: set[str]


def _latest_path_metadata_row(
    con: sqlite3.Connection,
    path_id: str,
    *,
    human_reviewed_only: bool = False,
):
    where = "WHERE path_id=?"
    if human_reviewed_only:
        where += " AND (source='human_reviewed' OR human_reviewed=1)"
    return con.execute(
        f"""
        SELECT source, data_json, program_title, air_date, needs_review,
               episode_no, subtitle, broadcaster, human_reviewed
        FROM path_metadata
        {where}
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (path_id,),
    ).fetchone()


def resolve_effective_path_metadata(con: sqlite3.Connection, path_id: str) -> EffectiveMetadata | None:
    """Resolve metadata with shared precedence.

    Precedence:
      1. Most recent human_reviewed metadata (authoritative when present)
      2. Otherwise, most recent metadata row
    """
    human_row = _latest_path_metadata_row(con, path_id, human_reviewed_only=True)
    if human_row:
        md = reconstruct_path_metadata(human_row)
        return EffectiveMetadata(
            metadata=md if isinstance(md, dict) else {},
            source=str(human_row["source"]) if human_row["source"] is not None else None,
            selected_from="latest_human_reviewed",
        )

    latest_row = _latest_path_metadata_row(con, path_id, human_reviewed_only=False)
    if not latest_row:
        return None
    md = reconstruct_path_metadata(latest_row)
    return EffectiveMetadata(
        metadata=md if isinstance(md, dict) else {},
        source=str(latest_row["source"]) if latest_row["source"] is not None else None,
        selected_from="latest_any",
    )


def metadata_source_flags(md: dict | None, source: str | None) -> tuple[bool, bool]:
    source_norm = str(source or "").strip().lower()
    is_human_reviewed = (
        source_norm == "human_reviewed"
        or bool(isinstance(md, dict) and md.get("human_reviewed"))
    )
    is_llm = source_norm in {"llm", "llm_subagent"}
    return is_human_reviewed, is_llm


def load_canonical_title_sources(con: sqlite3.Connection) -> CanonicalTitleSources:
    human_reviewed_titles: set[str] = set()
    programs_titles: set[str] = set()

    try:
        rows = con.execute(
            """SELECT DISTINCT program_title FROM path_metadata
               WHERE program_title IS NOT NULL AND program_title != ''
                 AND (source = 'human_reviewed' OR human_reviewed = 1)"""
        ).fetchall()
        for r in rows:
            title = str(r["program_title"]).strip()
            if title:
                human_reviewed_titles.add(title)
    except sqlite3.OperationalError:
        pass

    try:
        rows = con.execute(
            "SELECT canonical_title FROM programs WHERE canonical_title IS NOT NULL AND canonical_title != ''"
        ).fetchall()
        for r in rows:
            title = str(r["canonical_title"]).strip()
            if title:
                programs_titles.add(title)
    except sqlite3.OperationalError:
        pass

    return CanonicalTitleSources(
        human_reviewed_titles=human_reviewed_titles,
        programs_titles=programs_titles,
    )


def longest_prefix_title_match(
    program_title: str,
    canonical_titles: set[str],
    min_extra_chars: int,
) -> str | None:
    pt_norm = normalize_title_for_comparison(program_title)
    if not pt_norm:
        return None

    best_title: str | None = None
    best_len = 0
    for ct in canonical_titles:
        ct_norm = normalize_title_for_comparison(ct)
        if not ct_norm:
            continue
        if pt_norm.startswith(ct_norm) and len(pt_norm) >= len(ct_norm) + min_extra_chars and len(ct_norm) > best_len:
            best_len = len(ct_norm)
            best_title = ct
    return best_title
