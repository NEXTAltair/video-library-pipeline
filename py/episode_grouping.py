"""Helpers for episode-level grouping keys.

Issue #53 で `normalized_program_key` 依存を減らすため、
program identity はまず programs/program_id を優先し、
不足時のみ program_title 正規化にフォールバックする。
"""

from __future__ import annotations

import re
from typing import Any


def normalize_subtitle(subtitle: Any) -> str:
    return re.sub(r"\s+", " ", str(subtitle or "").strip().lower())


def normalize_program_title(title: Any) -> str:
    x = str(title or "").strip().lower()
    if not x:
        return ""
    x = re.sub(r"\s+", " ", x)
    return x


def build_episode_group_key(
    md: dict[str, Any],
    *,
    linked_program_id: str | None,
    allow_air_date_fallback: bool,
) -> tuple[str | None, str | None]:
    anchor = str(linked_program_id or "").strip()
    if not anchor:
        title_key = normalize_program_title(md.get("program_title"))
        if not title_key:
            return None, "missing_program_id_and_program_title"
        anchor = f"title::{title_key}"
    else:
        anchor = f"pid::{anchor}"

    ep = md.get("episode_no")
    if ep is not None and str(ep).strip():
        return f"{anchor}::ep::{str(ep).strip()}", None

    sub_key = normalize_subtitle(md.get("subtitle"))
    if sub_key:
        return f"{anchor}::sub::{sub_key}", None

    if allow_air_date_fallback:
        air = str(md.get("air_date") or "").strip()
        if air:
            return f"{anchor}::date::{air}", None

    return None, "missing_episode_and_subtitle"

