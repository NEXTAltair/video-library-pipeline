"""Extract series-level names from broadcast titles for program grouping.

Used by schema v3 migration and ingest_program_txt to create series-level
program records instead of per-broadcast-title records.

Note: This module handles EPG-level title grouping (franchise_rules, aliases,
episode-number stripping) and does NOT require a DB connection.
For DB-backed canonical title resolution used by contamination detection and
cleanup workflows, see ``title_resolution.py``.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from epg_common import normalize_program_key, program_id_for
from path_placement_rules import SUBTITLE_SEPARATORS

try:
    import yaml  # type: ignore
except Exception:
    yaml = None

# Extends SUBTITLE_SEPARATORS with episode-number markers (#/＃) for broader
# title splitting.  Base characters stay in sync with path_placement_rules.
SUBTITLE_SPLIT_RE = re.compile(SUBTITLE_SEPARATORS.pattern.rstrip("]") + r"#＃]")
# Matches episode-number suffixes: " 第N話...", " #N...", " #N-N..."
EPISODE_SUFFIX_RE = re.compile(r"\s+(?:第\d+話|#\d+).*$")


def _load_franchise_rules(path: str | None) -> list[dict[str, Any]]:
    if not path or yaml is None:
        return []
    p = Path(path)
    if not p.exists():
        return []
    try:
        with p.open("r", encoding="utf-8-sig") as f:
            obj = yaml.safe_load(f)
    except Exception:
        return []
    if not isinstance(obj, dict) or not isinstance(obj.get("rules"), list):
        return []
    return [r for r in obj["rules"] if isinstance(r, dict)]


def _load_aliases_from_file(path: Path) -> dict[str, str]:
    """Load a single aliases YAML and return {normalized_alias: canonical_title}."""
    if yaml is None or not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8-sig") as f:
            obj = yaml.safe_load(f)
    except Exception:
        return {}
    if not isinstance(obj, dict):
        return {}

    alias_map: dict[str, str] = {}
    hints = obj.get("hints") or []
    if not isinstance(hints, list):
        return alias_map
    for item in hints:
        if not isinstance(item, dict):
            continue
        canonical = item.get("canonical_title")
        if not isinstance(canonical, str) or not canonical.strip():
            continue
        canonical = canonical.strip()
        aliases = item.get("aliases", [])
        if isinstance(aliases, list):
            for a in aliases:
                if isinstance(a, str) and a.strip():
                    alias_map[normalize_program_key(a)] = canonical
        alias_map[normalize_program_key(canonical)] = canonical
    return alias_map


def _load_aliases(path: str | None) -> dict[str, str]:
    """Load program aliases YAML(s) and return {normalized_alias: canonical_title}.

    If path points to a file, loads it. Also loads sibling files matching
    ``program_aliases_*.yaml`` in the same directory.
    The primary file takes precedence over supplementary files.
    """
    if not path or yaml is None:
        return {}
    p = Path(path)
    if not p.exists():
        return {}

    # Load supplementary files first (lower priority)
    alias_map: dict[str, str] = {}
    for sibling in sorted(p.parent.glob("program_aliases_*.yaml")):
        if sibling != p:
            alias_map.update(_load_aliases_from_file(sibling))

    # Load primary file last (highest priority, overwrites)
    alias_map.update(_load_aliases_from_file(p))
    return alias_map


def extract_series_name(
    title: str,
    franchise_rules_path: str | None = None,
    aliases_path: str | None = None,
    *,
    _franchise_rules: list[dict[str, Any]] | None = None,
    _alias_map: dict[str, str] | None = None,
) -> str:
    """Extract series name from a broadcast title.

    Priority:
    1. franchise_rules.yaml title_patterns match -> that series name
    2. program_aliases.yaml canonical_title match -> that title
    3. Fallback: split on subtitle separators and take the prefix
    """
    t = str(title or "").strip()
    if not t:
        return "UNKNOWN"

    # 1. Franchise rules patterns
    rules = _franchise_rules if _franchise_rules is not None else _load_franchise_rules(franchise_rules_path)
    # franchise rules don't give us a series name directly; they group by franchise.
    # For series name, we still want the title prefix, not the franchise name.

    # 2. Alias map lookup
    alias_map = _alias_map if _alias_map is not None else _load_aliases(aliases_path)
    if alias_map:
        key = normalize_program_key(t)
        if key in alias_map:
            return alias_map[key]
        # 2b. Strip episode suffix (第N話, #N) and retry alias lookup
        stripped = EPISODE_SUFFIX_RE.sub("", t).strip()
        if stripped and stripped != t:
            stripped_key = normalize_program_key(stripped)
            if stripped_key in alias_map:
                return alias_map[stripped_key]

    # 3. Fallback: split on subtitle separators, take prefix
    parts = SUBTITLE_SPLIT_RE.split(t, maxsplit=1)
    prefix = parts[0].strip()
    if not prefix:
        return t
    # 3b. Check if the split prefix matches an alias
    if alias_map:
        prefix_key = normalize_program_key(prefix)
        if prefix_key in alias_map:
            return alias_map[prefix_key]
    return prefix


def series_program_key(
    title: str,
    franchise_rules_path: str | None = None,
    aliases_path: str | None = None,
    *,
    _franchise_rules: list[dict[str, Any]] | None = None,
    _alias_map: dict[str, str] | None = None,
) -> str:
    """Return normalized program_key for the series-level name."""
    name = extract_series_name(
        title,
        franchise_rules_path,
        aliases_path,
        _franchise_rules=_franchise_rules,
        _alias_map=_alias_map,
    )
    return normalize_program_key(name)


def series_program_id(
    title: str,
    franchise_rules_path: str | None = None,
    aliases_path: str | None = None,
    *,
    _franchise_rules: list[dict[str, Any]] | None = None,
    _alias_map: dict[str, str] | None = None,
) -> str:
    """Return deterministic UUID5 program_id for the series-level name."""
    key = series_program_key(
        title,
        franchise_rules_path,
        aliases_path,
        _franchise_rules=_franchise_rules,
        _alias_map=_alias_map,
    )
    return program_id_for(key)
