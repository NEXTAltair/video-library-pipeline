from __future__ import annotations

from pathlib import PureWindowsPath
from typing import Any

try:
    import yaml  # type: ignore
except Exception:
    yaml = None


def _load_rules(path: str | None) -> list[dict[str, Any]]:
    if not path or yaml is None:
        return []
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            obj = yaml.safe_load(f)
    except Exception:
        return []
    if not isinstance(obj, dict) or not isinstance(obj.get("rules"), list):
        return []
    return [r for r in obj["rules"] if isinstance(r, dict)]


def resolve_franchise(md: dict[str, Any], rules_path: str | None = None) -> str | None:
    title = str(md.get("program_title") or "")
    path = str(md.get("path") or "")

    for rule in _load_rules(rules_path):
        franchise = rule.get("franchise")
        patterns = rule.get("title_patterns")
        if not isinstance(franchise, str) or not isinstance(patterns, list):
            continue
        for p in patterns:
            if isinstance(p, str) and p and p in title:
                return franchise

    if path:
        for seg in PureWindowsPath(path).parts:
            if "シリーズ_" in seg:
                return seg
    return None
