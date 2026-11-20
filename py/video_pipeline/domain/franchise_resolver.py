from __future__ import annotations

import re
import unicodedata
from pathlib import Path, PureWindowsPath
from typing import Any

try:
    import yaml  # type: ignore
except Exception:
    yaml = None

DEFAULT_RULES_PATH = Path(__file__).resolve().parents[3] / "rules" / "franchise_rules.yaml"

# Evidence kinds a franchise rule may require before it groups a recording.
_GENRE_EVIDENCE = {
    "movie": {"movie", "映画"},
    "tokusatsu": {"tokusatsu", "特撮"},
}
_EPG_EVIDENCE = {
    "movie": lambda cat, sub: cat == "映画",
    "tokusatsu": lambda cat, sub: cat == "アニメ/特撮" and sub == "特撮",
}
# Broadcaster movie marker in EPG-derived recording names, e.g. "[映]" / "[2K][映]".
_MOVIE_NAME_MARKER = re.compile(r"\[映\]")


def _load_rules(path: str | Path | None) -> list[dict[str, Any]]:
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


def _epg_pairs(md: dict[str, Any]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    epg = md.get("epg_genres")
    if isinstance(epg, list):
        for entry in epg:
            if isinstance(entry, dict):
                pairs.append((str(entry.get("category") or ""), str(entry.get("subcategory") or "")))
            elif isinstance(entry, str):
                cat, _, sub = entry.partition(" - ")
                pairs.append((cat.strip(), sub.strip()))
    single = str(md.get("epg_genre") or "")
    if single:
        cat, _, sub = single.partition(" - ")
        pairs.append((cat.strip(), sub.strip()))
    return pairs


def _recording_names(md: dict[str, Any]) -> list[str]:
    names = [PureWindowsPath(str(md.get("path") or "")).name]
    evidence = md.get("evidence")
    if isinstance(evidence, dict):
        names.append(str(evidence.get("raw") or ""))
    return [unicodedata.normalize("NFKC", n) for n in names if n]


def has_genre_evidence(md: dict[str, Any], kinds: list[str]) -> bool:
    """True when metadata proves one of ``kinds`` (movie / tokusatsu).

    An explicit non-matching genre is decisive: a variety show that mentions
    ゴジラ stays outside the franchise even if its name looks like a film.
    """
    kinds = [k for k in kinds if k in _GENRE_EVIDENCE]
    if not kinds:
        return True
    genre = str(md.get("genre") or "").strip().casefold()
    if genre:
        return any(genre in _GENRE_EVIDENCE[k] for k in kinds)
    pairs = _epg_pairs(md)
    if pairs:
        return any(_EPG_EVIDENCE[k](cat, sub) for k in kinds for cat, sub in pairs)
    if "movie" in kinds:
        return any(_MOVIE_NAME_MARKER.search(n) for n in _recording_names(md))
    return False


def _required_evidence(rule: dict[str, Any]) -> list[str]:
    req = rule.get("require_evidence")
    if isinstance(req, str):
        return [req]
    if isinstance(req, list):
        return [str(r) for r in req if isinstance(r, str)]
    return []


def resolve_franchise(md: dict[str, Any], rules_path: str | None = None) -> str | None:
    title = str(md.get("program_title") or "")
    path = str(md.get("path") or "")

    rules = _load_rules(rules_path)
    for rule in rules:
        franchise = rule.get("franchise")
        patterns = rule.get("title_patterns")
        if not isinstance(franchise, str) or not isinstance(patterns, list):
            continue
        for p in patterns:
            if isinstance(p, str) and p and p in title:
                if not has_genre_evidence(md, _required_evidence(rule)):
                    # A mention inside a non-film title must not claim the series.
                    break
                return franchise

    if path:
        # Folder placement alone cannot prove membership for franchises whose
        # rule demands evidence; otherwise a misfiled show would keep itself
        # in the series forever.
        requirement_rules = rules or _load_rules(DEFAULT_RULES_PATH)
        requirements = {str(r.get("franchise")): _required_evidence(r) for r in requirement_rules
                        if isinstance(r.get("franchise"), str)}
        for seg in PureWindowsPath(path).parts:
            if "シリーズ_" in seg:
                if not has_genre_evidence(md, requirements.get(seg, [])):
                    return None
                return seg
    return None
