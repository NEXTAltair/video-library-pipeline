from __future__ import annotations

from typing import Any

GENRE_MAP = {
    "アニメ": "アニメ",
    "特撮": "特撮",
    "映画": "映画",
    "ドラマ": "ドラマ",
    "ドキュメンタリー": "ドキュメンタリー・情報",
    "情報": "ドキュメンタリー・情報",
    "バラエティ": "バラエティ",
    "ニュース": "ニュース・報道",
    "報道": "ニュース・報道",
    "放送大学": "放送大学",
}


def _pick_from_text(s: str) -> str | None:
    t = str(s or "")
    for key, val in GENRE_MAP.items():
        if key in t:
            return val
    return None


def resolve_genre(md: dict[str, Any]) -> str | None:
    if md.get("genre"):
        return str(md.get("genre"))
    epg = md.get("epg_genres")
    if isinstance(epg, list):
        for item in epg:
            if not isinstance(item, dict):
                continue
            got = _pick_from_text(str(item.get("subcategory") or "")) or _pick_from_text(str(item.get("category") or ""))
            if got:
                return got
    return _pick_from_text(str(md.get("program_title") or ""))
