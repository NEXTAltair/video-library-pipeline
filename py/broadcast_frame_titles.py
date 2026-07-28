"""Known title contamination rules for program-title extraction."""

from __future__ import annotations

import re
import unicodedata

FRAME_TITLE_PREFIXES = (
    "BS11ガンダムアワー",
)

_WS = re.compile(r"[\s\u3000_]+")
_COMPACT = re.compile(r"[\s\u3000_]+")
_FRAME_PREFIX_RE = re.compile(
    r"^\s*(?:BS11|Bs11|ｂｓ１１|ＢＳ１１)ガンダムアワー(?:[\s\u3000_]+|$)",
    re.IGNORECASE,
)
_NEWSOON_RE = re.compile(
    r"^\s*午後\s*LIVE\s*ニュースーン(?:\s*午後?\s*[345]時台)?",
    re.IGNORECASE,
)
_TOKI_TV_RE = re.compile(r"^\s*時をかけるテレビ(?:[\s\u3000_]+|$)")
_OVERMAN_KING_RE = re.compile(r"^\s*OVERMAN\s+キングゲイナー(?:[\s\u3000_]+|$)", re.IGNORECASE)
_EPISODE_SUFFIX_TITLE_RE = re.compile(r"[#＃][0-9０-９]+(?:[.-][0-9０-９]+)?\s*$")
_KNOWN_PROGRAM_PREFIX_RULES: tuple[tuple[re.Pattern[str], str], ...] = (
    (_NEWSOON_RE, "午後LIVEニュースーン"),
    (_TOKI_TV_RE, "時をかけるテレビ"),
    (_OVERMAN_KING_RE, "OVERMAN キングゲイナー"),
)


def _norm(s: str) -> str:
    return _WS.sub(" ", unicodedata.normalize("NFKC", str(s or ""))).strip().casefold()


def _compact(s: str) -> str:
    return _COMPACT.sub("", unicodedata.normalize("NFKC", str(s or ""))).casefold()


def is_broadcast_frame_title(title: str) -> bool:
    """Return true when the whole title is a known broadcast-frame name."""
    n = _norm(title)
    return any(n == _norm(prefix) for prefix in FRAME_TITLE_PREFIXES)


def has_broadcast_frame_prefix(title: str) -> bool:
    """Return true when a title starts with a known broadcast-frame prefix."""
    return bool(_FRAME_PREFIX_RE.match(str(title or "")))


def is_broadcast_frame_contaminated_title(title: str) -> bool:
    """Return true for frame names and frame-prefixed episode-level titles."""
    return is_broadcast_frame_title(title) or has_broadcast_frame_prefix(title)


def strip_broadcast_frame_prefix(title: str) -> str:
    """Remove a known broadcast-frame prefix while preserving the remaining title."""
    return _FRAME_PREFIX_RE.sub("", str(title or ""), count=1).strip()


def canonical_from_known_program_prefix(title: str) -> str | None:
    """Return canonical program title when a known program prefix is found."""
    t = unicodedata.normalize("NFKC", str(title or ""))
    for pattern, canonical in _KNOWN_PROGRAM_PREFIX_RULES:
        if pattern.match(t):
            return canonical
    return None


def is_known_program_prefix_contaminated_title(title: str) -> bool:
    """Return true when a known program prefix swallowed a corner/subtitle."""
    canonical = canonical_from_known_program_prefix(title)
    if not canonical:
        return False
    return _compact(title) != _compact(canonical)


def is_episode_suffix_contaminated_title(title: str) -> bool:
    """Return true when a program title ends with an episode marker like #23."""
    return bool(_EPISODE_SUFFIX_TITLE_RE.search(str(title or "")))


def is_contaminated_program_title(title: str) -> bool:
    """Return true for titles that must be excluded from DB-backed dictionaries."""
    return (
        is_broadcast_frame_contaminated_title(title)
        or is_known_program_prefix_contaminated_title(title)
        or is_episode_suffix_contaminated_title(title)
    )
