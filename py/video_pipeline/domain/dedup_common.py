"""Shared helpers for dedup workflows.

Functions extracted from dedup_recordings so they can be reused by
broadcaster-assign review, drop-review, and dedup_rebroadcasts.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Text normalisation
# ---------------------------------------------------------------------------

def normalize_subtitle(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip().lower())


def parse_confidence(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def parse_resolution_score(v: Any) -> int:
    if isinstance(v, (int, float)):
        return int(v)
    if not isinstance(v, str):
        return 0
    m = re.search(r"(\d+)\s*[xX]\s*(\d+)", v)
    if not m:
        return 0
    try:
        return int(m.group(1)) * int(m.group(2))
    except Exception:
        return 0


def safe_group_key(s: str) -> str:
    x = re.sub(r"[^0-9A-Za-z._-]+", "_", s)
    x = re.sub(r"_+", "_", x).strip("._-")
    return x[:120] if x else "group"


# ---------------------------------------------------------------------------
# Broadcast bucket classification
# ---------------------------------------------------------------------------

def parse_simple_yaml_lists(path: Path) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    if not path.exists():
        return out
    cur: str | None = None
    for i, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.rstrip()
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        m_key = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*:\s*$", line)
        if m_key:
            cur = m_key.group(1)
            out.setdefault(cur, [])
            continue
        m_item = re.match(r"^\s*-\s*(.+?)\s*$", line)
        if m_item and cur:
            v = m_item.group(1).strip()
            if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
                v = v[1:-1]
            out[cur].append(v)
            continue
        m_scalar = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*:\s*(.+?)\s*$", line)
        if m_scalar:
            out.setdefault(m_scalar.group(1), [])
            cur = None
            continue
        raise SystemExit(f"invalid bucket yaml at {path}:{i}: {line}")
    return out


def load_bucket_rules(path: Path) -> dict[str, list[str]]:
    data = parse_simple_yaml_lists(path)
    terrestrial = [str(x).strip() for x in data.get("terrestrial_keywords", []) if str(x).strip()]
    bs_cs = [str(x).strip() for x in data.get("bs_cs_keywords", []) if str(x).strip()]
    return {"terrestrial": terrestrial, "bs_cs": bs_cs}


def classify_broadcast_bucket(row: dict[str, Any], rules: dict[str, list[str]]) -> tuple[str, str]:
    explicit = str(row.get("broadcast_bucket") or "").strip().lower()
    if explicit in {"terrestrial", "bs_cs"}:
        return explicit, "explicit_field"

    sources = [
        str(row.get("broadcaster") or ""),
        str(row.get("channel") or ""),
        str(row.get("path") or ""),
        str((row.get("evidence") or {}).get("raw") if isinstance(row.get("evidence"), dict) else ""),
    ]
    merged = " ".join([s for s in sources if s]).lower()
    merged_no_space = re.sub(r"\s+", "", merged)

    for kw in rules.get("terrestrial", []):
        k = kw.lower()
        if k and (k in merged or re.sub(r"\s+", "", k) in merged_no_space):
            return "terrestrial", f"keyword:{kw}"
    for kw in rules.get("bs_cs", []):
        k = kw.lower()
        if k and (k in merged or re.sub(r"\s+", "", k) in merged_no_space):
            return "bs_cs", f"keyword:{kw}"
    return "unknown", "no_match"


# ---------------------------------------------------------------------------
# Candidate / grouping / ranking
# ---------------------------------------------------------------------------

@dataclass
class Candidate:
    path_id: str
    path: str
    group_key: str
    confidence: float
    needs_review: bool
    program_title: str
    air_date: str | None
    episode_no: str | None
    subtitle: str | None
    bucket: str
    bucket_reason: str
    size_bytes: int
    mtime_ts: float
    resolution_score: int
    not_corrupt: int
    raw_meta: dict[str, Any]


def choose_keep(candidates: list[Candidate]) -> Candidate:
    # Higher score first:
    # 1) not_corrupt, 2) resolution, 3) file size, 4) mtime, 5) path asc
    ranked = sorted(
        candidates,
        key=lambda c: (-c.not_corrupt, -c.resolution_score, -c.size_bytes, -c.mtime_ts, c.path),
    )
    return ranked[0]


def build_group_key(md: dict[str, Any]) -> tuple[str | None, str | None]:
    from epg_common import normalize_program_key
    key = normalize_program_key(str(md.get("program_title") or ""))
    if not key or key == "unknown":
        return None, "missing_program_title"
    ep = md.get("episode_no")
    if ep is not None and str(ep).strip():
        return f"{key}::ep::{str(ep).strip()}", None
    sub = str(md.get("subtitle") or "").strip()
    if sub:
        return f"{key}::sub::{normalize_subtitle(sub)}", None
    return None, "missing_episode_and_subtitle"
