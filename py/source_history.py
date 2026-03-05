from __future__ import annotations

from typing import Any

from pathscan_common import now_iso


def make_entry(source: str, fields: list[str], at: str | None = None) -> dict[str, Any]:
    uniq_fields = sorted({str(f) for f in fields if str(f) and str(f) != "source_history"})
    return {
        "source": str(source or "unknown"),
        "at": at or now_iso(),
        "fields": uniq_fields,
    }


def merge_data(existing: dict[str, Any] | None, incoming: dict[str, Any] | None, source: str) -> dict[str, Any]:
    base = dict(existing) if isinstance(existing, dict) else {}
    newv = dict(incoming) if isinstance(incoming, dict) else {}

    changed_fields: list[str] = []
    for k, v in newv.items():
        if k == "source_history":
            continue
        if v is None:
            continue
        if base.get(k) != v:
            changed_fields.append(k)
        base[k] = v

    history: list[Any] = []
    if isinstance(existing, dict) and isinstance(existing.get("source_history"), list):
        history.extend(existing.get("source_history") or [])
    if isinstance(incoming, dict) and isinstance(incoming.get("source_history"), list):
        history.extend(incoming.get("source_history") or [])

    if changed_fields:
        history.append(make_entry(source=source, fields=changed_fields))

    base["source_history"] = history
    return base
