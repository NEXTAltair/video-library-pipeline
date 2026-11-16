"""Pure, fail-closed physical cohort selection. No genre voting or metadata writes."""
from typing import Any


def partition_plan(rows: list[dict[str, Any]], selected: Any, held: Any) -> list[dict[str, Any]]:
    def ids(value: Any) -> set[str]:
        if (not isinstance(value, list) or not value
                or any(not isinstance(v, str) or not v.strip() or v != v.strip() for v in value)
                or len(set(value)) != len(value)):
            raise ValueError("selected/held path IDs must be explicit nonempty unique string arrays")
        return set(value)
    selected_set, held_set = ids(selected), ids(held)
    if selected_set & held_set:
        raise ValueError("held path ID cannot be selected")
    planned = [r for r in rows if "_meta" not in r and r.get("status") == "planned"]
    plan_ids = [r.get("path_id") for r in planned]
    if (any(not isinstance(v, str) or not v.strip() for v in plan_ids)
            or len(set(plan_ids)) != len(plan_ids)):
        raise ValueError("plan has missing or duplicate path IDs")
    if selected_set | held_set != set(plan_ids):
        raise ValueError("selection must partition every planned path ID; unknown/plan-outside IDs rejected")
    # A held row may share a companion with another recording. Such a partition
    # must not make that held member movable through its selected neighbour.
    paths: dict[str, str] = {}
    for row in planned:
        members = row.get("members") or [{"src": row.get("src"), "dst": row.get("dst")}]
        if not isinstance(members, list) or any(not isinstance(m, dict) for m in members):
            raise ValueError("invalid member cohort")
        for member in members:
            for key in ("src", "dst"):
                value = member.get(key)
                if not isinstance(value, str) or not value.strip():
                    raise ValueError("invalid member path")
                normalized = value.replace("/", "\\").casefold().rstrip("\\")
                owner = paths.setdefault(normalized, row["path_id"])
                if owner != row["path_id"]:
                    raise ValueError("plan rows share a physical member path")
    chosen = []
    for row in planned:
        if row["path_id"] not in selected_set:
            continue
        if any(row.get(k) for k in ("hold", "held", "hold_reason", "quarantined")):
            raise ValueError("held candidate cannot be selected")
        if not all(isinstance(row.get(k), str) and row[k].strip() for k in ("src", "dst")):
            raise ValueError("selected path identity missing")
        # Preserve complete immutable bundle/companion data and metadata fields verbatim.
        chosen.append(row)
    return chosen
