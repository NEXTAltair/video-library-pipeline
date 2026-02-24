#!/usr/bin/env python3
r"""Detect and isolate duplicate recordings.

This script reads latest LLM metadata from DB, groups duplicate episode candidates,
and outputs dedup plan/apply artifacts.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PureWindowsPath
from typing import Any

from mediaops_schema import begin_immediate, connect_db, create_schema_if_needed, fetchall
from windows_pwsh_bridge import run_pwsh_json


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ts_compact(d: datetime | None = None) -> str:
    dt_obj = d or datetime.now()
    return dt_obj.strftime("%Y%m%d_%H%M%S")


def safe_json(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False)


def as_bool(v: Any, default: bool) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"1", "true", "yes", "on"}:
            return True
        if s in {"0", "false", "no", "off"}:
            return False
    return default


def canonicalize_windows_path(s: str) -> str:
    p = str(s or "").replace("/", "\\")
    if len(p) >= 2 and p[1] == ":":
        p = p[0].upper() + p[1:]
    return p


def wsl_to_windows_path(s: str) -> str:
    p = str(s or "")
    if p.startswith("/mnt/") and len(p) > 6 and p[6] == "/":
        drive = p[5].upper()
        rest = p.split(f"/mnt/{p[5]}/", 1)[1].replace("/", "\\")
        return f"{drive}:\\{rest}"
    return canonicalize_windows_path(p)


def windows_to_wsl_path(s: str) -> str:
    p = canonicalize_windows_path(s)
    m = re.match(r"^([A-Za-z]):(?:[\\](.*))?$", p)
    if not m:
        return p
    drive = m.group(1).lower()
    rest = (m.group(2) or "").replace("\\", "/")
    return f"/mnt/{drive}/{rest}" if rest else f"/mnt/{drive}"


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


def split_win(p: str) -> tuple[str | None, str | None, str | None, str | None]:
    wp = PureWindowsPath(p)
    drive = wp.drive[:-1] if wp.drive.endswith(":") else (wp.drive or None)
    name = wp.name or None
    ext = wp.suffix or None
    parent = str(wp.parent) if str(wp.parent) not in (".", "") else None
    return drive, parent, name, ext


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


def unique_dst_path(dst: Path) -> Path:
    if not dst.exists():
        return dst
    stem = dst.stem
    suf = dst.suffix
    parent = dst.parent
    for i in range(1, 10000):
        p = parent / f"{stem}__dup{i}{suf}"
        if not p.exists():
            return p
    raise RuntimeError(f"failed to resolve unique dst path: {dst}")


def iter_jsonl_file(path: Path):
    with path.open("r", encoding="utf-8-sig") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                rec = json.loads(s)
            except Exception:
                continue
            if isinstance(rec, dict):
                yield rec


def build_group_key(md: dict[str, Any]) -> tuple[str | None, str | None]:
    key = str(md.get("normalized_program_key") or "").strip()
    if not key:
        return None, "missing_normalized_program_key"
    ep = md.get("episode_no")
    if ep is not None and str(ep).strip():
        return f"{key}::ep::{str(ep).strip()}", None
    sub = str(md.get("subtitle") or "").strip()
    if sub:
        return f"{key}::sub::{normalize_subtitle(sub)}", None
    return None, "missing_episode_and_subtitle"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--windows-ops-root", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--max-groups", type=int, default=0)
    ap.add_argument("--confidence-threshold", type=float, default=0.85)
    ap.add_argument("--allow-needs-review", default="false")
    ap.add_argument("--keep-terrestrial-and-bscs", default="true")
    ap.add_argument("--bucket-rules-path", default="")
    args = ap.parse_args()

    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db}")

    ops_root = Path(args.windows_ops_root).resolve()
    move_dir = ops_root / "move"
    quarantine_root = ops_root / "duplicates" / "quarantine"
    move_dir.mkdir(parents=True, exist_ok=True)
    quarantine_root.mkdir(parents=True, exist_ok=True)

    bucket_rules_path = (
        Path(args.bucket_rules_path)
        if args.bucket_rules_path.strip()
        else Path(__file__).resolve().parent.parent / "rules" / "broadcast_buckets.yaml"
    )
    bucket_rules = load_bucket_rules(bucket_rules_path)

    allow_needs_review = as_bool(args.allow_needs_review, False)
    keep_terrestrial_and_bscs = as_bool(args.keep_terrestrial_and_bscs, True)
    max_groups = max(0, int(args.max_groups or 0))

    con = connect_db(args.db)
    create_schema_if_needed(con)

    ts = ts_compact()
    plan_path = move_dir / f"dedup_plan_{ts}.jsonl"
    apply_path = move_dir / f"dedup_apply_{ts}.jsonl"
    errors: list[str] = []

    try:
        md_rows = fetchall(
            con,
            """
            SELECT pm.path_id, pm.data_json, p.path
            FROM path_metadata pm
            JOIN paths p ON p.path_id = pm.path_id
            WHERE pm.source='llm'
            """,
            (),
        )

        grouped: dict[str, list[Candidate]] = {}
        dropped_for_missing_key = 0
        for r in md_rows:
            path_id = str(r["path_id"])
            path_val = canonicalize_windows_path(str(r["path"]))
            try:
                md = json.loads(str(r["data_json"]))
            except Exception:
                errors.append(f"invalid metadata json: path_id={path_id}")
                continue
            if not isinstance(md, dict):
                continue
            group_key, reason = build_group_key(md)
            if not group_key:
                dropped_for_missing_key += 1
                continue
            conf = parse_confidence(md.get("confidence"))
            needs_review = bool(md.get("needs_review"))
            bucket, bucket_reason = classify_broadcast_bucket(
                {"path": path_val, **md},
                bucket_rules,
            )
            path_wsl = Path(windows_to_wsl_path(path_val))
            size_bytes = 0
            mtime_ts = 0.0
            not_corrupt = 0
            try:
                st = path_wsl.stat()
                size_bytes = int(st.st_size)
                mtime_ts = float(st.st_mtime)
                not_corrupt = 1 if size_bytes > 0 else 0
            except Exception:
                not_corrupt = 0
            c = Candidate(
                path_id=path_id,
                path=path_val,
                group_key=group_key,
                confidence=conf,
                needs_review=needs_review,
                program_title=str(md.get("program_title") or ""),
                air_date=str(md.get("air_date")) if md.get("air_date") else None,
                episode_no=str(md.get("episode_no")) if md.get("episode_no") is not None else None,
                subtitle=str(md.get("subtitle")) if md.get("subtitle") is not None else None,
                bucket=bucket,
                bucket_reason=bucket_reason,
                size_bytes=size_bytes,
                mtime_ts=mtime_ts,
                resolution_score=parse_resolution_score(md.get("resolution")),
                not_corrupt=not_corrupt,
                raw_meta=md,
            )
            grouped.setdefault(group_key, []).append(c)

        group_keys = [k for k, arr in grouped.items() if len(arr) > 1]
        group_keys.sort()
        if max_groups > 0:
            group_keys = group_keys[:max_groups]

        rows_plan: list[dict[str, Any]] = []
        rows_drop: list[dict[str, Any]] = []
        groups_auto_processed = 0
        groups_manual_review = 0
        groups_split_by_broadcast = 0
        files_kept = 0
        files_kept_by_broadcast_policy = 0
        files_dropped = 0

        for gk in group_keys:
            arr = grouped[gk]
            auto_eligible = [x for x in arr if (allow_needs_review or not x.needs_review) and x.confidence >= args.confidence_threshold]
            if len(auto_eligible) < 2:
                groups_manual_review += 1
                for x in arr:
                    rows_plan.append(
                        {
                            "group_key": gk,
                            "path_id": x.path_id,
                            "path": x.path,
                            "bucket": x.bucket,
                            "decision": "manual_review_required",
                            "reason": "low_confidence_or_needs_review",
                            "confidence": x.confidence,
                            "needs_review": x.needs_review,
                            "bucket_reason": x.bucket_reason,
                            "ts": now_iso(),
                        }
                    )
                continue

            cohorts: list[tuple[str, list[Candidate]]] = []
            if keep_terrestrial_and_bscs:
                by_bucket: dict[str, list[Candidate]] = {"terrestrial": [], "bs_cs": [], "unknown": []}
                for x in auto_eligible:
                    by_bucket.setdefault(x.bucket, []).append(x)
                if by_bucket["unknown"] and (by_bucket["terrestrial"] or by_bucket["bs_cs"]):
                    groups_manual_review += 1
                    for x in arr:
                        rows_plan.append(
                            {
                                "group_key": gk,
                                "path_id": x.path_id,
                                "path": x.path,
                                "bucket": x.bucket,
                                "decision": "manual_review_required",
                                "reason": "unknown_bucket_mixed",
                                "confidence": x.confidence,
                                "needs_review": x.needs_review,
                                "bucket_reason": x.bucket_reason,
                                "ts": now_iso(),
                            }
                        )
                    continue
                if by_bucket["terrestrial"] and by_bucket["bs_cs"]:
                    groups_split_by_broadcast += 1
                    files_kept_by_broadcast_policy += 1
                for b in ("terrestrial", "bs_cs", "unknown"):
                    if by_bucket[b]:
                        cohorts.append((b, by_bucket[b]))
            else:
                cohorts.append(("all", auto_eligible))

            group_has_drop = False
            for bucket_name, cohort in cohorts:
                if len(cohort) == 1:
                    keep = cohort[0]
                    rows_plan.append(
                        {
                            "group_key": gk,
                            "path_id": keep.path_id,
                            "path": keep.path,
                            "bucket": keep.bucket,
                            "decision": "keep",
                            "reason": f"single_in_bucket:{bucket_name}",
                            "confidence": keep.confidence,
                            "needs_review": keep.needs_review,
                            "bucket_reason": keep.bucket_reason,
                            "ts": now_iso(),
                        }
                    )
                    files_kept += 1
                    continue

                keep = choose_keep(cohort)
                files_kept += 1
                rows_plan.append(
                    {
                        "group_key": gk,
                        "path_id": keep.path_id,
                        "path": keep.path,
                        "bucket": keep.bucket,
                        "decision": "keep",
                        "reason": f"best_ranked:{bucket_name}",
                        "confidence": keep.confidence,
                        "needs_review": keep.needs_review,
                        "bucket_reason": keep.bucket_reason,
                        "ts": now_iso(),
                    }
                )
                for x in cohort:
                    if x.path_id == keep.path_id:
                        continue
                    group_has_drop = True
                    files_dropped += 1
                    row = {
                        "group_key": gk,
                        "path_id": x.path_id,
                        "path": x.path,
                        "bucket": x.bucket,
                        "decision": "drop",
                        "reason": f"lower_rank_in_bucket:{bucket_name}",
                        "confidence": x.confidence,
                        "needs_review": x.needs_review,
                        "bucket_reason": x.bucket_reason,
                        "ts": now_iso(),
                    }
                    rows_plan.append(row)
                    rows_drop.append(row)

            if group_has_drop:
                groups_auto_processed += 1

        with plan_path.open("w", encoding="utf-8") as w:
            w.write(
                safe_json(
                    {
                        "_meta": {
                            "kind": "dedup_plan",
                            "generated_at": now_iso(),
                            "db": args.db,
                            "groups_total": len(group_keys),
                            "allow_needs_review": allow_needs_review,
                            "confidence_threshold": args.confidence_threshold,
                            "keep_terrestrial_and_bscs": keep_terrestrial_and_bscs,
                            "bucket_rules_path": str(bucket_rules_path),
                            "dropped_for_missing_key": dropped_for_missing_key,
                        }
                    }
                )
                + "\n"
            )
            for row in rows_plan:
                w.write(safe_json(row) + "\n")

        files_moved = 0
        run_id: str | None = None
        apply_rows: list[dict[str, Any]] = []
        move_backend = "wsl_shutil_move"
        if args.apply:
            move_backend = "pwsh7_apply_move_plan"
            scripts_root = ops_root / "scripts"
            apply_move_script = scripts_root / "apply_move_plan.ps1"
            if not apply_move_script.exists():
                errors.append(f"apply_move_plan.ps1 not found: {apply_move_script}")
            else:
                ops_root_win = wsl_to_windows_path(str(ops_root))
                quarantine_root_win = wsl_to_windows_path(str(quarantine_root))
                internal_move_plan = move_dir / f"dedup_move_plan_internal_{ts}.jsonl"
                drop_by_path_id = {str(r["path_id"]): r for r in rows_drop if r.get("path_id")}
                with internal_move_plan.open("w", encoding="utf-8") as w:
                    w.write(
                        safe_json(
                            {
                                "_meta": {
                                    "kind": "dedup_move_plan_internal",
                                    "generated_at": now_iso(),
                                    "source": "dedup_recordings.py",
                                    "rows": len(rows_drop),
                                }
                            }
                        )
                        + "\n"
                    )
                    for row in rows_drop:
                        src_win = canonicalize_windows_path(str(row["path"]))
                        group_dir_win = canonicalize_windows_path(
                            quarantine_root_win + "\\" + safe_group_key(str(row["group_key"]))
                        )
                        base_name = PureWindowsPath(src_win).name
                        dst_win = canonicalize_windows_path(group_dir_win + "\\" + base_name)
                        w.write(
                            safe_json(
                                {
                                    "op": "move",
                                    "path_id": row["path_id"],
                                    "src": src_win,
                                    "dst": dst_win,
                                }
                            )
                            + "\n"
                        )

                move_apply_file: Path | None = None
                try:
                    apply_meta = run_pwsh_json(
                        str(apply_move_script),
                        [
                            "-PlanJsonl",
                            wsl_to_windows_path(str(internal_move_plan)),
                            "-OpsRoot",
                            ops_root_win,
                            "-OnDstExists",
                            "rename_suffix",
                        ],
                    )
                    out_jsonl = str(apply_meta.get("out_jsonl") or "").strip()
                    if not out_jsonl:
                        raise RuntimeError("apply_move_plan.ps1 did not return out_jsonl")
                    move_apply_file = Path(windows_to_wsl_path(out_jsonl))
                    if not move_apply_file.exists():
                        raise RuntimeError(f"move apply JSONL not found: {move_apply_file}")
                except Exception as e:
                    errors.append(f"dedup apply move engine failed: {e}")

                if move_apply_file is not None:
                    run_id = str(uuid.uuid4())
                    try:
                        begin_immediate(con)
                        con.execute(
                            """
                            INSERT INTO runs (run_id, kind, target_root, started_at, finished_at, tool_version, notes)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                run_id,
                                "dedup",
                                str(quarantine_root),
                                now_iso(),
                                None,
                                "dedup_recordings.py",
                                f"groups={len(group_keys)} drops={len(rows_drop)}",
                            ),
                        )

                        for rec in iter_jsonl_file(move_apply_file):
                            if rec.get("op") != "move":
                                continue
                            pid = str(rec.get("path_id") or "")
                            src_win = canonicalize_windows_path(str(rec.get("src") or ""))
                            dst_win_val = canonicalize_windows_path(str(rec.get("dst") or ""))
                            ok = bool(rec.get("ok"))
                            err_text = None if ok else str(rec.get("error") or "")
                            src_row = drop_by_path_id.get(pid, {})
                            group_key = src_row.get("group_key")

                            if ok and pid and dst_win_val:
                                drive, dir_, name, ext = split_win(dst_win_val)
                                con.execute(
                                    "UPDATE paths SET path=?, drive=?, dir=?, name=?, ext=?, updated_at=? WHERE path_id=?",
                                    (dst_win_val, drive, dir_, name, ext, now_iso(), pid),
                                )
                                con.execute(
                                    """
                                    INSERT INTO events (run_id, ts, kind, src_path_id, dst_path_id, detail_json, ok, error)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                    """,
                                    (
                                        run_id,
                                        now_iso(),
                                        "dedup_move",
                                        pid,
                                        None,
                                        safe_json({"src": src_win, "dst": dst_win_val, "group_key": group_key}),
                                        1,
                                        None,
                                    ),
                                )
                                files_moved += 1
                            else:
                                if src_win or dst_win_val or pid:
                                    errors.append(
                                        f"move failed: {src_win or '(empty)'} -> {dst_win_val or '(empty)'} :: {err_text or 'unknown_error'}"
                                    )
                                if pid:
                                    con.execute(
                                        """
                                        INSERT INTO events (run_id, ts, kind, src_path_id, dst_path_id, detail_json, ok, error)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                        """,
                                        (
                                            run_id,
                                            now_iso(),
                                            "dedup_move",
                                            pid,
                                            None,
                                            safe_json({"src": src_win, "dst": dst_win_val, "group_key": group_key}),
                                            0,
                                            err_text or "move_failed",
                                        ),
                                    )

                            apply_rows.append(
                                {
                                    "group_key": group_key,
                                    "path_id": pid or None,
                                    "src": src_win,
                                    "dst": dst_win_val,
                                    "ok": ok,
                                    "error": err_text,
                                    "ts": str(rec.get("ts") or now_iso()),
                                }
                            )

                        con.execute("UPDATE runs SET finished_at=? WHERE run_id=?", (now_iso(), run_id))
                        con.commit()
                    except Exception:
                        con.rollback()
                        raise

            with apply_path.open("w", encoding="utf-8") as w:
                w.write(
                    safe_json(
                        {
                            "_meta": {
                                "kind": "dedup_apply",
                                "generated_at": now_iso(),
                                "run_id": run_id,
                                "rows": len(apply_rows),
                            }
                        }
                    )
                    + "\n"
                )
                for row in apply_rows:
                    w.write(safe_json(row) + "\n")

        summary = {
            "ok": len(errors) == 0,
            "tool": "video_pipeline_dedup_recordings",
            "apply": bool(args.apply),
            "planPath": str(plan_path),
            "applyPath": str(apply_path) if args.apply else None,
            "groupsTotal": len(group_keys),
            "groupsAutoProcessed": groups_auto_processed,
            "groupsManualReview": groups_manual_review,
            "groupsSplitByBroadcast": groups_split_by_broadcast,
            "filesKept": files_kept,
            "filesKeptByBroadcastPolicy": files_kept_by_broadcast_policy,
            "filesDropped": files_dropped,
            "filesMoved": files_moved,
            "moveBackend": move_backend,
            "errors": errors,
        }
        print(safe_json(summary))
        return 0 if summary["ok"] else 1
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
