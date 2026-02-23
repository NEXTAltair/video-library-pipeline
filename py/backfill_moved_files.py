#!/usr/bin/env python3
r"""Backfill moved files into mediaops.sqlite.

This script scans configured roots, compares against DB paths/observations,
and writes backfill plan/apply artifacts under windowsOpsRoot/move.
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

from mediaops_schema import begin_immediate, connect_db, create_schema_if_needed, fetchall, fetchone

DB_CONTRACT_REQUIRED = {"program_title", "air_date", "needs_review"}
PATH_NAMESPACE = uuid.UUID("f4f67a6f-90c6-4ee4-9c1a-2c0d25b3b0c4")
DEFAULT_EXTENSIONS = [".mp4"]
DEFAULT_SCAN_RETRY_COUNT = 1
MAX_SUMMARY_WARNINGS = 200


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ts_compact(d: datetime | None = None) -> str:
    dt_obj = d or datetime.now()
    return dt_obj.strftime("%Y%m%d_%H%M%S")


def normalize_win_for_id(p: str) -> str:
    return p.replace("/", "\\").lower()


def path_id_for(p: str) -> str:
    return str(uuid.uuid5(PATH_NAMESPACE, "winpath:" + normalize_win_for_id(p)))


def split_win(p: str) -> tuple[str | None, str | None, str | None, str | None]:
    wp = PureWindowsPath(p)
    drive = wp.drive[:-1] if wp.drive.endswith(":") else (wp.drive or None)
    name = wp.name or None
    ext = wp.suffix or None
    parent = str(wp.parent) if str(wp.parent) not in (".", "") else None
    return drive, parent, name, ext


def canonicalize_windows_path(s: str) -> str:
    p = s.replace("/", "\\")
    if len(p) >= 2 and p[1] == ":":
        p = p[0].upper() + p[1:]
    return p


def windows_to_wsl_path(s: str) -> str:
    m = re.match(r"^([A-Za-z]):(?:[\\/](.*))?$", s)
    if not m:
        return s
    drive = m.group(1).lower()
    rest = (m.group(2) or "").replace("\\", "/")
    return f"/mnt/{drive}/{rest}" if rest else f"/mnt/{drive}"


def wsl_to_windows_path(s: str) -> str:
    if s.startswith("/mnt/") and len(s) > 6 and s[6] == "/":
        drive = s[5].upper()
        rest = s.split(f"/mnt/{s[5]}/", 1)[1].replace("/", "\\")
        return f"{drive}:\\{rest}"
    return canonicalize_windows_path(s)


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


def parse_json_arg(v: str | None, fallback: Any) -> Any:
    if v is None:
        return fallback
    s = v.strip()
    if not s:
        return fallback
    try:
        return json.loads(s)
    except Exception as e:
        raise SystemExit(f"invalid JSON arg: {e}")


def strip_quotes(s: str) -> str:
    t = s.strip()
    if len(t) >= 2 and ((t[0] == t[-1] == '"') or (t[0] == t[-1] == "'")):
        return t[1:-1]
    return t


def parse_simple_yaml_lists(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(str(path))
    data: dict[str, Any] = {}
    current_list_key: str | None = None
    lines = path.read_text(encoding="utf-8").splitlines()
    for i, raw in enumerate(lines, start=1):
        line = raw.rstrip()
        if not line.strip():
            continue
        if line.lstrip().startswith("#"):
            continue
        m_key_list = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*:\s*$", line)
        if m_key_list:
            key = m_key_list.group(1)
            current_list_key = key
            if key not in data:
                data[key] = []
            if not isinstance(data[key], list):
                raise SystemExit(f"invalid YAML at {path}:{i}: key '{key}' used as both scalar and list")
            continue
        m_key_value = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*:\s*(.+?)\s*$", line)
        if m_key_value:
            key = m_key_value.group(1)
            value = strip_quotes(m_key_value.group(2))
            if value.isdigit():
                data[key] = int(value)
            else:
                data[key] = value
            current_list_key = None
            continue
        m_list_item = re.match(r"^\s*-\s*(.+?)\s*$", line)
        if m_list_item and current_list_key:
            value = strip_quotes(m_list_item.group(1))
            lst = data.get(current_list_key)
            if not isinstance(lst, list):
                raise SystemExit(f"invalid YAML at {path}:{i}: list item outside list key")
            lst.append(value)
            continue
        raise SystemExit(f"invalid YAML syntax at {path}:{i}: {line}")
    return data


def ensure_exts(exts: list[str] | None) -> list[str]:
    vals = exts or DEFAULT_EXTENSIONS
    out: list[str] = []
    for e in vals:
        s = str(e or "").strip().lower()
        if not s:
            continue
        if not s.startswith("."):
            s = "." + s
        out.append(s)
    return sorted(set(out)) or DEFAULT_EXTENSIONS


@dataclass
class ScannedFile:
    wsl_path: str
    win_path: str
    drive: str | None
    dir: str | None
    name: str | None
    ext: str | None
    size: int
    mtime_utc: str | None
    corrupt_candidate: bool
    corrupt_reason: str | None


def read_head_ok(path: Path, read_bytes: int) -> tuple[bool, str | None]:
    try:
        with path.open("rb") as f:
            _ = f.read(max(1, read_bytes))
        return True, None
    except Exception as e:
        return False, str(e)


def collect_scanned_file(path: Path, exts: set[str], detect_corruption: bool, read_bytes: int, warnings: list[str]) -> ScannedFile | None:
    ext = path.suffix.lower()
    if ext not in exts:
        return None
    try:
        st = path.stat()
    except Exception as e:
        warnings.append(f"stat failed: {path} :: {e}")
        return None

    mtime = datetime.fromtimestamp(st.st_mtime, timezone.utc).isoformat()
    win_path = canonicalize_windows_path(wsl_to_windows_path(str(path)))
    drive, dir_, name, ext2 = split_win(win_path)
    corrupt = False
    reason = None
    if detect_corruption:
        if int(st.st_size) == 0:
            corrupt = True
            reason = "size_zero"
        else:
            ok, err = read_head_ok(path, read_bytes)
            if not ok:
                corrupt = True
                reason = f"read_failed:{err}"
    return ScannedFile(
        wsl_path=str(path),
        win_path=win_path,
        drive=drive,
        dir=dir_,
        name=name,
        ext=ext2,
        size=int(st.st_size),
        mtime_utc=mtime,
        corrupt_candidate=corrupt,
        corrupt_reason=reason,
    )


def scan_files(
    roots: list[str],
    exts: set[str],
    detect_corruption: bool,
    read_bytes: int,
    scan_retry_count: int,
) -> tuple[list[ScannedFile], list[str], list[str]]:
    files: list[ScannedFile] = []
    warnings: list[str] = []
    errors: list[str] = []
    seen: set[str] = set()
    for root_raw in roots:
        root = Path(root_raw)
        if not root.exists():
            errors.append(f"root missing: {root_raw}")
            continue
        if not root.is_dir():
            errors.append(f"root is not a directory: {root_raw}")
            continue

        failed_dirs: set[str] = set()

        def _collect_file(path: Path) -> None:
            sp = str(path)
            if sp in seen:
                return
            row = collect_scanned_file(path, exts, detect_corruption, read_bytes, warnings)
            if row is None:
                return
            seen.add(sp)
            files.append(row)

        def _on_walk_error(e: OSError) -> None:
            failed_path = str(getattr(e, "filename", "") or "")
            if failed_path:
                failed_dirs.add(failed_path)
            warnings.append(f"walk failed: root={root_raw} path={failed_path or '?'} :: {e}")

        for dirpath, _dirnames, filenames in os.walk(str(root), onerror=_on_walk_error):
            for fname in filenames:
                _collect_file(Path(dirpath) / fname)

        unresolved = set(failed_dirs)
        retries = max(0, int(scan_retry_count))
        for attempt in range(1, retries + 1):
            if not unresolved:
                break
            attempt_failed: set[str] = set()
            attempt_targets = sorted(unresolved)
            unresolved = set()
            for target in attempt_targets:
                target_path = Path(target)
                if not target_path.exists() or not target_path.is_dir():
                    warnings.append(f"walk retry skipped: root={root_raw} path={target} reason=not_directory")
                    continue

                def _on_retry_error(e: OSError) -> None:
                    retry_failed_path = str(getattr(e, "filename", "") or target)
                    attempt_failed.add(retry_failed_path)
                    warnings.append(
                        f"walk retry failed: root={root_raw} path={retry_failed_path} attempt={attempt}/{retries} :: {e}"
                    )

                for dirpath, _dirnames, filenames in os.walk(str(target_path), onerror=_on_retry_error):
                    for fname in filenames:
                        _collect_file(Path(dirpath) / fname)
            unresolved = attempt_failed

        if unresolved:
            warnings.append(f"walk unresolved: root={root_raw} dirs={len(unresolved)}")

    return files, warnings, errors


def normalize_drive_key(d: str) -> str:
    x = d.strip().upper()
    if x.endswith(":"):
        return x
    if len(x) == 1 and x.isalpha():
        return x + ":"
    return x


def build_drive_map(obj: dict[str, str] | None) -> dict[str, str]:
    if not obj:
        return {}
    out: dict[str, str] = {}
    for k, v in obj.items():
        kk = normalize_drive_key(str(k))
        vv = normalize_drive_key(str(v))
        if re.match(r"^[A-Z]:$", kk) and re.match(r"^[A-Z]:$", vv):
            out[kk] = vv
    return out


def safe_json(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False)


def metadata_row_needs_queue(data_json: str | None) -> bool:
    if not data_json:
        return True
    try:
        md = json.loads(data_json)
        if not isinstance(md, dict):
            return True
    except Exception:
        return True
    missing = [k for k in DB_CONTRACT_REQUIRED if k not in md]
    if missing:
        return True
    if not isinstance(md.get("needs_review"), bool):
        return True
    if md.get("needs_review") is True:
        return True
    if not md.get("program_title"):
        return True
    if md.get("air_date") is None:
        return True
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--windows-ops-root", required=True)
    ap.add_argument("--dest-root", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--roots-json", default="")
    ap.add_argument("--roots-file-path", default="")
    ap.add_argument("--extensions-json", default="")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--include-observations", default="true")
    ap.add_argument("--queue-missing-metadata", default="false")
    ap.add_argument("--drive-map-json", default="")
    ap.add_argument("--detect-corruption", default="true")
    ap.add_argument("--corruption-read-bytes", type=int, default=4096)
    ap.add_argument("--scan-error-policy", choices=["warn", "fail", "threshold"], default="warn")
    ap.add_argument("--scan-error-threshold", type=int, default=0)
    ap.add_argument("--scan-retry-count", type=int, default=DEFAULT_SCAN_RETRY_COUNT)
    args = ap.parse_args()

    if not os.path.exists(args.db):
        raise SystemExit(f"DB not found: {args.db}")

    ops_root = Path(args.windows_ops_root).resolve()
    move_dir = ops_root / "move"
    llm_dir = ops_root / "llm"
    move_dir.mkdir(parents=True, exist_ok=True)
    llm_dir.mkdir(parents=True, exist_ok=True)

    roots_from_param = parse_json_arg(args.roots_json, [])
    if roots_from_param and not isinstance(roots_from_param, list):
        raise SystemExit("roots must be JSON array")
    roots: list[str] = []
    if isinstance(roots_from_param, list) and roots_from_param:
        roots = [windows_to_wsl_path(str(x)) for x in roots_from_param if str(x).strip()]
    else:
        roots_file_path = args.roots_file_path.strip() or str((Path(__file__).resolve().parent.parent / "rules" / "backfill_roots.yaml"))
        try:
            parsed = parse_simple_yaml_lists(Path(roots_file_path))
            roots_yaml = parsed.get("roots", [])
            if isinstance(roots_yaml, list):
                roots = [windows_to_wsl_path(str(x)) for x in roots_yaml if str(x).strip()]
            if not args.extensions_json.strip():
                yaml_ext = parsed.get("extensions", [])
                if isinstance(yaml_ext, list):
                    args.extensions_json = safe_json(yaml_ext)
        except FileNotFoundError:
            if not args.roots_file_path.strip():
                roots = []
            else:
                raise SystemExit(f"rootsFilePath not found: {roots_file_path}")

    if not roots:
        roots = [windows_to_wsl_path(args.dest_root)]

    exts_raw = parse_json_arg(args.extensions_json, DEFAULT_EXTENSIONS)
    if exts_raw and not isinstance(exts_raw, list):
        raise SystemExit("extensions must be JSON array")
    extensions = ensure_exts(exts_raw if isinstance(exts_raw, list) else DEFAULT_EXTENSIONS)
    include_observations = as_bool(args.include_observations, True)
    queue_missing_metadata = as_bool(args.queue_missing_metadata, False)
    detect_corruption = as_bool(args.detect_corruption, True)
    read_bytes = max(1, int(args.corruption_read_bytes or 4096))
    limit = max(0, int(args.limit or 0))
    scan_retry_count = max(0, int(args.scan_retry_count or DEFAULT_SCAN_RETRY_COUNT))
    drive_map_obj = parse_json_arg(args.drive_map_json, {})
    if drive_map_obj and not isinstance(drive_map_obj, dict):
        raise SystemExit("driveMap must be JSON object")
    drive_map = build_drive_map({str(k): str(v) for k, v in (drive_map_obj or {}).items()})

    scanned, scan_warnings, scan_errors = scan_files(
        roots=roots,
        exts=set(extensions),
        detect_corruption=detect_corruption,
        read_bytes=read_bytes,
        scan_retry_count=scan_retry_count,
    )
    if limit > 0:
        scanned = scanned[:limit]

    con = connect_db(args.db)
    create_schema_if_needed(con)
    ts = ts_compact()
    plan_path = move_dir / f"backfill_plan_{ts}.jsonl"
    apply_path = move_dir / f"backfill_apply_{ts}.jsonl"
    queue_path = llm_dir / f"backfill_metadata_queue_{ts}.jsonl"

    rows_for_apply: list[dict[str, Any]] = []
    rows_for_plan: list[dict[str, Any]] = []
    remapped_paths = 0
    rename_detected = 0
    corrupt_candidates = 0
    skipped_existing = 0
    missing_in_paths = 0
    errors: list[str] = []
    queue_candidates: list[dict[str, Any]] = []

    try:
        for err in scan_errors:
            errors.append(err)
        for sf in scanned:
            if sf.corrupt_candidate:
                corrupt_candidates += 1
                rows_for_plan.append(
                    {
                        "path": sf.win_path,
                        "path_id": path_id_for(sf.win_path),
                        "status": "error",
                        "reason": f"corrupt_candidate:{sf.corrupt_reason}",
                        "ts": now_iso(),
                    }
                )
                continue

            existing = fetchone(con, "SELECT path_id, path FROM paths WHERE path = ?", (sf.win_path,))
            if existing:
                pid = str(existing["path_id"])
                if include_observations:
                    obs = fetchone(con, "SELECT path_id FROM observations WHERE path_id = ? LIMIT 1", (pid,))
                    if obs is None:
                        rows_for_apply.append(
                            {
                                "op": "obs_only",
                                "path_id": pid,
                                "path": sf.win_path,
                                "drive": sf.drive,
                                "dir": sf.dir,
                                "name": sf.name,
                                "ext": sf.ext,
                                "size_bytes": sf.size,
                                "mtime_utc": sf.mtime_utc,
                                "type": sf.ext,
                            }
                        )
                        rows_for_plan.append(
                            {
                                "path": sf.win_path,
                                "path_id": pid,
                                "status": "planned",
                                "reason": "missing_observation",
                                "ts": now_iso(),
                            }
                        )
                    else:
                        skipped_existing += 1
                else:
                    skipped_existing += 1
                continue

            missing_in_paths += 1

            mapped_old_candidates: list[dict[str, Any]] = []
            if drive_map:
                inv_map = {v: k for k, v in drive_map.items()}
                if len(sf.win_path) >= 3 and sf.win_path[1] == ":":
                    cur_drive = sf.win_path[:2].upper()
                    if cur_drive in inv_map:
                        old_drive = inv_map[cur_drive]
                        old_path = old_drive + sf.win_path[2:]
                        old_row = fetchone(con, "SELECT path_id, path FROM paths WHERE path = ?", (old_path,))
                        if old_row:
                            new_conflict = fetchone(con, "SELECT path_id FROM paths WHERE path = ?", (sf.win_path,))
                            if new_conflict and str(new_conflict["path_id"]) != str(old_row["path_id"]):
                                rows_for_plan.append(
                                    {
                                        "path": sf.win_path,
                                        "path_id": str(old_row["path_id"]),
                                        "status": "skipped",
                                        "reason": "conflict_skip",
                                        "ts": now_iso(),
                                    }
                                )
                            else:
                                mapped_old_candidates.append(
                                    {
                                        "path_id": str(old_row["path_id"]),
                                        "old_path": old_path,
                                        "new_path": sf.win_path,
                                    }
                                )

            if mapped_old_candidates:
                c = mapped_old_candidates[0]
                remapped_paths += 1
                rows_for_apply.append(
                    {
                        "op": "remap_update",
                        "path_id": c["path_id"],
                        "path": sf.win_path,
                        "old_path": c["old_path"],
                        "drive": sf.drive,
                        "dir": sf.dir,
                        "name": sf.name,
                        "ext": sf.ext,
                        "size_bytes": sf.size,
                        "mtime_utc": sf.mtime_utc,
                        "type": sf.ext,
                    }
                )
                rows_for_plan.append(
                    {
                        "path": sf.win_path,
                        "path_id": c["path_id"],
                        "status": "remapped",
                        "reason": "drive_map",
                        "ts": now_iso(),
                    }
                )
                continue

            rename_rows = fetchall(
                con,
                """
                SELECT p.path_id, p.path
                FROM paths p
                JOIN observations o ON o.path_id = p.path_id
                WHERE p.name = ? AND o.size_bytes = ?
                GROUP BY p.path_id, p.path
                LIMIT 20
                """,
                (sf.name, sf.size),
            )
            if len(rename_rows) == 1:
                old = rename_rows[0]
                rename_detected += 1
                rows_for_apply.append(
                    {
                        "op": "rename_update",
                        "path_id": str(old["path_id"]),
                        "path": sf.win_path,
                        "old_path": str(old["path"]),
                        "drive": sf.drive,
                        "dir": sf.dir,
                        "name": sf.name,
                        "ext": sf.ext,
                        "size_bytes": sf.size,
                        "mtime_utc": sf.mtime_utc,
                        "type": sf.ext,
                    }
                )
                rows_for_plan.append(
                    {
                        "path": sf.win_path,
                        "path_id": str(old["path_id"]),
                        "status": "planned",
                        "reason": "rename_detected",
                        "ts": now_iso(),
                    }
                )
                continue
            if len(rename_rows) > 1:
                rows_for_plan.append(
                    {
                        "path": sf.win_path,
                        "path_id": path_id_for(sf.win_path),
                        "status": "skipped",
                        "reason": "rename_ambiguous",
                        "ts": now_iso(),
                    }
                )
                continue

            pid = path_id_for(sf.win_path)
            rows_for_apply.append(
                {
                    "op": "insert_or_upsert",
                    "path_id": pid,
                    "path": sf.win_path,
                    "drive": sf.drive,
                    "dir": sf.dir,
                    "name": sf.name,
                    "ext": sf.ext,
                    "size_bytes": sf.size,
                    "mtime_utc": sf.mtime_utc,
                    "type": sf.ext,
                }
            )
            rows_for_plan.append(
                {
                    "path": sf.win_path,
                    "path_id": pid,
                    "status": "planned",
                    "reason": "missing_path",
                    "ts": now_iso(),
                }
            )

        with plan_path.open("w", encoding="utf-8") as w:
            meta = {
                "_meta": {
                    "kind": "backfill_plan",
                    "generated_at": now_iso(),
                    "db": args.db,
                    "roots": roots,
                    "extensions": extensions,
                    "apply": bool(args.apply),
                    "queue_missing_metadata": queue_missing_metadata,
                }
            }
            w.write(safe_json(meta) + "\n")
            for r in rows_for_plan:
                w.write(safe_json(r) + "\n")

        upserted_paths = 0
        upserted_obs = 0
        run_id: str | None = None
        if args.apply:
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
                        "backfill",
                        args.dest_root,
                        now_iso(),
                        None,
                        "backfill_moved_files.py",
                        f"roots={len(roots)} scanned={len(scanned)}",
                    ),
                )
                for row in rows_for_apply:
                    pid = str(row["path_id"])
                    pth = str(row["path"])
                    con.execute(
                        """
                        INSERT INTO paths (path_id, path, drive, dir, name, ext, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(path_id) DO UPDATE SET
                          path=excluded.path,
                          drive=excluded.drive,
                          dir=excluded.dir,
                          name=excluded.name,
                          ext=excluded.ext,
                          updated_at=excluded.updated_at
                        """,
                        (pid, pth, row.get("drive"), row.get("dir"), row.get("name"), row.get("ext"), now_iso(), now_iso()),
                    )
                    upserted_paths += 1

                    if include_observations:
                        con.execute(
                            """
                            INSERT INTO observations (run_id, path_id, size_bytes, mtime_utc, type, name_flags)
                            VALUES (?, ?, ?, ?, ?, ?)
                            ON CONFLICT(run_id, path_id) DO UPDATE SET
                              size_bytes=excluded.size_bytes,
                              mtime_utc=excluded.mtime_utc,
                              type=excluded.type,
                              name_flags=excluded.name_flags
                            """,
                            (
                                run_id,
                                pid,
                                int(row.get("size_bytes") or 0),
                                row.get("mtime_utc"),
                                row.get("type"),
                                None,
                            ),
                        )
                        upserted_obs += 1

                    event_kind = "backfill_register"
                    detail: dict[str, Any] = {"path": pth, "op": row.get("op")}
                    if row.get("op") == "remap_update":
                        event_kind = "backfill_remap"
                        detail["old_path"] = row.get("old_path")
                        detail["new_path"] = pth
                    elif row.get("op") == "rename_update":
                        event_kind = "backfill_rename_detected"
                        detail["old_path"] = row.get("old_path")
                        detail["new_path"] = pth

                    con.execute(
                        """
                        INSERT INTO events (run_id, ts, kind, src_path_id, dst_path_id, detail_json, ok, error)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (run_id, now_iso(), event_kind, pid, None, safe_json(detail), 1, None),
                    )

                con.execute("UPDATE runs SET finished_at=? WHERE run_id=?", (now_iso(), run_id))
                con.commit()
            except Exception:
                con.rollback()
                raise

            with apply_path.open("w", encoding="utf-8") as w:
                meta = {
                    "_meta": {
                        "kind": "backfill_apply",
                        "generated_at": now_iso(),
                        "run_id": run_id,
                        "rows": len(rows_for_apply),
                    }
                }
                w.write(safe_json(meta) + "\n")
                for row in rows_for_apply:
                    w.write(
                        safe_json(
                            {
                                "path": row.get("path"),
                                "path_id": row.get("path_id"),
                                "status": "upserted",
                                "reason": row.get("op"),
                                "ts": now_iso(),
                            }
                        )
                        + "\n"
                    )

        target_rows: list[dict[str, Any]] = []
        for row in rows_for_apply:
            target_rows.append(
                {
                    "path_id": str(row["path_id"]),
                    "path": str(row["path"]),
                    "name": row.get("name"),
                    "mtime_utc": row.get("mtime_utc"),
                }
            )

        metadata_queue_planned_count = 0
        metadata_queue_path: str | None = None
        if queue_missing_metadata and target_rows:
            for tr in target_rows:
                md_row = fetchone(
                    con,
                    """
                    SELECT data_json
                    FROM path_metadata
                    WHERE path_id=? AND source='llm'
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """,
                    (tr["path_id"],),
                )
                data_json = str(md_row["data_json"]) if md_row and "data_json" in md_row.keys() else None
                if metadata_row_needs_queue(data_json):
                    queue_candidates.append(tr)
            metadata_queue_planned_count = len(queue_candidates)
            if args.apply:
                metadata_queue_path = str(queue_path)
                with queue_path.open("w", encoding="utf-8") as w:
                    w.write(
                        safe_json(
                            {
                                "_meta": {
                                    "kind": "backfill_metadata_queue",
                                    "generated_at": now_iso(),
                                    "source": "backfill_moved_files.py",
                                    "rows": len(queue_candidates),
                                }
                            }
                        )
                        + "\n"
                    )
                    for row in queue_candidates:
                        w.write(safe_json(row) + "\n")

        if args.scan_error_policy == "fail" and scan_warnings:
            errors.append(f"scan warnings treated as fatal by policy=fail: count={len(scan_warnings)}")
        if args.scan_error_policy == "threshold":
            threshold = max(0, int(args.scan_error_threshold or 0))
            if threshold <= 0:
                errors.append("scanErrorThreshold must be > 0 when scanErrorPolicy=threshold")
            elif len(scan_warnings) > threshold:
                errors.append(f"scan warnings exceeded threshold: {len(scan_warnings)} > {threshold}")

        warning_count = len(scan_warnings)
        warnings_out = scan_warnings
        warnings_truncated = False
        if warning_count > MAX_SUMMARY_WARNINGS:
            warnings_out = scan_warnings[:MAX_SUMMARY_WARNINGS]
            warnings_truncated = True

        summary = {
            "ok": len(errors) == 0,
            "tool": "video_pipeline_backfill_moved_files",
            "apply": bool(args.apply),
            "db": args.db,
            "planPath": str(plan_path),
            "applyPath": str(apply_path) if args.apply else None,
            "metadataQueuePath": metadata_queue_path,
            "scannedFiles": len(scanned),
            "missingInPaths": missing_in_paths,
            "upsertedPaths": upserted_paths if args.apply else 0,
            "upsertedObservations": upserted_obs if args.apply else 0,
            "remappedPaths": remapped_paths,
            "renameDetected": rename_detected,
            "corruptCandidates": corrupt_candidates,
            "skippedExisting": skipped_existing,
            "metadataQueuePlannedCount": metadata_queue_planned_count,
            "scanErrorPolicy": args.scan_error_policy,
            "scanErrorThreshold": int(args.scan_error_threshold or 0),
            "scanRetryCount": scan_retry_count,
            "warningCount": warning_count,
            "warningsTruncated": warnings_truncated,
            "warnings": warnings_out,
            "errors": errors,
        }
        print(safe_json(summary))
        return 0 if summary["ok"] else 1
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
