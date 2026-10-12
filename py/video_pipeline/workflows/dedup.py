"""Auditable, recoverable quarantine for semantic recording duplicates.

This is an explicit maintenance CLI, not a hidden V1 tool or V2 gate bypass.
All Windows moves use the bundled PowerShell boundary and immutable identities.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from video_pipeline.platform.writer_lock import serialized_ops_function
from video_pipeline.platform.sqlite_snapshot import online_snapshot
import sqlite3
import uuid

from video_pipeline.db.mediaops_schema import connect_db, create_schema_if_needed
from video_pipeline.db.file_identity import ensure_identity_schema, observe_file, record_move
from video_pipeline.domain.recording_dedup import file_signature, plan_recordings, within_roots, observed_signature_matches, quarantine_root
from video_pipeline.platform.pathscan_common import windows_to_wsl_path, wsl_to_windows_path, now_iso
from video_pipeline.platform.windows_pwsh_bridge import run_pwsh_json
from video_pipeline.workflows.source_root import write_json
from video_pipeline.workflows.store import sha256_file


def readonly_db(db):
    con = sqlite3.connect(Path(db).resolve().as_uri() + "?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con


def metadata_checksum(con, path):
    row = con.execute("SELECT pm.*,p.path FROM path_metadata pm JOIN paths p USING(path_id) WHERE p.path=?", (path,)).fetchone()
    if not row:
        raise ValueError("recording metadata missing: " + path)
    return hashlib.sha256(json.dumps(dict(row), sort_keys=True, ensure_ascii=False).encode()).hexdigest()


@serialized_ops_function
def make_plan(db, ops_root, roots, *, max_groups=50, min_age_seconds=3600, probe=None):
    ops = Path(ops_root)
    checkpoint = ops / "duplicates" / "checkpoint.json"
    offset = json.loads(checkpoint.read_text()).get("next_group_offset", 0) if checkpoint.exists() else 0
    kwargs = {"max_groups": max_groups, "min_age_seconds": min_age_seconds, "group_offset": offset}
    if probe is not None:
        kwargs["probe"] = probe
    with readonly_db(db) as con:
        plan = plan_recordings(con, roots, **kwargs)
    run_id = "dedup_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:8]
    run_dir = ops / "duplicates" / "runs" / run_id
    run_dir.mkdir(parents=True)
    plan_path = run_dir / "plan.json"
    plan.update(run_id=run_id, db=str(Path(db).resolve()), roots=list(map(str, roots)), generated_at=now_iso())
    write_json(plan_path, plan)
    write_json(run_dir / "manifest.json", {"run_id": run_id, "plan_checksum": sha256_file(plan_path), "phase": "plan_ready"})
    return plan, run_dir


def commit_receipts(db, receipt_path, run_id):
    records = []
    lines = Path(receipt_path).read_text(encoding="utf-8-sig").splitlines()
    for i, line in enumerate(lines):
        if not line.strip():
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            if i != len(lines) - 1:
                raise
            break
    rows = [r for r in records if r.get("op") == "move"]
    if any(r.get("_meta", {}).get("dry_run") for r in records):
        raise ValueError("dry-run receipts cannot prove a quarantine")
    con = connect_db(str(db))
    try:
        create_schema_if_needed(con)
        ensure_identity_schema(con)
        con.commit()
        con.execute("BEGIN IMMEDIATE")
        con.execute("INSERT OR IGNORE INTO runs(run_id,kind,started_at) VALUES(?,'recording_dedup',?)", (run_id, now_iso()))
        for row in rows:
            if row.get("ok") is True and not row.get("dry_run"):
                receipt_id = hashlib.sha256(json.dumps(row, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
                record_move(con, row, receipt_id, run_id)
        con.execute("UPDATE runs SET finished_at=? WHERE run_id=?", (now_iso(), run_id))
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()
    return rows


@serialized_ops_function
def recover_quarantines(db, ops_root):
    recovered = []
    for intent_path in sorted((Path(ops_root) / "duplicates" / "runs").glob("*/intent.json")):
        marker = intent_path.with_name("db_committed.json")
        if marker.exists():
            continue
        intent = json.loads(intent_path.read_text())
        if Path(intent["db"]).resolve() != Path(db).resolve():
            raise ValueError("interrupted quarantine belongs to a different DB")
        if sha256_file(Path(intent["plan_path"])) != intent["checksum"]:
            raise ValueError("interrupted quarantine plan changed")
        receipt = Path(intent["receipt_path"])
        if not receipt.exists():
            raise ValueError("interrupted quarantine lacks durable receipts: " + str(intent_path))
        rows = commit_receipts(db, receipt, intent["run_id"])
        if len(rows) != intent["operations"]:
            raise ValueError("partial quarantine log; committed known receipts, manual reconciliation needed")
        write_json(marker, {"recovered": True, "rows": len(rows)})
        recovered.append(intent["run_id"])
    return recovered


@serialized_ops_function
def apply_plan(db, ops_root, run_dir, *, runner=run_pwsh_json):
    directory = Path(run_dir)
    plan_path = directory / "plan.json"
    manifest = json.loads((directory / "manifest.json").read_text())
    if sha256_file(plan_path) != manifest["plan_checksum"]:
        raise ValueError("dedup plan checksum changed")
    plan = json.loads(plan_path.read_text())
    if Path(plan["db"]).resolve() != Path(db).resolve():
        raise ValueError("dedup plan belongs to a different DB")
    if manifest["phase"] != "plan_ready":
        raise ValueError("dedup plan was already applied or interrupted")
    moves = plan["moves"]
    # Global keep/drop separation, including across original/repeat cohorts.
    drops = {r["src"] for r in moves}
    if len(drops) != len(moves) or drops & {r["keep"]["path"] for r in moves}:
        raise ValueError("invalid keep/drop overlap")
    roots = [Path(windows_to_wsl_path(p)).resolve() for p in plan["roots"]]
    with readonly_db(db) as con:
        for row in moves:
            for path, expected, checksum in [(row["src"], row["precondition"], row["metadata_checksum"]),
                                              (row["keep"]["path"], row["keep"], row["keep_metadata_checksum"])]:
                local = Path(windows_to_wsl_path(path))
                if not within_roots(local, roots):
                    raise ValueError("recording escaped configured roots")
                actual = file_signature(local)
                if any(actual[k] != expected[k] for k in actual):
                    raise ValueError("source or keeper changed after planning")
                if metadata_checksum(con, path) != checksum:
                    raise ValueError("source or keeper metadata changed after planning")
                if con.execute("SELECT 1 FROM recording_holds h JOIN paths p USING(path_id) WHERE p.path=? AND h.resolved_at IS NULL", (path,)).fetchone():
                    raise ValueError("source or keeper is held")
            dst = Path(windows_to_wsl_path(row["dst"]))
            source = Path(windows_to_wsl_path(row["src"]))
            expected_dst = quarantine_root(source, roots) / row["group_id"] / row["broadcast_kind"] / row["path_id"] / source.name
            if dst != expected_dst or dst.resolve() != expected_dst.absolute() or dst.exists() or dst.is_symlink():
                raise ValueError("quarantine destination invalid or already exists")
            if con.execute("SELECT 1 FROM path_file_state s JOIN paths p USING(path_id) WHERE p.path=?", (row["dst"],)).fetchone():
                raise ValueError("quarantine destination already has a file identity")
    if not moves:
        manifest["phase"] = "complete"
        write_json(directory / "manifest.json", manifest)
        return {"ok": True, "moved": 0, "failed": 0}
    backup = directory / "before.sqlite"
    online_snapshot(db, backup)
    # Capture observed instances before moving, not UUIDs derived from filenames.
    con = connect_db(str(db))
    try:
        create_schema_if_needed(con); ensure_identity_schema(con); con.commit()
        con.execute("BEGIN IMMEDIATE")
        con.execute("INSERT INTO runs(run_id,kind,started_at) VALUES(?,'recording_dedup',?)", (plan["run_id"], now_iso()))
        for row in moves:
            sig = row["precondition"]
            state = con.execute("SELECT * FROM path_file_state WHERE path_id=?", (row["path_id"],)).fetchone()
            if state:
                if not observed_signature_matches(dict(state), sig):
                    raise ValueError("source file identity changed before apply")
                row["file_id"] = state["file_id"]
            else:
                row["file_id"] = observe_file(con, row["path_id"], sig["size_bytes"], sig["mtime_utc"], plan["run_id"])
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()
    move_plan = directory / "move_plan.jsonl"
    move_plan.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in moves), encoding="utf-8")
    receipt_path = directory / "move_receipts.jsonl"
    intent = {"run_id": plan["run_id"], "db": str(Path(db).resolve()), "plan_path": str(move_plan),
              "checksum": sha256_file(move_plan), "receipt_path": str(receipt_path), "operations": len(moves)}
    write_json(directory / "intent.json", intent)
    manifest["phase"] = "applying"
    write_json(directory / "manifest.json", manifest)
    engine_error = None
    try:
        runner(str(Path(ops_root) / "scripts" / "apply_move_plan.ps1"), [
            "-PlanJsonl", wsl_to_windows_path(str(move_plan)), "-OutJsonl", wsl_to_windows_path(str(receipt_path)),
            "-OpsRoot", wsl_to_windows_path(str(ops_root)), "-OnDstExists", "error",
        ])
    except Exception as exc:
        engine_error = str(exc)
    if not receipt_path.exists():
        raise RuntimeError(engine_error or "quarantine engine returned no receipts")
    rows = commit_receipts(db, receipt_path, plan["run_id"])
    if len(rows) != len(moves):
        raise RuntimeError("partial quarantine receipts; manual reconciliation needed")
    write_json(directory / "db_committed.json", {"rows": len(rows)})
    moved = sum(r.get("ok") is True for r in rows)
    result = {"ok": moved == len(moves) and not engine_error, "moved": moved,
              "failed": len(moves) - moved, "engine_error": engine_error, "receipt_path": str(receipt_path)}
    manifest["phase"] = "complete" if result["ok"] else "failed"
    write_json(directory / "manifest.json", manifest)
    return result


def native_restore_signature(local, destination, expected, roots, native_cache):
    """Use checked native ancestry, not slow/unaddressable DrvFS realpath calls."""
    from video_pipeline.platform.native_file_signatures import native_path_key
    observed = native_cache.get(native_path_key(local), {})
    target = native_cache.get(native_path_key(destination), {})
    if (not observed.get("exists") or observed.get("error")
            or not observed.get("ancestors_checked") or target.get("error")
            or not target.get("ancestors_checked")):
        raise ValueError("restore native path/ancestor verification failed")
    if local.absolute() != expected.absolute() or not any(
            destination.absolute().is_relative_to(root) for root in roots):
        raise ValueError("restore outside configured roots")
    if target.get("exists"):
        raise ValueError("restore would overwrite an existing recording")
    return observed["signature"]


@serialized_ops_function
def restore_plan(db, ops_root, original_dir, *, apply=False, runner=run_pwsh_json):
    original = Path(original_dir).resolve()
    runs_root = (Path(ops_root) / "duplicates" / "runs").resolve()
    if original.parent != runs_root:
        raise ValueError("restore requires an exact dedup run directory")
    plan_path = original / "plan.json"
    manifest = json.loads((original / "manifest.json").read_text())
    if sha256_file(plan_path) != manifest["plan_checksum"]:
        raise ValueError("original dedup plan changed")
    plan = json.loads(plan_path.read_text())
    if Path(plan["db"]).resolve() != Path(db).resolve():
        raise ValueError("restore DB mismatch")
    roots = [Path(windows_to_wsl_path(p)).resolve() for p in plan["roots"]]
    intended = {(m["src"], m["dst"]): m for m in plan["moves"]}
    receipt_path = original / "move_receipts.jsonl"
    records = [json.loads(line) for line in receipt_path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]
    native_paths = {}
    for r in records:
        if r.get('op') == 'move' and r.get('ok') is True and not r.get('dry_run'):
            for name in ('src','dst'):
                local = Path(windows_to_wsl_path(r[name]))
                win = wsl_to_windows_path(str(local))
                if len(win) >= 3 and win[1:3] == ':\\':
                    native_paths[win.casefold()] = local
    from video_pipeline.platform.native_file_signatures import read_native_signatures
    native_cache = read_native_signatures(list(native_paths.values())) if native_paths else {}
    moves = []
    with readonly_db(db) as con:
        for r in records:
            if r.get("op") != "move" or r.get("ok") is not True or r.get("dry_run"):
                continue
            m = intended.get((r["src"], r["dst"]))
            if not m or not r.get("file_id"):
                raise ValueError("restore receipt does not match quarantine plan")
            local = Path(windows_to_wsl_path(r["dst"]))
            destination = Path(windows_to_wsl_path(r["src"]))
            expected = quarantine_root(destination, roots) / m["group_id"] / m["broadcast_kind"] / m["path_id"] / destination.name
            if wsl_to_windows_path(str(local)).casefold() in native_paths:
                sig = native_restore_signature(local, destination, expected, roots, native_cache)
            else:
                if local != expected or local.resolve() != expected.absolute() or not within_roots(destination,roots):
                    raise ValueError("restore outside configured roots")
                if destination.exists() or destination.is_symlink():
                    raise ValueError("restore would overwrite an existing recording")
                sig = file_signature(local)
            # UTC strings are presentation: float timestamp conversions can round
            # them by 1us despite identical integer NTFS nanoseconds (seen on 66
            # actual recordings). Keep exact size/mtime_ns as the authority.
            if any(sig[k] != m["precondition"][k] for k in ("size_bytes", "mtime_ns")):
                raise ValueError("quarantined recording changed")
            state = con.execute("SELECT s.* FROM path_file_state s JOIN paths p USING(path_id) WHERE p.path=?", (r["dst"],)).fetchone()
            if not state or state["file_id"] != r["file_id"]:
                raise ValueError("quarantine identity mismatch")
            moves.append({"op": "move", "src": r["dst"], "dst": r["src"], "path_id": state["path_id"],
                          "file_id": r["file_id"], "precondition": sig})
    result = {"ok": True, "preview": not apply, "restore_operations": len(moves)}
    if not apply or not moves:
        return result
    directory = runs_root / ("restore_" + uuid.uuid4().hex)
    directory.mkdir()
    online_snapshot(db, directory / "before.sqlite")
    move_plan = directory / "move_plan.jsonl"
    move_plan.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in moves), encoding="utf-8")
    receipt = directory / "move_receipts.jsonl"
    write_json(directory / "intent.json", {"run_id": directory.name, "db": str(Path(db).resolve()),
        "plan_path": str(move_plan), "checksum": sha256_file(move_plan), "receipt_path": str(receipt), "operations": len(moves)})
    runner(str(Path(ops_root) / "scripts" / "apply_move_plan.ps1"), ["-PlanJsonl", wsl_to_windows_path(str(move_plan)),
        "-OutJsonl", wsl_to_windows_path(str(receipt)), "-OpsRoot", wsl_to_windows_path(str(ops_root)), "-OnDstExists", "error"])
    rows = commit_receipts(db, receipt, directory.name)
    if len(rows) != len(moves):
        raise RuntimeError("partial restore receipts")
    write_json(directory / "db_committed.json", {"rows": len(rows)})
    result.update(ok=all(r.get("ok") is True for r in rows), receipt_path=str(receipt))
    return result
