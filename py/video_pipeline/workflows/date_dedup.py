"""Exact-plan application for explicitly authorized date-first quarantine."""
from datetime import datetime, timezone
import json
from pathlib import Path
from video_pipeline.platform.writer_lock import serialized_ops_function
from video_pipeline.platform.sqlite_snapshot import online_snapshot
import sqlite3
import uuid

from video_pipeline.db.file_identity import ensure_identity_schema, ensure_location, observe_file
from video_pipeline.db.mediaops_schema import connect_db, create_schema_if_needed
from video_pipeline.domain.date_recording_dedup import location_snapshot, snapshot_cache, plan_by_date, validate_candidate
from video_pipeline.domain.recording_dedup import quarantine_root, observed_signature_matches
from video_pipeline.platform.pathscan_common import windows_to_wsl_path, wsl_to_windows_path, now_iso
from video_pipeline.platform.windows_pwsh_bridge import run_pwsh_json
from video_pipeline.workflows.dedup import readonly_db, commit_receipts
from video_pipeline.workflows.source_root import write_json
from video_pipeline.workflows.store import sha256_file


@serialized_ops_function
def make_date_plan(db, ops_root, roots, evidence_dir):
    with readonly_db(db) as con:
        plan = plan_by_date(con, roots, evidence_dir)
    run_id = "dedup_date_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:8]
    directory = Path(ops_root)/"duplicates"/"runs"/run_id
    directory.mkdir(parents=True)
    plan.update(run_id=run_id,db=str(Path(db).resolve()),generated_at=now_iso())
    write_json(directory/"plan.json",plan)
    write_json(directory/"manifest.json",{"run_id":run_id,"plan_checksum":sha256_file(directory/"plan.json"),"phase":"plan_ready"})
    return plan,directory


def validate_date_plan(db, directory):
    directory = Path(directory)
    manifest = json.loads((directory/"manifest.json").read_text())
    if manifest["phase"] != "plan_ready" or sha256_file(directory/"plan.json") != manifest["plan_checksum"]:
        raise ValueError("changed, applied or interrupted exact date plan")
    plan = json.loads((directory/"plan.json").read_text())
    if plan["policy"] != "explicit_date_first_preserve_original_and_later_airings" or Path(plan["db"]).resolve() != Path(db).resolve():
        raise ValueError("date plan DB/policy mismatch")
    for name,digest in plan["evidence_checksums"].items():
        if sha256_file(Path(plan["evidence_directory"])/name) != digest:
            raise ValueError("date evidence changed")
    roots = [Path(windows_to_wsl_path(p)).resolve() for p in plan["roots"]]
    drops = {d["path"]:d for d in plan["drops"]}
    keeps = {k["path"]:k for k in plan["keepers"]}
    if len(drops) != len(plan["moves"]) or drops.keys() & keeps.keys():
        raise ValueError("global keep/drop overlap")
    with readonly_db(db) as con:
        cache = snapshot_cache(con)
        native_paths = []
        for item in [*drops.values(),*keeps.values()]:
            local=Path(windows_to_wsl_path(item['path']))
            if str(local).startswith('/mnt/'):
                try: local.stat()
                except OSError: native_paths.append(local)
        from video_pipeline.platform.native_file_signatures import read_native_signatures
        native_cache=read_native_signatures(native_paths) if native_paths else {}
        for item in [*drops.values(),*keeps.values()]:
            validate_candidate(con,item,roots,cache,native_cache)
        for m in plan["moves"]:
            if m["src"] not in drops or not m.get("keeps") or any(k["path"] not in keeps or k != {"path":k["path"],**keeps[k["path"]]["signature"]} for k in m["keeps"]):
                raise ValueError("move has invalid coverage keepers")
            source = Path(windows_to_wsl_path(m["src"]))
            dst = Path(windows_to_wsl_path(m["dst"]))
            expected = quarantine_root(source,roots)/m["group_id"]/m["broadcast_kind"]/m["path_id"]/source.name
            if dst != expected or dst.resolve() != expected.absolute() or dst.exists() or dst.is_symlink():
                raise ValueError("invalid/occupied quarantine destination")
            current = location_snapshot(con,m["dst"],cache)
            if current["state"] or current["metadata"] or current["historical"]:
                raise ValueError("quarantine destination occupied in DB")
    return plan,manifest


@serialized_ops_function
def preview_date_plan(db, ops_root, directory, *, runner=run_pwsh_json):
    directory = Path(directory)
    plan,_ = validate_date_plan(db,directory)
    preview = directory/"native_preview_plan.jsonl"
    receipt = directory/"native_preview_receipts.jsonl"
    if receipt.exists():
        raise ValueError("native preview already recorded; inspect it instead of replaying")
    preview.write_text("".join(json.dumps(m,ensure_ascii=False)+"\n" for m in plan["moves"]))
    runner(str(Path(ops_root)/"scripts"/"apply_move_plan.ps1"),["-PlanJsonl",wsl_to_windows_path(str(preview)),"-OutJsonl",wsl_to_windows_path(str(receipt)),"-OpsRoot",wsl_to_windows_path(str(ops_root)),"-OnDstExists","error","-DryRun"])
    records = [json.loads(line) for line in receipt.read_text(encoding="utf-8-sig").splitlines() if line.strip()]
    rows = [r for r in records if r.get("op")=="move"]
    result = {"ok":len(rows)==len(plan["moves"]) and all(r.get("ok") and r.get("dry_run") for r in rows),"checked":len(rows),"moved":0,"failed":[r for r in rows if not r.get("ok")]}
    write_json(directory/"native_preview_result.json",result)
    if not result["ok"]:
        raise ValueError("native date plan preflight failed; inspect receipts")
    return result


@serialized_ops_function
def apply_date_plan(db, ops_root, directory, *, runner=run_pwsh_json):
    directory = Path(directory)
    plan,manifest = validate_date_plan(db,directory)
    moves = plan["moves"]
    native = json.loads((directory/"native_preview_result.json").read_text())
    if not native["ok"] or native["checked"] != len(moves):
        raise ValueError("successful exact native preflight required")
    online_snapshot(db, directory / "before.sqlite")
    con = connect_db(str(db))
    try:
        create_schema_if_needed(con);ensure_identity_schema(con);con.commit()
        con.execute("BEGIN IMMEDIATE")
        con.execute("INSERT INTO runs(run_id,kind,started_at) VALUES(?,'recording_dedup',?)",(plan["run_id"],now_iso()))
        for m in moves:
            pid = ensure_location(con,m["src"])
            if pid != m["path_id"]:
                raise ValueError("source location identity changed")
            state = con.execute("SELECT * FROM path_file_state WHERE path_id=?",(pid,)).fetchone()
            if state and not observed_signature_matches(dict(state),m["precondition"]):
                raise ValueError("source file instance changed")
            m["file_id"] = state["file_id"] if state else observe_file(con,pid,m["precondition"]["size_bytes"],m["precondition"]["mtime_utc"],plan["run_id"])
        con.commit()
    except Exception:
        con.rollback();raise
    finally:
        con.close()
    move_plan = directory/"move_plan.jsonl"
    move_plan.write_text("".join(json.dumps(m,ensure_ascii=False)+"\n" for m in moves))
    receipt = directory/"move_receipts.jsonl"
    write_json(directory/"intent.json",{"run_id":plan["run_id"],"db":str(Path(db).resolve()),"plan_path":str(move_plan),"checksum":sha256_file(move_plan),"receipt_path":str(receipt),"operations":len(moves)})
    manifest["phase"]="applying";write_json(directory/"manifest.json",manifest)
    engine_error = None
    try:
        runner(str(Path(ops_root)/"scripts"/"apply_move_plan.ps1"),["-PlanJsonl",wsl_to_windows_path(str(move_plan)),"-OutJsonl",wsl_to_windows_path(str(receipt)),"-OpsRoot",wsl_to_windows_path(str(ops_root)),"-OnDstExists","error"])
    except Exception as exc:
        engine_error = str(exc)
    if not receipt.exists():
        raise RuntimeError(engine_error or "date quarantine has no durable receipts; reconcile before retry")
    rows = commit_receipts(db,receipt,plan["run_id"])
    if len(rows) != len(moves):
        raise RuntimeError("partial date quarantine receipts; known DB effects committed, reconcile before retry")
    write_json(directory/"db_committed.json",{"rows":len(rows)})
    moved = sum(r.get("ok") is True for r in rows)
    result = {"ok":moved==len(moves) and not engine_error,"moved":moved,"failed":len(moves)-moved,"engine_error":engine_error,"receipt_path":str(receipt)}
    manifest["phase"]="complete" if result["ok"] else "failed"
    write_json(directory/"manifest.json",manifest);write_json(directory/"apply_result.json",result)
    return result
