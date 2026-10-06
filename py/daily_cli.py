#!/usr/bin/env python3
"""Deterministic daily controller. No model, implicit review approval or overwrite.

Default preview clones SQLite and uses a separate workflow root. --apply is the
explicit scheduled policy: fresh, conflict-free operations only, no review bypass.
"""
from __future__ import annotations
import argparse
import fcntl
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from video_pipeline.workflows.source_root import (
    SourceRootWorkflowService, SourceRootDryRunConfig, SourceRootApplyConfig,
    parse_last_json_object_line, write_json, local_path_from_any,
)
from video_pipeline.workflows.store import WorkflowStore
from video_pipeline.domain.move_apply_stats import aggregate_move_apply

PY_ROOT = Path(__file__).resolve().parent


def run_python(script, args, cwd=None):
    result = subprocess.run([sys.executable,str(script),*args],cwd=cwd,capture_output=True,text=True,encoding="utf-8")
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout


def recover_receipts(ops_root, db):
    recovered = []
    for intent in sorted(Path(ops_root).glob("runs/*/apply/apply_intent.json")):
        marker = intent.with_name("db_committed.json")
        if marker.exists():
            continue
        info = json.loads(intent.read_text())
        log = Path(info["receiptPath"])
        if not log.exists():
            raise RuntimeError("実行中断: 移動記録がありません。状態確認が必要です: " + str(intent))
        from video_pipeline.workflows.store import sha256_file
        if sha256_file(Path(info["plan"])) != info["planChecksum"]:
            raise RuntimeError("中断した移動計画のchecksumが変わっています: " + str(intent))
        result = parse_last_json_object_line(run_python(PY_ROOT / "update_db_paths_from_move_apply.py", ["--db",str(db),"--applied",str(log)]))
        stats = aggregate_move_apply(log)
        expected = sum(1 for line in Path(info["plan"]).read_text().splitlines() if line.strip() and "_meta" not in json.loads(line))
        write_json(intent.with_name("recovery_result.json"), {"dbUpdate":result,"moveStats":stats})
        if stats["totalOps"] != expected:
            # Successful receipts are committed, but unlogged filesystem changes
            # cannot be guessed from equal filenames/size.
            raise RuntimeError("移動記録が途中で切れています。記録済み分のDBを復旧しました: " + str(intent))
        write_json(marker,{"recovered":True,"dbUpdate":result,"moveStats":stats})
        recovered.append(info["runId"])
    return recovered


def evaluate_plan(run):
    artifact = run.artifacts.get("source_root_move_plan")
    summary = artifact.metadata.get("summary", {}) if artifact else {}
    blockers = {key:value for key,value in summary.items() if key.startswith("skipped_") and value}
    blockers.update({key:value for key,value in summary.get("preflight_counts",{}).items() if key not in {"destination_exists","held_legacy_collision","source_changed","source_missing"} and value})
    if any(g.status == "open" for g in run.review_gates.values()):
        blockers["metadata_review"] = True
    return summary, blockers


def execute(args):
    ops = local_path_from_any(args.windows_ops_root)
    db = local_path_from_any(args.db)
    ops.mkdir(parents=True,exist_ok=True)
    # A single owner across CLI, scheduled and preview invocations.
    with (ops / "daily.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if not Path(args.drive_routes).is_file():
            raise ValueError("振り分け設定が存在しません: " + args.drive_routes)
        import shutil
        from datetime import datetime,timezone
        templates = PY_ROOT.parent / "assets" / "windows-scripts"
        for name in ("unwatched_inventory.ps1","apply_move_plan.ps1","_long_path_utils.ps1"):
            target = ops / "scripts" / name; bundled = templates / name
            if not target.exists() or target.read_bytes() != bundled.read_bytes():
                target.parent.mkdir(parents=True,exist_ok=True)
                if target.exists(): shutil.copy2(target,target.with_name(name+".before-"+datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")))
                shutil.copy2(bundled,target)
        recovered = recover_receipts(ops,db) if args.apply else []
        workflow_ops = ops
        if not args.apply:
            import tempfile
            workflow_ops = Path(tempfile.mkdtemp(prefix="preview_",dir=ops))
            import shutil
            shutil.copytree(ops / "scripts",workflow_ops / "scripts")
            preview_db = workflow_ops / "mediaops.sqlite"
            with sqlite3.connect(f"file:{db}?mode=ro",uri=True) as source, sqlite3.connect(preview_db) as target:
                source.backup(target)
            db = preview_db
        epg = parse_last_json_object_line(run_python(PY_ROOT / "ingest_program_txt.py", ["--db",str(db),"--ts-root",args.ts_root,"--apply"]))
        if epg.get("ok") is not True or epg.get("parseFailed",0):
            raise RuntimeError("EPG取り込みに失敗: " + json.dumps(epg,ensure_ascii=False))
        service = SourceRootWorkflowService(python_runner=run_python)
        result = service.dry_run(SourceRootDryRunConfig(
            windows_ops_root=str(workflow_ops),source_root=args.source_root,dest_root=args.dest_root,
            db=str(db),drive_routes=args.drive_routes,max_files_per_run=args.limit,auto_apply=args.apply))
        store = WorkflowStore(workflow_ops)
        run = store.read_run(result.run_id)
        summary, blockers = evaluate_plan(run)
        if not result.ok or blockers or run.phase == "review_required":
            return {"ok":False,"outcome":"review_or_execution_blocked","runId":run.run_id,"phase":run.phase,"blockers":blockers,"workflow":result.to_dict()},2
        conflicts = sum(summary.get("preflight_counts",{}).get(key,0) for key in ("destination_exists","held_legacy_collision"))
        if args.apply and run.phase == "plan_ready":
            result = service.apply(SourceRootApplyConfig(windows_ops_root=str(ops),run_id=run.run_id,db=str(db)))
        payload = {"ok":result.ok,"runId":run.run_id,"phase":str(result.to_dict()["phase"]),"preview":not args.apply,"planned":summary.get("planned",0),"conflicts":conflicts,"deferred":sum(summary.get("preflight_counts",{}).get(key,0) for key in ("source_changed","source_missing")),"recoveredRuns":recovered,"workflow":result.to_dict()}
        write_json(workflow_ops / "daily_result.json",payload)
        return payload, 0 if result.ok else 1


def main():
    parser = argparse.ArgumentParser()
    for name in ("db","windows-ops-root","source-root","dest-root","ts-root","drive-routes"):
        parser.add_argument("--"+name)
    parser.add_argument("--config",help="OpenClaw config; read only this plugin section")
    parser.add_argument("--limit",type=int,default=500)
    parser.add_argument("--apply",action="store_true")
    parser.add_argument("--scheduled",action="store_true")
    args = parser.parse_args()
    try:
        if args.config:
            cfg = json.loads(Path(args.config).read_text())["plugins"]["entries"]["video-library-pipeline"]["config"]
            for field,key in {"db":"db","windows_ops_root":"windowsOpsRoot","source_root":"sourceRoot","dest_root":"destRoot","ts_root":"tsRoot","drive_routes":"driveRoutesPath"}.items():
                if not getattr(args,field): setattr(args,field,cfg.get(key))
            if not args.db and args.windows_ops_root:
                args.db = str(local_path_from_any(args.windows_ops_root) / "db" / "mediaops.sqlite")
        missing = [key for key in ("db","windows_ops_root","source_root","dest_root","ts_root","drive_routes") if not getattr(args,key)]
        if missing: raise ValueError("設定不足: " + ", ".join(missing))
        payload, code = execute(args)
    except Exception as exc:
        payload,code = {"ok":False,"error":str(exc)},1
    if args.scheduled:
        ops = local_path_from_any(args.windows_ops_root) if args.windows_ops_root else Path(args.config or ".").resolve().parent
        write_json(ops / "daily_result.json",payload)
        if not payload.get("ok"):
            print("未視聴整理: 停止\n" + str(payload.get("error") or payload.get("blockers") or payload.get("outcome")) + "\n詳細: " + str(ops / "daily_result.json"))
        else:
            from hashlib import sha256
            conflict_artifact = next((a for a in payload["workflow"].get("artifacts",[]) if a["type"] == "source_root_move_conflicts"),None)
            content = Path(conflict_artifact["path"]).read_text() if conflict_artifact else "[]"
            known_conflicts = [r for r in json.loads(content) if r.get("reason") in {"destination_exists","held_legacy_collision"}]
            fingerprint = sha256(json.dumps(known_conflicts,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
            notice = ops / "daily_conflict_notice.json"
            previous = json.loads(notice.read_text()).get("fingerprint") if notice.exists() else None
            if payload["conflicts"] and fingerprint != previous:
                print(f"未視聴整理: 移動先が存在する{payload['conflicts']}件を計画から除外しました。上書き・削除・同一内容の推定はしていません。\n詳細: {conflict_artifact['path']}")
            else:
                print("NO_REPLY")
            write_json(notice,{"fingerprint":fingerprint,"count":payload["conflicts"]})
    else:
        print(json.dumps(payload,ensure_ascii=False))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
