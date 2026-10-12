#!/usr/bin/env python3
"""Deterministic daily duplicate quarantine. Preview never writes the media DB."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from video_pipeline.platform.writer_lock import workflow_writer
from video_pipeline.platform.sqlite_snapshot import online_snapshot
import shutil
import sys
from datetime import datetime, timezone

import yaml

from video_pipeline.platform.pathscan_common import windows_to_wsl_path
from video_pipeline.workflows.dedup import make_plan, apply_plan, recover_quarantines, restore_plan
from video_pipeline.workflows.source_root import write_json


def configured_roots(source_root, dest_root, drive_routes):
    roots = [source_root, dest_root]
    if drive_routes:
        route_file = Path(windows_to_wsl_path(drive_routes))
        if not route_file.is_file():
            raise ValueError("drive routes file missing: " + str(route_file))
        routes = yaml.safe_load(route_file.read_text(encoding="utf-8")) or {}
        if routes.get("default_dest"):
            roots.append(routes["default_dest"])
        roots.extend(r["dest_root"] for r in routes.get("routes", []) if r.get("dest_root"))
    return list(dict.fromkeys(str(Path(windows_to_wsl_path(r)).resolve()) for r in roots if r))


def provision_scripts(ops):
    templates = Path(__file__).resolve().parent.parent / "templates" / "windows-scripts"
    for name in ("apply_move_plan.ps1", "_long_path_utils.ps1"):
        target = ops / "scripts" / name
        bundled = templates / name
        if not target.exists() or target.read_bytes() != bundled.read_bytes():
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                shutil.copy2(target, target.with_name(name + ".before-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")))
            shutil.copy2(bundled, target)


def execute(args):
    if args.restore_run and (args.date_evidence or args.apply_run or args.native_preview):
        raise ValueError("restore and date-plan preparation/application are separate operations")
    if args.date_evidence and args.apply_run:
        raise ValueError("use either new date evidence or an exact prepared run, not both")
    if args.native_preview and not (args.date_evidence or args.apply_run):
        raise ValueError("native preview requires a concrete date-first plan")
    if args.scheduled and (args.date_evidence or args.apply_run):
        raise ValueError("date-first evidence is explicit maintenance only; never implicit scheduled policy")
    ops = Path(windows_to_wsl_path(args.windows_ops_root)).resolve()
    db = Path(windows_to_wsl_path(args.db)).resolve()
    ops.mkdir(parents=True, exist_ok=True)
    with workflow_writer(ops):
        # Share ownership with daily_cli.py, including previews and restoration.
        recovered = recover_quarantines(db, ops) if args.apply else []
        if args.restore_run:
            if args.apply:
                provision_scripts(ops)
            return restore_plan(db, ops, ops / "duplicates" / "runs" / args.restore_run, apply=args.apply)
        if args.date_evidence or args.apply_run:
            from video_pipeline.workflows.date_dedup import make_date_plan, preview_date_plan, apply_date_plan
            if args.scheduled:
                raise ValueError("date-first evidence is explicit maintenance only; never implicit scheduled policy")
            if args.apply_run:
                directory = ops / "duplicates" / "runs" / args.apply_run
                if directory.resolve().parent != (ops / "duplicates" / "runs").resolve():
                    raise ValueError("apply requires an exact owned run ID")
                plan = json.loads((directory / "plan.json").read_text())
            else:
                roots = configured_roots(args.source_root, args.dest_root, args.drive_routes)
                plan,directory = make_date_plan(db,ops,roots,args.date_evidence)
            result = {"ok":True,"run_id":plan["run_id"],"preview":not args.apply,"plan_path":str(directory / "plan.json"),"decisions":plan["decisions"],"defer_reasons":plan["defer_reasons"],"video_content_reads":plan["video_content_reads"]}
            result.update(
                unresolved_same_episode_groups=plan.get("unresolved_same_episode_groups", []),
                unresolved_same_episode_files=plan.get("unresolved_same_episode_files", 0),
                cleanup_complete=plan.get("cleanup_complete", False),
            )
            if args.native_preview:
                provision_scripts(ops)
                result["native_preview"] = preview_date_plan(db,ops,directory)
            if args.apply:
                provision_scripts(ops)
                result["apply"] = apply_date_plan(db,ops,directory)
                result["ok"] = result["apply"]["ok"]
                result["cleanup_complete"] = result["ok"] and plan.get("candidate_review_complete", False)
            write_json(directory / "result.json",result)
            return result
        roots = configured_roots(args.source_root, args.dest_root, args.drive_routes)
        if not roots:
            raise ValueError("no configured recording roots")
        plan, directory = make_plan(db, ops, roots, max_groups=args.max_groups, min_age_seconds=args.min_age_seconds)
        result = {"ok": True, "run_id": plan["run_id"], "preview": not args.apply,
                  "policy": plan["policy"], "plan_path": str(directory / "plan.json"),
                  "groups_total": plan["groups_total"], "groups_checked": plan["groups_checked"],
                  "groups_remaining": plan["groups_remaining"], "decisions": plan["decisions"],
                  "defer_reasons": plan["defer_reasons"], "excluded": plan["excluded"], "recovered_runs": recovered}
        if args.apply:
            provision_scripts(ops)
            result["apply"] = apply_plan(db, ops, directory)
            result["ok"] = result["apply"]["ok"]
            write_json(ops / "duplicates" / "checkpoint.json", {"next_group_offset": plan["next_group_offset"], "run_id": plan["run_id"]})
        write_json(directory / "result.json", result)
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", help="Read only the video-library-pipeline config section")
    for name in ("db", "windows-ops-root", "source-root", "dest-root", "drive-routes"):
        parser.add_argument("--" + name)
    parser.add_argument("--max-groups", type=int, default=50)
    parser.add_argument("--min-age-seconds", type=int, default=3600)
    parser.add_argument("--apply", action="store_true", help="Quarantine confirmed duplicates; never permanently delete")
    parser.add_argument("--scheduled", action="store_true", help="Silent no-op success; report moves, changed deferrals and failures")
    parser.add_argument("--restore-run", help="Exact dedup run ID to restore; default preview, no overwrites")
    parser.add_argument("--date-evidence", help="Explicit date-first physical discovery directory; original-period and unknown recordings stay protected")
    parser.add_argument("--apply-run", help="Exact prepared date-first run ID; no replanning or automatic replay")
    parser.add_argument("--native-preview", action="store_true", help="Validate the exact date-first plan and all keepers on Windows without moving media")
    args = parser.parse_args()
    try:
        if args.config:
            cfg = json.loads(Path(args.config).read_text())["plugins"]["entries"]["video-library-pipeline"]["config"]
            for field, key in {"db": "db", "windows_ops_root": "windowsOpsRoot", "source_root": "sourceRoot", "dest_root": "destRoot", "drive_routes": "driveRoutesPath"}.items():
                if not getattr(args, field):
                    setattr(args, field, cfg.get(key))
        if not args.db and args.windows_ops_root:
            args.db = str(Path(windows_to_wsl_path(args.windows_ops_root)) / "db" / "mediaops.sqlite")
        missing = [k for k in ("db", "windows_ops_root", "source_root", "dest_root") if not getattr(args, k)]
        if missing:
            raise ValueError("missing config: " + ", ".join(missing))
        result = execute(args)
    except Exception as exc:
        result = {"ok": False, "error": str(exc)}
    if args.scheduled:
        if not args.windows_ops_root:
            print("重複番組整理: 設定不足\n" + result.get("error", ""))
            return 1
        ops = Path(windows_to_wsl_path(args.windows_ops_root))
        write_json(ops / "duplicates" / "last_result.json", result)
        if not result["ok"]:
            print("重複番組整理: 停止\n" + str(result.get("error") or result.get("apply")))
        else:
            import hashlib
            fingerprint = hashlib.sha256(json.dumps(result.get("defer_reasons", {}), sort_keys=True).encode()).hexdigest()
            notice_path = ops / "duplicates" / "notice.json"
            previous = json.loads(notice_path.read_text()).get("fingerprint") if notice_path.exists() else None
            moved = result.get("apply", {}).get("moved", 0)
            if moved or (result.get("defer_reasons") and fingerprint != previous):
                print(f"重複番組整理: {moved}件を復元可能な場所へ退避。本放送と再放送は別々に保持。完全削除はしていません。\n保留: {json.dumps(result.get('defer_reasons', {}), ensure_ascii=False)}\n詳細: {result.get('plan_path', '')}")
            else:
                print("NO_REPLY")
            write_json(notice_path, {"fingerprint": fingerprint})
    else:
        print(json.dumps(result, ensure_ascii=False))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
