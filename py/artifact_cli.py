#!/usr/bin/env python3
"""Artifact inventory and explicit, recoverable cache maintenance; no LLM."""
import argparse
import json
from pathlib import Path

from video_pipeline.platform.artifact_retention import (
    apply_cleanup, create_workspace, finish_workspace, inventory, plan_cleanup,
    restore_cleanup, valid_id,
)
from video_pipeline.platform.pathscan_common import windows_to_wsl_path
from video_pipeline.platform.legacy_artifact_archive import plan_archive, apply_archive, restore_archive


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True)
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--create", metavar="JOB_ID")
    actions.add_argument("--complete", metavar="JOB_ID")
    actions.add_argument("--plan", action="store_true")
    actions.add_argument("--apply-plan", metavar="PLAN_ID")
    actions.add_argument("--restore", metavar="PLAN_ID")
    actions.add_argument("--archive-to", metavar="DIRECTORY", help="Plan byte-preserving archival of a legacy artifacts/ root outside the plugin")
    actions.add_argument("--apply-archive", metavar="PLAN_PATH")
    actions.add_argument("--restore-archive", metavar="RECEIPT_PATH")
    parser.add_argument("--apply", action="store_true", help="Only for --restore/--restore-archive; otherwise restoration is a preview")
    parser.add_argument("--cache-days", type=int, default=7)
    args = parser.parse_args(argv)
    try:
        if args.apply and not (args.restore or args.restore_archive):
            raise ValueError("--apply is only valid with --restore or --restore-archive; cleanup requires --apply-plan")
        root = Path(windows_to_wsl_path(args.root))
        if args.create:
            job = create_workspace(root, args.create, cache_days=args.cache_days)
            result = {"ok": True, "jobId": job.name, "cache": str(job / "cache"), "evidence": str(job / "evidence")}
        elif args.complete:
            finish_workspace(root / valid_id(args.complete))
            result = {"ok": True, "jobId": args.complete, "status": "complete"}
        elif args.plan:
            result = plan_cleanup(root)
        elif args.apply_plan:
            result = apply_cleanup(root, args.apply_plan)
        elif args.restore:
            result = restore_cleanup(root, args.restore, apply=args.apply)
        elif args.archive_to:
            result = plan_archive(root, Path(windows_to_wsl_path(args.archive_to)))
        elif args.apply_archive:
            result = apply_archive(root, Path(windows_to_wsl_path(args.apply_archive)))
        elif args.restore_archive:
            result = restore_archive(root, Path(windows_to_wsl_path(args.restore_archive)), apply=args.apply)
        else:
            result = inventory(root)
    except Exception as error:
        result = {"ok": False, "error": str(error)}
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
