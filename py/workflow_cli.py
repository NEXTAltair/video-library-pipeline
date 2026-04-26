"""Thin CLI dispatcher for V2 run-based workflows."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from video_pipeline.platform.pathscan_common import windows_to_wsl_path
from video_pipeline.workflows import (
    NextAction,
    RelocateApplyConfig,
    RelocateDryRunConfig,
    RelocateWorkflowService,
    ReviewGateStatus,
    SourceRootApplyConfig,
    SourceRootDryRunConfig,
    SourceRootWorkflowService,
    WorkflowStore,
)


def _json_out(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))


def _local_path(value: str) -> Path:
    return Path(windows_to_wsl_path(str(value))).resolve()


def _parse_json_array(value: str) -> list[str]:
    if not value:
        return []
    parsed = json.loads(value)
    if not isinstance(parsed, list):
        raise ValueError("expected JSON array")
    return [str(item) for item in parsed if isinstance(item, str) and item]


def _store(windows_ops_root: str) -> WorkflowStore:
    return WorkflowStore(_local_path(windows_ops_root))


def _run_to_status(run: Any, *, include_artifacts: bool) -> dict[str, Any]:
    payload = run.to_dict()
    payload["nextActions"] = [action.to_dict() for action in _next_actions_for_run(run)]
    if not include_artifacts:
        payload.pop("artifacts", None)
        payload.pop("reviewGates", None)
    return payload


def _latest_artifact_id(run: Any, artifact_type: str) -> str | None:
    for artifact_id in reversed(run.artifact_ids):
        artifact = run.artifacts.get(artifact_id)
        if artifact is not None and artifact.type == artifact_type:
            return artifact.id
    return None


def _next_actions_for_run(run: Any) -> list[NextAction]:
    if run.phase == "plan_ready":
        if run.flow == "source_root":
            plan_id = _latest_artifact_id(run, "source_root_move_plan")
            if plan_id:
                return [
                    NextAction(
                        action="review_plan",
                        label="Review sourceRoot move plan",
                        tool="video_pipeline_resume",
                        params={
                            "runId": run.run_id,
                            "artifactId": plan_id,
                            "resumeAction": "apply_source_root_move_plan",
                        },
                        requires_human_input=True,
                    )
                ]
        if run.flow == "relocate":
            plan_id = _latest_artifact_id(run, "relocate_plan")
            if plan_id:
                return [
                    NextAction(
                        action="review_plan",
                        label="Review relocate move plan",
                        tool="video_pipeline_resume",
                        params={
                            "runId": run.run_id,
                            "artifactId": plan_id,
                            "resumeAction": "apply_relocate_move_plan",
                        },
                        requires_human_input=True,
                    )
                ]
    if run.phase != "review_required":
        return []

    open_gates = [
        gate
        for gate_id in run.review_gate_ids
        if (gate := run.review_gates.get(gate_id)) is not None and gate.status == ReviewGateStatus.OPEN.value
    ]
    if run.flow == "source_root":
        metadata_gate = next((gate for gate in open_gates if gate.type == "metadata_review"), None)
        if metadata_gate is None:
            return []
        review_yaml_paths = [
            run.artifacts[artifact_id].path
            for artifact_id in metadata_gate.artifact_ids
            if artifact_id in run.artifacts and run.artifacts[artifact_id].type == "metadata_review_yaml"
        ]
        return [
            NextAction(
                action="review_metadata",
                label="Review extracted metadata YAML",
                tool="video_pipeline_resume",
                params={
                    "runId": run.run_id,
                    "gateId": metadata_gate.id,
                    "artifactIds": list(metadata_gate.artifact_ids),
                    "reviewYamlPaths": review_yaml_paths,
                    "resumeAction": "apply_reviewed_metadata",
                },
                requires_human_input=True,
            )
        ]
    if run.flow == "relocate":
        queue_id = _latest_artifact_id(run, "relocate_metadata_queue")
        diagnostics_id = _latest_artifact_id(run, "relocate_diagnostics")
        review_gate = next((gate for gate in open_gates if gate.type == "relocate_metadata_review"), None)
        if queue_id:
            return [
                NextAction(
                    action="prepare_relocate_metadata",
                    label="Prepare missing or blocked relocate metadata",
                    tool="video_pipeline_resume",
                    params={"runId": run.run_id, "artifactIds": [queue_id]},
                    requires_human_input=review_gate is not None,
                )
            ]
        if review_gate is not None:
            return [
                NextAction(
                    action="review_relocate_metadata",
                    label="Review blocked relocate metadata",
                    tool="video_pipeline_resume",
                    params={
                        "runId": run.run_id,
                        "gateId": review_gate.id,
                        "artifactIds": list(review_gate.artifact_ids),
                    },
                    requires_human_input=True,
                )
            ]
        if diagnostics_id:
            return [
                NextAction(
                    action="prepare_relocate_metadata",
                    label="Prepare missing or blocked relocate metadata",
                    tool="video_pipeline_resume",
                    params={"runId": run.run_id, "artifactIds": [diagnostics_id]},
                    requires_human_input=False,
                )
            ]
    return []


def _cmd_start(args: argparse.Namespace) -> dict[str, Any]:
    if args.flow == "source_root":
        result = SourceRootWorkflowService().dry_run(
            SourceRootDryRunConfig(
                windows_ops_root=args.windows_ops_root,
                source_root=args.source_root,
                dest_root=args.dest_root,
                db=args.db,
                drive_routes=args.drive_routes_path,
                max_files_per_run=args.max_files_per_run,
                allow_needs_review=args.allow_needs_review,
                run_id=args.run_id or None,
            )
        )
        return result.to_dict()
    if args.flow == "relocate":
        result = RelocateWorkflowService().dry_run(
            RelocateDryRunConfig(
                windows_ops_root=args.windows_ops_root,
                dest_root=args.dest_root,
                db=args.db,
                roots=_parse_json_array(args.roots_json),
                roots_file_path=args.roots_file_path,
                extensions=_parse_json_array(args.extensions_json),
                drive_routes=args.drive_routes_path,
                limit=args.limit,
                allow_needs_review=args.allow_needs_review,
                allow_unreviewed_metadata=args.allow_unreviewed_metadata,
                queue_missing_metadata=args.queue_missing_metadata,
                write_metadata_queue_on_dry_run=args.write_metadata_queue_on_dry_run,
                scan_error_policy=args.scan_error_policy,
                scan_error_threshold=args.scan_error_threshold,
                scan_retry_count=args.scan_retry_count,
                on_dst_exists=args.on_dst_exists,
                skip_suspicious_title_check=args.skip_suspicious_title_check,
                run_id=args.run_id or None,
            )
        )
        return result.to_dict()
    raise ValueError(f"unsupported flow: {args.flow}")


def _cmd_resume(args: argparse.Namespace) -> dict[str, Any]:
    store = _store(args.windows_ops_root)
    run = store.read_run(args.run_id)
    action = args.action or None
    artifact_id = args.artifact_id or None
    if run.flow == "source_root":
        result = SourceRootWorkflowService().resume(
            SourceRootApplyConfig(
                windows_ops_root=args.windows_ops_root,
                run_id=args.run_id,
                artifact_id=artifact_id or "source_root_move_plan",
                db=args.db or None,
            ),
            action=action or "apply_source_root_move_plan",
        )
        return result.to_dict()
    if run.flow == "relocate":
        result = RelocateWorkflowService().resume(
            RelocateApplyConfig(
                windows_ops_root=args.windows_ops_root,
                run_id=args.run_id,
                artifact_id=artifact_id or "relocate_plan",
                db=args.db or None,
                on_dst_exists=args.on_dst_exists or None,
            ),
            action=action or "apply_relocate_move_plan",
        )
        return result.to_dict()
    raise ValueError(f"unsupported run flow: {run.flow}")


def _cmd_status(args: argparse.Namespace) -> dict[str, Any]:
    store = _store(args.windows_ops_root)
    if args.run_id:
        return {
            "ok": True,
            "run": _run_to_status(store.read_run(args.run_id), include_artifacts=args.include_artifacts),
        }

    runs_root = store.runs_root
    runs: list[dict[str, Any]] = []
    full_runs: list[dict[str, Any]] = []
    if runs_root.exists():
        manifests = sorted(runs_root.glob("*/run.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        for manifest in manifests[: args.limit]:
            run = store.read_run(manifest.parent.name)
            full_runs.append(run.to_dict())
            runs.append(_run_to_status(run, include_artifacts=args.include_artifacts))
    open_gates = []
    latest_artifacts = []
    for run in full_runs:
        review_gates = run.get("reviewGates") if isinstance(run.get("reviewGates"), dict) else {}
        for gate in review_gates.values():
            if isinstance(gate, dict) and gate.get("status") == "open":
                open_gates.append({"runId": run.get("runId"), **gate})
        artifacts = run.get("artifacts") if isinstance(run.get("artifacts"), dict) else {}
        for artifact in artifacts.values():
            if isinstance(artifact, dict):
                latest_artifacts.append({"runId": run.get("runId"), **artifact})
    return {"ok": True, "runs": runs, "openGates": open_gates, "latestArtifacts": latest_artifacts[: args.limit]}


def _cmd_inspect_artifact(args: argparse.Namespace) -> dict[str, Any]:
    store = _store(args.windows_ops_root)
    run = store.read_run(args.run_id)
    artifact = run.artifacts.get(args.artifact_id)
    if artifact is None:
        return {"ok": False, "error": f"artifact not found: {args.artifact_id}", "runId": args.run_id}
    payload = artifact.to_dict()
    path = Path(artifact.path)
    related_gates = [
        gate.to_dict()
        for gate in run.review_gates.values()
        if args.artifact_id in gate.artifact_ids
    ]
    out: dict[str, Any] = {
        "ok": True,
        "runId": args.run_id,
        "artifact": payload,
        "exists": path.exists(),
        "relatedGates": related_gates,
    }
    if args.include_content_preview and path.exists() and path.is_file():
        with path.open("r", encoding="utf-8", errors="replace") as f:
            out["contentPreview"] = f.read(args.preview_bytes)
    return out


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    start = sub.add_parser("start")
    start.add_argument("--flow", choices=["source_root", "relocate"], required=True)
    start.add_argument("--windows-ops-root", required=True)
    start.add_argument("--source-root", default="")
    start.add_argument("--dest-root", required=True)
    start.add_argument("--db", required=True)
    start.add_argument("--drive-routes-path", default="")
    start.add_argument("--run-id", default="")
    start.add_argument("--max-files-per-run", type=int, default=200)
    start.add_argument("--allow-needs-review", action="store_true")
    start.add_argument("--roots-json", default="[]")
    start.add_argument("--roots-file-path", default="")
    start.add_argument("--extensions-json", default="[]")
    start.add_argument("--limit", type=int, default=0)
    start.add_argument("--allow-unreviewed-metadata", action="store_true")
    start.add_argument("--queue-missing-metadata", action="store_true")
    start.add_argument("--write-metadata-queue-on-dry-run", action="store_true")
    start.add_argument("--scan-error-policy", choices=["warn", "fail", "threshold"], default="warn")
    start.add_argument("--scan-error-threshold", type=int, default=0)
    start.add_argument("--scan-retry-count", type=int, default=1)
    start.add_argument("--on-dst-exists", choices=["error", "rename_suffix"], default="error")
    start.add_argument("--skip-suspicious-title-check", action="store_true")

    resume = sub.add_parser("resume")
    resume.add_argument("--windows-ops-root", required=True)
    resume.add_argument("--db", default="")
    resume.add_argument("--run-id", required=True)
    resume.add_argument("--action", default="")
    resume.add_argument("--artifact-id", default="")
    resume.add_argument("--on-dst-exists", choices=["error", "rename_suffix"], default="")

    status = sub.add_parser("status")
    status.add_argument("--windows-ops-root", required=True)
    status.add_argument("--run-id", default="")
    status.add_argument("--limit", type=int, default=10)
    status.add_argument("--include-artifacts", action="store_true")

    inspect = sub.add_parser("inspect-artifact")
    inspect.add_argument("--windows-ops-root", required=True)
    inspect.add_argument("--run-id", required=True)
    inspect.add_argument("--artifact-id", required=True)
    inspect.add_argument("--include-content-preview", action="store_true")
    inspect.add_argument("--preview-bytes", type=int, default=4096)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        if args.command == "start":
            _json_out(_cmd_start(args))
        elif args.command == "resume":
            _json_out(_cmd_resume(args))
        elif args.command == "status":
            _json_out(_cmd_status(args))
        elif args.command == "inspect-artifact":
            _json_out(_cmd_inspect_artifact(args))
        else:
            parser.error(f"unsupported command: {args.command}")
    except Exception as exc:
        _json_out({"ok": False, "error": str(exc), "exceptionType": type(exc).__name__})
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
