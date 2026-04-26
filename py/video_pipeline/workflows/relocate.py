"""Relocate V2 workflow service."""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from video_pipeline.domain.move_apply_stats import aggregate_move_apply
from video_pipeline.platform.pathscan_common import (
    canonicalize_windows_path,
    iter_jsonl,
    windows_to_wsl_path,
    wsl_to_windows_path,
)

from .models import (
    ArtifactRef,
    ArtifactStatus,
    Diagnostic,
    DiagnosticSeverity,
    NextAction,
    ReviewGateStatus,
    WorkflowFlow,
    WorkflowPhase,
    WorkflowResult,
)
from .source_root import parse_last_json_object_line, path_for_powershell, run_py_uv, write_json
from .store import WorkflowStore, sha256_file

PythonRunner = Callable[[Path, list[str], str | None], str]
PowerShellRunner = Callable[[str, list[str]], dict[str, Any]]


class RelocateApplyRejected(Exception):
    def __init__(self, diagnostic: Diagnostic) -> None:
        self.diagnostic = diagnostic
        super().__init__(diagnostic.message)


@dataclass
class RelocateDryRunConfig:
    windows_ops_root: str
    dest_root: str
    db: str
    roots: list[str] | None = None
    roots_file_path: str = ""
    extensions: list[str] | None = None
    drive_routes: str = ""
    limit: int = 0
    allow_needs_review: bool = False
    allow_unreviewed_metadata: bool = False
    queue_missing_metadata: bool = True
    write_metadata_queue_on_dry_run: bool = True
    scan_error_policy: str = "warn"
    scan_error_threshold: int = 0
    scan_retry_count: int = 1
    on_dst_exists: str = "error"
    skip_suspicious_title_check: bool = False
    run_id: str | None = None


@dataclass
class RelocateApplyConfig:
    windows_ops_root: str
    run_id: str
    artifact_id: str = "relocate_plan"
    db: str | None = None
    on_dst_exists: str | None = None


def local_path_from_any(path_str: str) -> Path:
    return Path(windows_to_wsl_path(str(path_str))).resolve()


def read_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for rec in iter_jsonl(str(path)):
        if isinstance(rec, dict):
            rows.append(rec)
    return rows


def next_artifact_id(run: Any, base_id: str) -> str:
    if base_id not in run.artifacts:
        return base_id
    idx = 2
    while f"{base_id}_{idx:04d}" in run.artifacts:
        idx += 1
    return f"{base_id}_{idx:04d}"


def next_gate_id(run: Any, base_id: str) -> str:
    if base_id not in run.review_gates:
        return base_id
    idx = 2
    while f"{base_id}_{idx:04d}" in run.review_gates:
        idx += 1
    return f"{base_id}_{idx:04d}"


class RelocateWorkflowService:
    def __init__(
        self,
        *,
        python_runner: PythonRunner = run_py_uv,
        powershell_runner: PowerShellRunner | None = None,
        py_root: Path | None = None,
    ) -> None:
        self.python_runner = python_runner
        self.powershell_runner = powershell_runner
        self.py_root = py_root or Path(__file__).resolve().parents[2]

    def dry_run(self, config: RelocateDryRunConfig) -> WorkflowResult:
        store = WorkflowStore(local_path_from_any(config.windows_ops_root))
        db_path = str(local_path_from_any(config.db))
        run = store.init_run(
            WorkflowFlow.RELOCATE,
            run_id=config.run_id,
            config_snapshot={
                "windowsOpsRoot": config.windows_ops_root,
                "destRoot": config.dest_root,
                "db": db_path,
                "roots": list(config.roots or []),
                "rootsFilePath": config.roots_file_path,
                "extensions": list(config.extensions or []),
                "driveRoutes": config.drive_routes,
                "limit": int(config.limit),
                "allowNeedsReview": bool(config.allow_needs_review),
                "allowUnreviewedMetadata": bool(config.allow_unreviewed_metadata),
                "queueMissingMetadata": bool(config.queue_missing_metadata),
                "writeMetadataQueueOnDryRun": bool(config.write_metadata_queue_on_dry_run),
                "scanErrorPolicy": config.scan_error_policy,
                "scanErrorThreshold": int(config.scan_error_threshold),
                "scanRetryCount": int(config.scan_retry_count),
                "onDstExists": config.on_dst_exists,
                "skipSuspiciousTitleCheck": bool(config.skip_suspicious_title_check),
            },
        )
        try:
            return self._dry_run_existing(run.run_id, config, store, db_path)
        except Exception as exc:
            diagnostic = Diagnostic(
                code="relocate_dry_run_failed",
                severity=DiagnosticSeverity.ERROR,
                message=str(exc),
                details={"exceptionType": type(exc).__name__},
            )
            self._record_failure(store, run.run_id, diagnostic)
            failed_run = store.read_run(run.run_id)
            return self._result(
                ok=False,
                store=store,
                run_id=run.run_id,
                phase=failed_run.phase,
                outcome="relocate_dry_run_failed",
            )

    def resume(self, config: RelocateApplyConfig, *, action: str = "apply_relocate_move_plan") -> WorkflowResult:
        store = WorkflowStore(local_path_from_any(config.windows_ops_root))
        if action in {"prepare_relocate_metadata", "review_relocate_metadata"}:
            return self._resume_metadata_action(store, config.run_id, action)
        if action != "apply_relocate_move_plan":
            diagnostic = Diagnostic(
                code="relocate_resume_action_unsupported",
                severity=DiagnosticSeverity.ERROR,
                message=f"unsupported relocate resume action: {action}",
                details={"action": action},
            )
            return self._block_apply(store, config.run_id, "relocate_resume_action_unsupported", diagnostic)
        return self.apply(config)

    def apply(self, config: RelocateApplyConfig) -> WorkflowResult:
        store = WorkflowStore(local_path_from_any(config.windows_ops_root))
        try:
            plan_artifact = self._validate_apply_plan(store, config.run_id, config.artifact_id)
        except RelocateApplyRejected as exc:
            return self._block_apply(store, config.run_id, "relocate_apply_rejected", exc.diagnostic)
        except Exception as exc:
            diagnostic = Diagnostic(
                code="relocate_apply_rejected",
                severity=DiagnosticSeverity.ERROR,
                message=str(exc),
                details={"exceptionType": type(exc).__name__, "artifactId": config.artifact_id},
            )
            return self._block_apply(store, config.run_id, "relocate_apply_rejected", diagnostic)

        run = store.read_run(config.run_id)
        db_path = str(local_path_from_any(config.db or str(run.config_snapshot.get("db") or "")))
        run_dir = store.run_dir(config.run_id)
        apply_dir = run_dir / "apply"
        logs_dir = run_dir / "logs"
        ops_root_win = canonicalize_windows_path(str(local_path_from_any(config.windows_ops_root)))
        scripts_root_win = canonicalize_windows_path(str(local_path_from_any(config.windows_ops_root) / "scripts"))

        if self.powershell_runner is None:
            from video_pipeline.platform.windows_pwsh_bridge import run_pwsh_json

            powershell_runner = run_pwsh_json
        else:
            powershell_runner = self.powershell_runner

        try:
            backup_raw = self.python_runner(
                self.py_root / "backup_mediaops_db.py",
                ["--db", db_path, "--action", "backup", "--descriptor", "pre_relocate_apply"],
                str(self.py_root),
            )
            backup_summary = parse_last_json_object_line(backup_raw)
            if backup_summary.get("ok") is not True:
                raise RelocateApplyRejected(
                    Diagnostic(
                        code="relocate_apply_backup_failed",
                        severity=DiagnosticSeverity.ERROR,
                        message=str(backup_summary.get("error") or backup_raw or "pre-relocate DB backup failed"),
                        details={"summary": backup_summary},
                    )
                )
            backup_path_raw = str(backup_summary.get("backup_path") or "")
            backup_path = local_path_from_any(backup_path_raw) if backup_path_raw else None
            backup_artifact = None
            if backup_path and backup_path.exists():
                backup_artifact = store.register_artifact(
                    config.run_id,
                    artifact_type="relocate_db_backup",
                    path=backup_path,
                    producer="backup_mediaops_db.py",
                    artifact_id="relocate_db_backup",
                    input_artifact_ids=[plan_artifact.id],
                    metadata={"summary": backup_summary},
                )
            else:
                backup_summary_path = logs_dir / "relocate_db_backup.json"
                write_json(backup_summary_path, backup_summary)
                backup_artifact = store.register_artifact(
                    config.run_id,
                    artifact_type="relocate_db_backup",
                    path=backup_summary_path,
                    producer="backup_mediaops_db.py",
                    artifact_id="relocate_db_backup",
                    input_artifact_ids=[plan_artifact.id],
                    metadata={"summary": backup_summary, "backupFileMissing": True},
                )
            try:
                self.python_runner(
                    self.py_root / "backup_mediaops_db.py",
                    ["--db", db_path, "--action", "rotate", "--keep", "10"],
                    str(self.py_root),
                )
            except Exception as exc:
                self._append_diagnostic(
                    store,
                    config.run_id,
                    Diagnostic(
                        code="relocate_apply_backup_rotate_failed",
                        severity=DiagnosticSeverity.WARNING,
                        message=str(exc),
                        details={"exceptionType": type(exc).__name__},
                    ),
                )

            internal_plan_path = apply_dir / "relocate_internal_move_plan.jsonl"
            planned_rows = [
                row
                for row in read_jsonl_rows(Path(plan_artifact.path))
                if row.get("status") == "planned" and row.get("src") and row.get("dst")
            ]
            with internal_plan_path.open("w", encoding="utf-8") as f:
                f.write(json.dumps({"_meta": {"kind": "relocate_move_plan_internal"}}, ensure_ascii=False) + "\n")
                for row in planned_rows:
                    f.write(
                        json.dumps(
                            {"path_id": row.get("path_id"), "src": row.get("src"), "dst": row.get("dst")},
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
            internal_plan_artifact = store.register_artifact(
                config.run_id,
                artifact_type="relocate_internal_move_plan",
                path=internal_plan_path,
                producer="RelocateWorkflowService.apply",
                artifact_id="relocate_internal_move_plan",
                input_artifact_ids=[plan_artifact.id, backup_artifact.id],
                metadata={"plannedRows": len(planned_rows)},
            )

            apply_meta = powershell_runner(
                scripts_root_win + r"\apply_move_plan.ps1",
                [
                    "-PlanJsonl",
                    path_for_powershell(internal_plan_path),
                    "-OpsRoot",
                    ops_root_win,
                    "-OnDstExists",
                    str(config.on_dst_exists or run.config_snapshot.get("onDstExists") or "error"),
                ],
            )
            applied_out = str(apply_meta.get("out_jsonl") or "")
            if not applied_out:
                raise RuntimeError("apply_move_plan.ps1 did not return out_jsonl")
            applied_path = local_path_from_any(applied_out)
            apply_log_artifact = store.register_artifact(
                config.run_id,
                artifact_type="relocate_move_apply_log",
                path=applied_path,
                producer="apply_move_plan.ps1",
                artifact_id="relocate_move_apply_log",
                input_artifact_ids=[internal_plan_artifact.id],
                metadata={"summary": apply_meta},
            )

            db_update_raw = self.python_runner(
                self.py_root / "update_db_paths_from_move_apply.py",
                [
                    "--db",
                    db_path,
                    "--applied",
                    str(applied_path),
                    "--run-kind",
                    "relocate",
                    "--event-kind",
                    "move",
                    "--detail-source",
                    "RelocateWorkflowService.apply",
                ],
                str(self.py_root),
            )
            db_update_summary = parse_last_json_object_line(db_update_raw)
            db_update_path = logs_dir / "relocate_db_update.json"
            write_json(db_update_path, db_update_summary or {"raw": db_update_raw})
            store.register_artifact(
                config.run_id,
                artifact_type="relocate_db_update",
                path=db_update_path,
                producer="update_db_paths_from_move_apply.py",
                artifact_id="relocate_db_update",
                input_artifact_ids=[apply_log_artifact.id],
                metadata={"summary": db_update_summary},
            )

            move_stats = aggregate_move_apply(applied_path)
            stats_path = apply_dir / "relocate_move_apply_stats.json"
            write_json(stats_path, move_stats)
            store.register_artifact(
                config.run_id,
                artifact_type="relocate_move_apply_stats",
                path=stats_path,
                producer="move_apply_stats.py",
                artifact_id="relocate_move_apply_stats",
                input_artifact_ids=[apply_log_artifact.id],
                metadata={"summary": move_stats},
            )

            if int(move_stats.get("failed") or 0) > 0:
                diagnostic = Diagnostic(
                    code="relocate_apply_move_failures",
                    severity=DiagnosticSeverity.ERROR,
                    message="one or more move operations failed during relocate apply",
                    details={"moveApplyStats": move_stats},
                )
                self._record_failure(store, config.run_id, diagnostic)
                final_run = store.read_run(config.run_id)
                return self._result(
                    ok=False,
                    store=store,
                    run_id=config.run_id,
                    phase=final_run.phase,
                    outcome="relocate_apply_failed",
                )

            store.transition_run(config.run_id, WorkflowPhase.APPLIED)
            store.transition_run(config.run_id, WorkflowPhase.COMPLETE)
            return self._result(
                ok=True,
                store=store,
                run_id=config.run_id,
                phase=WorkflowPhase.COMPLETE,
                outcome="relocate_apply_complete",
            )
        except RelocateApplyRejected as exc:
            return self._block_apply(store, config.run_id, "relocate_apply_rejected", exc.diagnostic)
        except Exception as exc:
            diagnostic = Diagnostic(
                code="relocate_apply_failed",
                severity=DiagnosticSeverity.ERROR,
                message=str(exc),
                details={"exceptionType": type(exc).__name__, "artifactId": config.artifact_id},
            )
            self._record_failure(store, config.run_id, diagnostic)
            final_run = store.read_run(config.run_id)
            return self._result(
                ok=False,
                store=store,
                run_id=config.run_id,
                phase=final_run.phase,
                outcome="relocate_apply_failed",
            )

    def _dry_run_existing(
        self,
        run_id: str,
        config: RelocateDryRunConfig,
        store: WorkflowStore,
        db_path: str,
    ) -> WorkflowResult:
        run_dir = store.run_dir(run_id)
        plan_dir = run_dir / "plan"
        metadata_dir = run_dir / "metadata"
        logs_dir = run_dir / "logs"
        run = store.read_run(run_id)
        diagnostics_id = next_artifact_id(run, "relocate_diagnostics")
        diagnostics_suffix = "" if diagnostics_id == "relocate_diagnostics" else diagnostics_id.removeprefix("relocate_diagnostics")
        summary_path = logs_dir / f"relocate_summary{diagnostics_suffix}.json"

        args = [
            "--db",
            db_path,
            "--windows-ops-root",
            config.windows_ops_root,
            "--dest-root",
            canonicalize_windows_path(config.dest_root),
            "--allow-needs-review",
            str(bool(config.allow_needs_review)).lower(),
            "--allow-unreviewed-metadata",
            str(bool(config.allow_unreviewed_metadata)).lower(),
            "--queue-missing-metadata",
            str(bool(config.queue_missing_metadata)).lower(),
            "--write-metadata-queue-on-dry-run",
            str(bool(config.write_metadata_queue_on_dry_run)).lower(),
            "--scan-error-policy",
            str(config.scan_error_policy),
            "--scan-retry-count",
            str(int(config.scan_retry_count)),
            "--on-dst-exists",
            str(config.on_dst_exists),
            "--skip-suspicious-title-check",
            str(bool(config.skip_suspicious_title_check)).lower(),
        ]
        if config.roots:
            args.extend(["--roots-json", json.dumps(list(config.roots), ensure_ascii=False)])
        elif config.roots_file_path:
            args.extend(["--roots-file-path", config.roots_file_path])
        if config.extensions:
            args.extend(["--extensions-json", json.dumps(list(config.extensions), ensure_ascii=False)])
        if config.limit:
            args.extend(["--limit", str(int(config.limit))])
        if config.scan_error_threshold:
            args.extend(["--scan-error-threshold", str(int(config.scan_error_threshold))])
        if config.drive_routes:
            args.extend(["--drive-routes", config.drive_routes])

        raw = self.python_runner(self.py_root / "relocate_existing_files.py", args, str(self.py_root))
        summary = parse_last_json_object_line(raw)
        if not summary:
            raise RuntimeError("relocate_existing_files.py did not emit a JSON summary")
        write_json(summary_path, summary)
        diagnostics_artifact = store.register_artifact(
            run_id,
            artifact_type="relocate_diagnostics",
            path=summary_path,
            producer="relocate_existing_files.py",
            artifact_id=diagnostics_id,
            metadata={"summary": summary},
        )

        plan_artifact: ArtifactRef | None = None
        plan_path_raw = summary.get("planPath")
        if isinstance(plan_path_raw, str) and plan_path_raw:
            plan_source = local_path_from_any(plan_path_raw)
            run = store.read_run(run_id)
            plan_id = next_artifact_id(run, "relocate_plan")
            plan_target = plan_dir / f"{plan_id}.jsonl"
            if plan_source.resolve() != plan_target.resolve():
                shutil.copyfile(plan_source, plan_target)
            plan_artifact = store.register_artifact(
                run_id,
                artifact_type="relocate_plan",
                path=plan_target,
                producer="relocate_existing_files.py",
                artifact_id=plan_id,
                input_artifact_ids=[diagnostics_artifact.id],
                metadata={"summary": summary},
            )

        queue_artifact: ArtifactRef | None = None
        queue_path_raw = summary.get("metadataQueuePath")
        if isinstance(queue_path_raw, str) and queue_path_raw:
            queue_source = local_path_from_any(queue_path_raw)
            run = store.read_run(run_id)
            queue_id = next_artifact_id(run, "relocate_metadata_queue")
            queue_target = metadata_dir / f"{queue_id}.jsonl"
            if queue_source.resolve() != queue_target.resolve():
                shutil.copyfile(queue_source, queue_target)
            queue_artifact = store.register_artifact(
                run_id,
                artifact_type="relocate_metadata_queue",
                path=queue_target,
                producer="relocate_existing_files.py",
                artifact_id=queue_id,
                input_artifact_ids=[diagnostics_artifact.id],
                metadata={"summary": summary},
            )

        self._append_summary_diagnostics(store, run_id, summary)

        planned_moves = int(summary.get("plannedMoves") or 0)
        already_correct = int(summary.get("alreadyCorrect") or 0)
        metadata_queue_count = int(summary.get("metadataQueuePlannedCount") or 0)
        metadata_missing = int(summary.get("metadataMissingSkipped") or 0)
        suspicious = int(summary.get("suspiciousProgramTitleSkipped") or 0)
        needs_review = int(summary.get("needsReviewSkipped") or 0)
        unreviewed = int(summary.get("unreviewedMetadataSkipped") or 0)
        has_metadata_gap = metadata_queue_count > 0 or metadata_missing > 0 or unreviewed > 0
        has_review_blocker = suspicious > 0 or needs_review > 0 or unreviewed > 0

        if planned_moves > 0 and plan_artifact is not None:
            self._approve_blocking_metadata_gates(store, run_id, "relocate_metadata_recheck_plan_ready")
            self._advance_to_phase(store, run_id, WorkflowPhase.PLAN_READY)
            return self._result(
                ok=True,
                store=store,
                run_id=run_id,
                phase=WorkflowPhase.PLAN_READY,
                outcome="relocate_plan_ready",
                next_actions=[
                    NextAction(
                        action="review_plan",
                        label="Review relocate move plan",
                        tool="video_pipeline_resume",
                        params={
                            "runId": run_id,
                            "artifactId": plan_artifact.id,
                            "resumeAction": "apply_relocate_move_plan",
                        },
                        requires_human_input=True,
                    )
                ],
            )

        if has_metadata_gap or metadata_queue_count > 0:
            artifact_ids = [queue_artifact.id] if queue_artifact is not None else [diagnostics_artifact.id]
            if has_review_blocker:
                self._ensure_review_gate(
                    store,
                    run_id,
                    gate_type="relocate_metadata_review",
                    artifact_ids=artifact_ids,
                    gate_id="relocate_metadata_review",
                )
            self._advance_to_phase(store, run_id, WorkflowPhase.REVIEW_REQUIRED)
            return self._result(
                ok=False,
                store=store,
                run_id=run_id,
                phase=WorkflowPhase.REVIEW_REQUIRED,
                outcome="relocate_metadata_preparation_required",
                next_actions=[
                    NextAction(
                        action="prepare_relocate_metadata",
                        label="Prepare missing or blocked relocate metadata",
                        tool="video_pipeline_resume",
                        params={"runId": run_id, "artifactIds": artifact_ids},
                        requires_human_input=has_review_blocker,
                    )
                ],
            )

        if has_review_blocker:
            gate = self._ensure_review_gate(
                store,
                run_id,
                gate_type="relocate_metadata_review",
                artifact_ids=[diagnostics_artifact.id],
                gate_id="relocate_metadata_review",
            )
            self._advance_to_phase(store, run_id, WorkflowPhase.REVIEW_REQUIRED)
            return self._result(
                ok=False,
                store=store,
                run_id=run_id,
                phase=WorkflowPhase.REVIEW_REQUIRED,
                outcome="relocate_review_required",
                next_actions=[
                    NextAction(
                        action="review_relocate_metadata",
                        label="Review blocked relocate metadata",
                        tool="video_pipeline_resume",
                        params={
                            "runId": run_id,
                            "gateId": gate.id,
                            "artifactIds": [diagnostics_artifact.id],
                        },
                        requires_human_input=True,
                    )
                ],
            )

        if already_correct > 0 and planned_moves == 0:
            self._approve_blocking_metadata_gates(store, run_id, "relocate_metadata_recheck_complete")
            self._advance_to_phase(store, run_id, WorkflowPhase.COMPLETE)
            return self._result(
                ok=True,
                store=store,
                run_id=run_id,
                phase=WorkflowPhase.COMPLETE,
                outcome="relocate_already_correct",
            )

        self._approve_blocking_metadata_gates(store, run_id, "relocate_metadata_recheck_complete")
        self._advance_to_phase(store, run_id, WorkflowPhase.COMPLETE)
        return self._result(
            ok=True,
            store=store,
            run_id=run_id,
            phase=WorkflowPhase.COMPLETE,
            outcome="relocate_no_action_needed",
        )

    def _validate_apply_plan(self, store: WorkflowStore, run_id: str, artifact_id: str) -> ArtifactRef:
        run = store.read_run(run_id)
        if run.flow != WorkflowFlow.RELOCATE.value:
            raise RelocateApplyRejected(Diagnostic(
                code="relocate_apply_wrong_flow",
                severity=DiagnosticSeverity.ERROR,
                message=f"run is not a relocate workflow: {run.flow}",
                details={"runId": run_id, "flow": run.flow},
            ))
        if run.phase != WorkflowPhase.PLAN_READY.value:
            raise RelocateApplyRejected(Diagnostic(
                code="relocate_apply_wrong_phase",
                severity=DiagnosticSeverity.ERROR,
                message=f"relocate apply requires phase plan_ready, got {run.phase}",
                details={"runId": run_id, "phase": run.phase},
            ))
        blocking_gates = [
            gate
            for gate in run.review_gates.values()
            if gate.requires_human_review
            and gate.status in {ReviewGateStatus.OPEN.value, ReviewGateStatus.REJECTED.value}
        ]
        if blocking_gates:
            raise RelocateApplyRejected(Diagnostic(
                code="relocate_apply_review_gate_blocked",
                severity=DiagnosticSeverity.ERROR,
                message="relocate apply is blocked by unresolved or rejected review gates",
                details={
                    "runId": run_id,
                    "blockingGateIds": [gate.id for gate in blocking_gates],
                    "blockingGateStatuses": {gate.id: gate.status for gate in blocking_gates},
                },
            ))
        plan_artifact = run.artifacts.get(artifact_id)
        if plan_artifact is None:
            raise RelocateApplyRejected(Diagnostic(
                code="relocate_apply_plan_not_in_run",
                severity=DiagnosticSeverity.ERROR,
                message=f"relocate plan artifact does not belong to run {run_id}: {artifact_id}",
                details={"runId": run_id, "artifactId": artifact_id},
            ))
        if plan_artifact.type != "relocate_plan":
            raise RelocateApplyRejected(Diagnostic(
                code="relocate_apply_invalid_artifact_type",
                severity=DiagnosticSeverity.ERROR,
                message=f"artifact is not a relocate plan: {plan_artifact.type}",
                details={"runId": run_id, "artifactId": artifact_id, "artifactType": plan_artifact.type},
            ))
        if plan_artifact.status != ArtifactStatus.AVAILABLE.value:
            raise RelocateApplyRejected(Diagnostic(
                code="relocate_apply_plan_not_available",
                severity=DiagnosticSeverity.ERROR,
                message=f"relocate plan artifact is not available: {plan_artifact.status}",
                details={"runId": run_id, "artifactId": artifact_id, "artifactStatus": plan_artifact.status},
            ))
        plan_artifact_ids = [
            aid
            for aid in run.artifact_ids
            if aid in run.artifacts and run.artifacts[aid].type == "relocate_plan"
        ]
        if not plan_artifact_ids or plan_artifact_ids[-1] != artifact_id:
            raise RelocateApplyRejected(Diagnostic(
                code="relocate_apply_plan_not_current",
                severity=DiagnosticSeverity.ERROR,
                message=f"relocate plan artifact is not current for run {run_id}: {artifact_id}",
                details={
                    "runId": run_id,
                    "artifactId": artifact_id,
                    "currentArtifactId": plan_artifact_ids[-1] if plan_artifact_ids else None,
                },
            ))
        plan_path = Path(plan_artifact.path)
        if not plan_path.exists():
            raise RelocateApplyRejected(Diagnostic(
                code="relocate_apply_plan_missing",
                severity=DiagnosticSeverity.ERROR,
                message=f"relocate plan artifact file is missing: {plan_path}",
                details={"runId": run_id, "artifactId": artifact_id, "path": str(plan_path)},
            ))
        current_sha = sha256_file(plan_path)
        if current_sha != plan_artifact.sha256:
            raise RelocateApplyRejected(Diagnostic(
                code="relocate_apply_plan_checksum_mismatch",
                severity=DiagnosticSeverity.ERROR,
                message=f"relocate plan artifact checksum changed: {plan_path}",
                details={
                    "runId": run_id,
                    "artifactId": artifact_id,
                    "path": str(plan_path),
                    "expectedSha256": plan_artifact.sha256,
                    "actualSha256": current_sha,
                },
            ))
        return plan_artifact

    def _append_summary_diagnostics(self, store: WorkflowStore, run_id: str, summary: dict[str, Any]) -> None:
        diagnostics: list[Diagnostic] = []
        if int(summary.get("suspiciousProgramTitleSkipped") or 0) > 0:
            diagnostics.append(Diagnostic(
                code="relocate_suspicious_program_titles",
                severity=DiagnosticSeverity.WARNING,
                message="some relocate candidates have suspicious program titles",
                details={"suspiciousProgramTitleSkipped": int(summary.get("suspiciousProgramTitleSkipped") or 0)},
            ))
        if int(summary.get("metadataMissingSkipped") or 0) > 0 or int(summary.get("metadataQueuePlannedCount") or 0) > 0:
            diagnostics.append(Diagnostic(
                code="relocate_metadata_preparation_required",
                severity=DiagnosticSeverity.INFO,
                message="metadata preparation is required before relocating some files",
                details={
                    "metadataMissingSkipped": int(summary.get("metadataMissingSkipped") or 0),
                    "metadataQueuePlannedCount": int(summary.get("metadataQueuePlannedCount") or 0),
                },
            ))
        errors = summary.get("errors")
        if isinstance(errors, list) and errors:
            diagnostics.append(Diagnostic(
                code="relocate_summary_errors",
                severity=DiagnosticSeverity.ERROR,
                message="relocate_existing_files.py reported errors",
                details={"errors": errors},
            ))
        for diagnostic in diagnostics:
            self._append_diagnostic(store, run_id, diagnostic)

    def _append_diagnostic(self, store: WorkflowStore, run_id: str, diagnostic: Diagnostic) -> None:
        run = store.read_run(run_id)
        run.diagnostics.append(diagnostic)
        store.write_run(run)

    def _resume_metadata_action(self, store: WorkflowStore, run_id: str, action: str) -> WorkflowResult:
        try:
            run = store.read_run(run_id)
        except Exception as exc:
            return WorkflowResult(
                ok=False,
                run_id=run_id,
                flow=WorkflowFlow.RELOCATE,
                phase=WorkflowPhase.FAILED,
                outcome="relocate_resume_action_failed",
                diagnostics=[
                    Diagnostic(
                        code="relocate_resume_run_missing",
                        severity=DiagnosticSeverity.ERROR,
                        message=str(exc),
                        details={"exceptionType": type(exc).__name__, "action": action},
                    )
                ],
            )
        if run.flow != WorkflowFlow.RELOCATE.value:
            diagnostic = Diagnostic(
                code="relocate_resume_wrong_flow",
                severity=DiagnosticSeverity.ERROR,
                message=f"run is not a relocate workflow: {run.flow}",
                details={"runId": run_id, "flow": run.flow, "action": action},
            )
            run.diagnostics.append(diagnostic)
            store.write_run(run)
            return self._result(
                ok=False,
                store=store,
                run_id=run_id,
                phase=run.phase,
                outcome="relocate_resume_action_rejected",
            )
        if run.phase != WorkflowPhase.REVIEW_REQUIRED.value:
            diagnostic = Diagnostic(
                code="relocate_resume_wrong_phase",
                severity=DiagnosticSeverity.ERROR,
                message=f"relocate metadata resume requires phase review_required, got {run.phase}",
                details={"runId": run_id, "phase": run.phase, "action": action},
            )
            run.diagnostics.append(diagnostic)
            store.write_run(run)
            return self._result(
                ok=False,
                store=store,
                run_id=run_id,
                phase=run.phase,
                outcome="relocate_resume_action_rejected",
            )

        gate_ids = [
            gid
            for gid in run.review_gate_ids
            if gid in run.review_gates and run.review_gates[gid].status == ReviewGateStatus.OPEN.value
        ]
        if action == "review_relocate_metadata":
            for gate_id in gate_ids:
                store.update_review_gate(
                    run_id,
                    gate_id,
                    status=ReviewGateStatus.APPROVED,
                    resolution={"action": action},
                )

        rerun = store.read_run(run_id)
        cfg = self._config_from_run(rerun)
        result = self._dry_run_existing(
            run_id,
            cfg,
            store,
            str(local_path_from_any(str(rerun.config_snapshot.get("db") or ""))),
        )
        if result.outcome == "relocate_metadata_preparation_required":
            result.outcome = "relocate_metadata_preparation_still_required"
        elif result.outcome == "relocate_review_required":
            result.outcome = "relocate_metadata_review_still_required"
        return result

    def _config_from_run(self, run: Any) -> RelocateDryRunConfig:
        snapshot = run.config_snapshot
        return RelocateDryRunConfig(
            windows_ops_root=str(snapshot.get("windowsOpsRoot") or ""),
            dest_root=str(snapshot.get("destRoot") or ""),
            db=str(snapshot.get("db") or ""),
            roots=[str(v) for v in snapshot.get("roots") or [] if isinstance(v, str) and v],
            roots_file_path=str(snapshot.get("rootsFilePath") or ""),
            extensions=[str(v) for v in snapshot.get("extensions") or [] if isinstance(v, str) and v],
            drive_routes=str(snapshot.get("driveRoutes") or ""),
            limit=int(snapshot.get("limit") or 0),
            allow_needs_review=bool(snapshot.get("allowNeedsReview")),
            allow_unreviewed_metadata=bool(snapshot.get("allowUnreviewedMetadata")),
            queue_missing_metadata=bool(snapshot.get("queueMissingMetadata")),
            write_metadata_queue_on_dry_run=bool(snapshot.get("writeMetadataQueueOnDryRun")),
            scan_error_policy=str(snapshot.get("scanErrorPolicy") or "warn"),
            scan_error_threshold=int(snapshot.get("scanErrorThreshold") or 0),
            scan_retry_count=int(snapshot.get("scanRetryCount") or 1),
            on_dst_exists=str(snapshot.get("onDstExists") or "error"),
            skip_suspicious_title_check=bool(snapshot.get("skipSuspiciousTitleCheck")),
            run_id=run.run_id,
        )

    def _ensure_review_gate(
        self,
        store: WorkflowStore,
        run_id: str,
        *,
        gate_type: str,
        artifact_ids: list[str],
        gate_id: str,
    ) -> Any:
        run = store.read_run(run_id)
        if gate_id in run.review_gates and run.review_gates[gate_id].status == ReviewGateStatus.OPEN.value:
            return store.update_review_gate_artifacts(run_id, gate_id, artifact_ids=artifact_ids)
        if gate_id in run.review_gates:
            gate_id = next_gate_id(run, gate_id)
        return store.create_review_gate(
            run_id,
            gate_type=gate_type,
            artifact_ids=artifact_ids,
            gate_id=gate_id,
        )

    def _approve_blocking_metadata_gates(self, store: WorkflowStore, run_id: str, action: str) -> None:
        run = store.read_run(run_id)
        for gate_id in run.review_gate_ids:
            gate = run.review_gates.get(gate_id)
            if (
                gate is not None
                and gate.type == "relocate_metadata_review"
                and gate.status in {ReviewGateStatus.OPEN.value, ReviewGateStatus.REJECTED.value}
            ):
                store.update_review_gate(
                    run_id,
                    gate_id,
                    status=ReviewGateStatus.APPROVED,
                    resolution={"action": action},
                )

    def _advance_to_phase(self, store: WorkflowStore, run_id: str, target: WorkflowPhase) -> None:
        run = store.read_run(run_id)
        if run.phase == target.value:
            return
        if target == WorkflowPhase.REVIEW_REQUIRED:
            if run.phase == WorkflowPhase.CREATED.value:
                store.transition_run(run_id, WorkflowPhase.METADATA_EXTRACTED)
                store.transition_run(run_id, WorkflowPhase.REVIEW_REQUIRED)
            elif run.phase == WorkflowPhase.METADATA_EXTRACTED.value:
                store.transition_run(run_id, WorkflowPhase.REVIEW_REQUIRED)
            return
        if target == WorkflowPhase.PLAN_READY:
            if run.phase == WorkflowPhase.REVIEW_REQUIRED.value:
                store.transition_run(run_id, WorkflowPhase.METADATA_ACCEPTED)
                store.transition_run(run_id, WorkflowPhase.PLAN_READY)
            elif run.phase == WorkflowPhase.METADATA_EXTRACTED.value:
                store.transition_run(run_id, WorkflowPhase.METADATA_ACCEPTED)
                store.transition_run(run_id, WorkflowPhase.PLAN_READY)
            elif run.phase == WorkflowPhase.CREATED.value:
                store.transition_run(run_id, WorkflowPhase.PLAN_READY)
            elif run.phase == WorkflowPhase.METADATA_ACCEPTED.value:
                store.transition_run(run_id, WorkflowPhase.PLAN_READY)
            return
        if target == WorkflowPhase.COMPLETE:
            self._advance_to_phase(store, run_id, WorkflowPhase.PLAN_READY)
            current = store.read_run(run_id)
            if current.phase == WorkflowPhase.PLAN_READY.value:
                store.transition_run(run_id, WorkflowPhase.COMPLETE)

    def _block_apply(
        self,
        store: WorkflowStore,
        run_id: str,
        outcome: str,
        diagnostic: Diagnostic,
    ) -> WorkflowResult:
        try:
            run = store.read_run(run_id)
        except Exception:
            return WorkflowResult(
                ok=False,
                run_id=run_id,
                flow=WorkflowFlow.RELOCATE,
                phase=WorkflowPhase.FAILED,
                outcome=outcome,
                diagnostics=[diagnostic],
            )
        run.diagnostics.append(diagnostic)
        store.write_run(run)
        if run.flow == WorkflowFlow.RELOCATE.value and run.phase not in {
            WorkflowPhase.BLOCKED.value,
            WorkflowPhase.COMPLETE.value,
            WorkflowPhase.FAILED.value,
        }:
            store.transition_run(run_id, WorkflowPhase.BLOCKED)
        final_run = store.read_run(run_id)
        return self._result(
            ok=False,
            store=store,
            run_id=run_id,
            phase=final_run.phase,
            outcome=outcome,
        )

    def _record_failure(self, store: WorkflowStore, run_id: str, diagnostic: Diagnostic) -> None:
        try:
            run = store.read_run(run_id)
            run.diagnostics.append(diagnostic)
            store.write_run(run)
            if run.phase != WorkflowPhase.FAILED.value:
                store.transition_run(run_id, WorkflowPhase.FAILED)
        except Exception:
            pass

    def _result(
        self,
        *,
        ok: bool,
        store: WorkflowStore,
        run_id: str,
        phase: WorkflowPhase | str,
        outcome: str,
        next_actions: list[NextAction] | None = None,
    ) -> WorkflowResult:
        run = store.read_run(run_id)
        return WorkflowResult(
            ok=ok,
            run_id=run_id,
            flow=run.flow,
            phase=phase,
            outcome=outcome,
            artifacts=[run.artifacts[aid] for aid in run.artifact_ids],
            gates=[run.review_gates[gid] for gid in run.review_gate_ids],
            next_actions=list(next_actions or []),
            diagnostics=run.diagnostics,
        )
