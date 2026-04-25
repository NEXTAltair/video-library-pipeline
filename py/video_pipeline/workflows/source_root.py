"""SourceRoot V2 workflow service skeleton."""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from video_pipeline.domain.move_apply_stats import aggregate_move_apply
from video_pipeline.platform.pathscan_common import (
    canonicalize_windows_path,
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
from .store import WorkflowStore, sha256_file

PythonRunner = Callable[[Path, list[str], str | None], str]
PowerShellRunner = Callable[[str, list[str]], dict[str, Any]]


class SourceRootApplyRejected(Exception):
    def __init__(self, diagnostic: Diagnostic) -> None:
        self.diagnostic = diagnostic
        super().__init__(diagnostic.message)


@dataclass
class SourceRootDryRunConfig:
    windows_ops_root: str
    source_root: str
    dest_root: str
    db: str
    drive_routes: str = ""
    max_files_per_run: int = 200
    allow_needs_review: bool = False
    run_id: str | None = None


@dataclass
class SourceRootApplyConfig:
    windows_ops_root: str
    run_id: str
    artifact_id: str = "source_root_move_plan"
    db: str | None = None


def run_py_uv(script: Path, args: list[str], cwd: str | None = None) -> str:
    env = dict(os.environ)
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")
    cp = subprocess.run(
        ["uv", "run", "python", str(script), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=cwd,
        env=env,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if cp.returncode != 0:
        raise RuntimeError(cp.stdout.strip() or f"python failed rc={cp.returncode}: {script}")
    return cp.stdout


def parse_last_json_object_line(output: str) -> dict[str, Any]:
    for line in reversed(str(output or "").splitlines()):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            parsed = json.loads(stripped)
        except Exception:
            continue
        if isinstance(parsed, dict):
            return parsed
    return {}


def string_array(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if isinstance(item, str) and item]


def local_path_from_any(path_str: str) -> Path:
    return Path(windows_to_wsl_path(str(path_str))).resolve()


def path_for_powershell(path: Path) -> str:
    value = str(path)
    if value.startswith("/mnt/"):
        return wsl_to_windows_path(value)
    return value


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


class SourceRootWorkflowService:
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

    def dry_run(self, config: SourceRootDryRunConfig) -> WorkflowResult:
        store = WorkflowStore(local_path_from_any(config.windows_ops_root))
        db_path = str(local_path_from_any(config.db))
        run = store.init_run(
            WorkflowFlow.SOURCE_ROOT,
            run_id=config.run_id,
            config_snapshot={
                "windowsOpsRoot": config.windows_ops_root,
                "sourceRoot": config.source_root,
                "destRoot": config.dest_root,
                "db": db_path,
                "driveRoutes": config.drive_routes or "",
                "maxFilesPerRun": int(config.max_files_per_run),
                "allowNeedsReview": bool(config.allow_needs_review),
            },
        )
        try:
            return self._dry_run_existing(run.run_id, config, store, db_path)
        except Exception as exc:
            diagnostic = Diagnostic(
                code="source_root_dry_run_failed",
                severity=DiagnosticSeverity.ERROR,
                message=str(exc),
                details={"exceptionType": type(exc).__name__},
            )
            self._record_failure(store, run.run_id, diagnostic)
            failed_run = store.read_run(run.run_id)
            return WorkflowResult(
                ok=False,
                run_id=run.run_id,
                flow=WorkflowFlow.SOURCE_ROOT,
                phase=failed_run.phase,
                outcome="source_root_dry_run_failed",
                artifacts=[failed_run.artifacts[aid] for aid in failed_run.artifact_ids],
                gates=[failed_run.review_gates[gid] for gid in failed_run.review_gate_ids],
                diagnostics=failed_run.diagnostics,
            )

    def resume(self, config: SourceRootApplyConfig, *, action: str = "apply_source_root_move_plan") -> WorkflowResult:
        if action != "apply_source_root_move_plan":
            store = WorkflowStore(local_path_from_any(config.windows_ops_root))
            diagnostic = Diagnostic(
                code="source_root_resume_action_unsupported",
                severity=DiagnosticSeverity.ERROR,
                message=f"unsupported sourceRoot resume action: {action}",
                details={"action": action},
            )
            return self._block_apply(store, config.run_id, "source_root_resume_action_unsupported", diagnostic)
        return self.apply(config)

    def apply(self, config: SourceRootApplyConfig) -> WorkflowResult:
        store = WorkflowStore(local_path_from_any(config.windows_ops_root))
        try:
            plan_artifact = self._validate_apply_plan(store, config.run_id, config.artifact_id)
        except SourceRootApplyRejected as exc:
            return self._block_apply(store, config.run_id, "source_root_apply_rejected", exc.diagnostic)
        except Exception as exc:
            diagnostic = Diagnostic(
                code="source_root_apply_rejected",
                severity=DiagnosticSeverity.ERROR,
                message=str(exc),
                details={"exceptionType": type(exc).__name__, "artifactId": config.artifact_id},
            )
            return self._block_apply(store, config.run_id, "source_root_apply_rejected", diagnostic)

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
            apply_meta = powershell_runner(
                scripts_root_win + r"\apply_move_plan.ps1",
                ["-PlanJsonl", path_for_powershell(Path(plan_artifact.path)), "-OpsRoot", ops_root_win],
            )
            applied_out = str(apply_meta.get("out_jsonl") or "")
            if not applied_out:
                raise RuntimeError("apply_move_plan.ps1 did not return out_jsonl")
            applied_path = local_path_from_any(applied_out)
            apply_log_artifact = store.register_artifact(
                config.run_id,
                artifact_type="source_root_move_apply_log",
                path=applied_path,
                producer="apply_move_plan.ps1",
                artifact_id="source_root_move_apply_log",
                input_artifact_ids=[plan_artifact.id],
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
                    "source_root_apply",
                    "--event-kind",
                    "move",
                    "--detail-source",
                    "SourceRootWorkflowService.apply",
                ],
                str(self.py_root),
            )
            db_update_summary = parse_last_json_object_line(db_update_raw)
            db_update_path = logs_dir / "source_root_db_update.json"
            write_json(db_update_path, db_update_summary or {"raw": db_update_raw})
            db_update_artifact = store.register_artifact(
                config.run_id,
                artifact_type="source_root_db_update",
                path=db_update_path,
                producer="update_db_paths_from_move_apply.py",
                artifact_id="source_root_db_update",
                input_artifact_ids=[apply_log_artifact.id],
                metadata={"summary": db_update_summary},
            )

            move_stats = aggregate_move_apply(applied_path)
            stats_path = apply_dir / "source_root_move_apply_stats.json"
            write_json(stats_path, move_stats)
            stats_artifact = store.register_artifact(
                config.run_id,
                artifact_type="source_root_move_apply_stats",
                path=stats_path,
                producer="move_apply_stats.py",
                artifact_id="source_root_move_apply_stats",
                input_artifact_ids=[apply_log_artifact.id],
                metadata={"summary": move_stats},
            )

            if int(move_stats.get("failed") or 0) > 0:
                diagnostic = Diagnostic(
                    code="source_root_apply_move_failures",
                    severity=DiagnosticSeverity.ERROR,
                    message="one or more move operations failed during sourceRoot apply",
                    details={"moveApplyStats": move_stats},
                )
                self._record_failure(store, config.run_id, diagnostic)
                final_run = store.read_run(config.run_id)
                return WorkflowResult(
                    ok=False,
                    run_id=config.run_id,
                    flow=WorkflowFlow.SOURCE_ROOT,
                    phase=final_run.phase,
                    outcome="source_root_apply_failed",
                    artifacts=[final_run.artifacts[aid] for aid in final_run.artifact_ids],
                    gates=[final_run.review_gates[gid] for gid in final_run.review_gate_ids],
                    diagnostics=final_run.diagnostics,
                )

            store.transition_run(config.run_id, WorkflowPhase.APPLIED)
            store.transition_run(config.run_id, WorkflowPhase.COMPLETE)
            final_run = store.read_run(config.run_id)
            return WorkflowResult(
                ok=True,
                run_id=config.run_id,
                flow=WorkflowFlow.SOURCE_ROOT,
                phase=WorkflowPhase.COMPLETE,
                outcome="source_root_apply_complete",
                artifacts=[final_run.artifacts[aid] for aid in final_run.artifact_ids],
                gates=[final_run.review_gates[gid] for gid in final_run.review_gate_ids],
                diagnostics=final_run.diagnostics,
            )
        except Exception as exc:
            diagnostic = Diagnostic(
                code="source_root_apply_failed",
                severity=DiagnosticSeverity.ERROR,
                message=str(exc),
                details={"exceptionType": type(exc).__name__, "artifactId": config.artifact_id},
            )
            self._record_failure(store, config.run_id, diagnostic)
            final_run = store.read_run(config.run_id)
            return WorkflowResult(
                ok=False,
                run_id=config.run_id,
                flow=WorkflowFlow.SOURCE_ROOT,
                phase=final_run.phase,
                outcome="source_root_apply_failed",
                artifacts=[final_run.artifacts[aid] for aid in final_run.artifact_ids],
                gates=[final_run.review_gates[gid] for gid in final_run.review_gate_ids],
                diagnostics=final_run.diagnostics,
            )

    def _dry_run_existing(
        self,
        run_id: str,
        config: SourceRootDryRunConfig,
        store: WorkflowStore,
        db_path: str,
    ) -> WorkflowResult:
        run_dir = store.run_dir(run_id)
        inventory_dir = run_dir / "inventory"
        metadata_dir = run_dir / "metadata"
        review_dir = run_dir / "review"
        plan_dir = run_dir / "plan"
        inventory_path = inventory_dir / "inventory_unwatched.jsonl"
        queue_path = metadata_dir / "queue_unwatched_batch.jsonl"
        plan_path = plan_dir / "move_plan_from_inventory.jsonl"

        source_root_win = canonicalize_windows_path(config.source_root)
        dest_root_win = canonicalize_windows_path(config.dest_root)
        ops_root_win = canonicalize_windows_path(config.windows_ops_root)
        scripts_root_win = canonicalize_windows_path(str(local_path_from_any(config.windows_ops_root) / "scripts"))

        if self.powershell_runner is None:
            from video_pipeline.platform.windows_pwsh_bridge import run_pwsh_json

            powershell_runner = run_pwsh_json
        else:
            powershell_runner = self.powershell_runner

        powershell_runner(
            scripts_root_win + r"\unwatched_inventory.ps1",
            [
                "-Root",
                source_root_win,
                "-OpsRoot",
                ops_root_win,
                "-OutJsonl",
                path_for_powershell(inventory_path),
                "-IncludeHash",
            ],
        )
        inventory_artifact = store.register_artifact(
            run_id,
            artifact_type="source_root_inventory",
            path=inventory_path,
            producer="unwatched_inventory.ps1",
            artifact_id="source_root_inventory",
        )

        self.python_runner(
            self.py_root / "ingest_inventory_jsonl.py",
            ["--db", db_path, "--jsonl", str(inventory_path), "--target-root", source_root_win],
            str(self.py_root),
        )
        self.python_runner(
            self.py_root / "make_metadata_queue_from_inventory.py",
            [
                "--db",
                db_path,
                "--inventory",
                str(inventory_path),
                "--source-root",
                source_root_win,
                "--out",
                str(queue_path),
                "--limit",
                str(int(config.max_files_per_run)),
            ],
            str(self.py_root),
        )
        queue_artifact = store.register_artifact(
            run_id,
            artifact_type="metadata_queue",
            path=queue_path,
            producer="make_metadata_queue_from_inventory.py",
            artifact_id="metadata_queue",
            input_artifact_ids=[inventory_artifact.id],
        )
        store.transition_run(run_id, WorkflowPhase.INVENTORY_READY)

        reextract_raw = self.python_runner(
            self.py_root / "run_metadata_batches_promptv1.py",
            [
                "--db",
                db_path,
                "--queue",
                str(queue_path),
                "--outdir",
                str(metadata_dir),
                "--hints",
                str(self.py_root.parent / "rules" / "program_aliases.yaml"),
                "--batch-size",
                "50",
                "--start-batch",
                "1",
            ],
            str(self.py_root),
        )
        reextract_summary = parse_last_json_object_line(reextract_raw)
        output_paths = string_array(reextract_summary.get("outputJsonlPaths"))
        latest_output = reextract_summary.get("latestOutputJsonlPath")
        if not output_paths and isinstance(latest_output, str) and latest_output:
            output_paths = [latest_output]

        metadata_artifacts: list[ArtifactRef] = []
        for idx, output_path in enumerate(output_paths, start=1):
            metadata_artifacts.append(
                store.register_artifact(
                    run_id,
                    artifact_type="metadata_extract_output",
                    path=output_path,
                    producer="run_metadata_batches_promptv1.py",
                    artifact_id=f"metadata_extract_output_{idx:04d}",
                    input_artifact_ids=[queue_artifact.id],
                    metadata={"summary": reextract_summary},
                )
            )
        store.transition_run(run_id, WorkflowPhase.METADATA_EXTRACTED)

        review_artifacts: list[ArtifactRef] = []
        for idx, metadata_artifact in enumerate(metadata_artifacts, start=1):
            review_output_path = review_dir / f"metadata_review_{idx:04d}.yaml"
            review_raw = self.python_runner(
                self.py_root / "export_program_yaml.py",
                [
                    "--source-jsonl",
                    metadata_artifact.path,
                    "--output",
                    str(review_output_path),
                    "--only-if-reviewable",
                ],
                str(self.py_root),
            )
            review_summary = parse_last_json_object_line(review_raw)
            if review_summary.get("ok") is not True:
                raise RuntimeError(
                    f"failed to export review YAML for {metadata_artifact.path}: {review_summary or review_raw}"
                )
            review_yaml_path = review_summary.get("outputPath")
            if not isinstance(review_yaml_path, str) or not review_yaml_path:
                continue
            review_artifacts.append(
                store.register_artifact(
                    run_id,
                    artifact_type="metadata_review_yaml",
                    path=review_yaml_path,
                    producer="export_program_yaml.py",
                    artifact_id=f"metadata_review_yaml_{idx:04d}",
                    input_artifact_ids=[metadata_artifact.id],
                    metadata={
                        "sourceJsonlPath": metadata_artifact.path,
                        "reviewSummary": review_summary.get("reviewSummary") or {},
                        "reviewCandidates": review_summary.get("reviewCandidates") or [],
                        "reviewCandidatesTruncated": bool(review_summary.get("reviewCandidatesTruncated")),
                    },
                )
            )

        if review_artifacts:
            gate = store.create_review_gate(
                run_id,
                gate_type="metadata_review",
                artifact_ids=[artifact.id for artifact in review_artifacts],
                gate_id="metadata_review",
            )
            store.transition_run(run_id, WorkflowPhase.REVIEW_REQUIRED)
            final_run = store.read_run(run_id)
            review_yaml_paths = [artifact.path for artifact in review_artifacts]
            return WorkflowResult(
                ok=False,
                run_id=run_id,
                flow=WorkflowFlow.SOURCE_ROOT,
                phase=WorkflowPhase.REVIEW_REQUIRED,
                outcome="source_root_metadata_review_required",
                artifacts=[final_run.artifacts[aid] for aid in final_run.artifact_ids],
                gates=[final_run.review_gates[gid] for gid in final_run.review_gate_ids],
                next_actions=[
                    NextAction(
                        action="review_metadata",
                        label="Review extracted metadata YAML",
                        tool="video_pipeline_resume",
                        params={
                            "runId": run_id,
                            "gateId": gate.id,
                            "artifactIds": [artifact.id for artifact in review_artifacts],
                            "reviewYamlPaths": review_yaml_paths,
                            "resumeAction": "apply_reviewed_metadata",
                        },
                        requires_human_input=True,
                    )
                ],
                diagnostics=final_run.diagnostics,
            )

        store.transition_run(run_id, WorkflowPhase.METADATA_ACCEPTED)

        plan_args = [
            "--db",
            db_path,
            "--inventory",
            str(inventory_path),
            "--source-root",
            source_root_win,
            "--dest-root",
            dest_root_win,
            "--out",
            str(plan_path),
            "--limit",
            str(int(config.max_files_per_run)),
        ]
        if config.drive_routes:
            plan_args.extend(["--drive-routes", config.drive_routes])
        if config.allow_needs_review:
            plan_args.append("--allow-needs-review")
        plan_raw = self.python_runner(self.py_root / "make_move_plan_from_inventory.py", plan_args, str(self.py_root))
        plan_summary = parse_last_json_object_line(plan_raw)
        plan_artifact = store.register_artifact(
            run_id,
            artifact_type="source_root_move_plan",
            path=plan_path,
            producer="make_move_plan_from_inventory.py",
            artifact_id="source_root_move_plan",
            input_artifact_ids=[inventory_artifact.id, queue_artifact.id, *[a.id for a in metadata_artifacts]],
            metadata={"summary": plan_summary},
        )
        store.transition_run(run_id, WorkflowPhase.PLAN_READY)

        final_run = store.read_run(run_id)
        return WorkflowResult(
            ok=True,
            run_id=run_id,
            flow=WorkflowFlow.SOURCE_ROOT,
            phase=WorkflowPhase.PLAN_READY,
            outcome="source_root_dry_run_complete",
            artifacts=[final_run.artifacts[aid] for aid in final_run.artifact_ids],
            gates=[final_run.review_gates[gid] for gid in final_run.review_gate_ids],
            next_actions=[
                NextAction(
                    action="review_plan",
                    label="Review sourceRoot move plan",
                    tool="video_pipeline_resume",
                    params={
                        "runId": run_id,
                        "artifactId": plan_artifact.id,
                        "resumeAction": "apply_source_root_move_plan",
                    },
                    requires_human_input=True,
                )
            ],
            diagnostics=final_run.diagnostics,
        )

    def _validate_apply_plan(self, store: WorkflowStore, run_id: str, artifact_id: str) -> ArtifactRef:
        run = store.read_run(run_id)
        if run.flow != WorkflowFlow.SOURCE_ROOT.value:
            raise SourceRootApplyRejected(Diagnostic(
                code="source_root_apply_wrong_flow",
                severity=DiagnosticSeverity.ERROR,
                message=f"run is not a sourceRoot workflow: {run.flow}",
                details={"runId": run_id, "flow": run.flow},
            ))
        if run.phase != WorkflowPhase.PLAN_READY.value:
            raise SourceRootApplyRejected(Diagnostic(
                code="source_root_apply_wrong_phase",
                severity=DiagnosticSeverity.ERROR,
                message=f"sourceRoot apply requires phase plan_ready, got {run.phase}",
                details={"runId": run_id, "phase": run.phase},
            ))

        blocking_gates = [
            gate
            for gate in run.review_gates.values()
            if gate.requires_human_review
            and gate.status in {ReviewGateStatus.OPEN.value, ReviewGateStatus.REJECTED.value}
        ]
        if blocking_gates:
            raise SourceRootApplyRejected(Diagnostic(
                code="source_root_apply_review_gate_blocked",
                severity=DiagnosticSeverity.ERROR,
                message="sourceRoot apply is blocked by unresolved or rejected review gates",
                details={
                    "runId": run_id,
                    "blockingGateIds": [gate.id for gate in blocking_gates],
                    "blockingGateStatuses": {gate.id: gate.status for gate in blocking_gates},
                },
            ))

        plan_artifact = run.artifacts.get(artifact_id)
        if plan_artifact is None:
            raise SourceRootApplyRejected(Diagnostic(
                code="source_root_apply_plan_not_in_run",
                severity=DiagnosticSeverity.ERROR,
                message=f"move plan artifact does not belong to run {run_id}: {artifact_id}",
                details={"runId": run_id, "artifactId": artifact_id},
            ))
        if plan_artifact.type != "source_root_move_plan":
            raise SourceRootApplyRejected(Diagnostic(
                code="source_root_apply_invalid_artifact_type",
                severity=DiagnosticSeverity.ERROR,
                message=f"artifact is not a sourceRoot move plan: {plan_artifact.type}",
                details={"runId": run_id, "artifactId": artifact_id, "artifactType": plan_artifact.type},
            ))
        if plan_artifact.status != ArtifactStatus.AVAILABLE.value:
            raise SourceRootApplyRejected(Diagnostic(
                code="source_root_apply_plan_not_available",
                severity=DiagnosticSeverity.ERROR,
                message=f"move plan artifact is not available: {plan_artifact.status}",
                details={"runId": run_id, "artifactId": artifact_id, "artifactStatus": plan_artifact.status},
            ))

        plan_artifact_ids = [
            aid
            for aid in run.artifact_ids
            if aid in run.artifacts and run.artifacts[aid].type == "source_root_move_plan"
        ]
        if not plan_artifact_ids or plan_artifact_ids[-1] != artifact_id:
            raise SourceRootApplyRejected(Diagnostic(
                code="source_root_apply_plan_not_current",
                severity=DiagnosticSeverity.ERROR,
                message=f"move plan artifact is not current for run {run_id}: {artifact_id}",
                details={
                    "runId": run_id,
                    "artifactId": artifact_id,
                    "currentArtifactId": plan_artifact_ids[-1] if plan_artifact_ids else None,
                },
            ))

        plan_path = Path(plan_artifact.path)
        if not plan_path.exists():
            raise SourceRootApplyRejected(Diagnostic(
                code="source_root_apply_plan_missing",
                severity=DiagnosticSeverity.ERROR,
                message=f"move plan artifact file is missing: {plan_path}",
                details={"runId": run_id, "artifactId": artifact_id, "path": str(plan_path)},
            ))
        current_sha = sha256_file(plan_path)
        if current_sha != plan_artifact.sha256:
            raise SourceRootApplyRejected(Diagnostic(
                code="source_root_apply_plan_checksum_mismatch",
                severity=DiagnosticSeverity.ERROR,
                message=f"move plan artifact checksum changed: {plan_path}",
                details={
                    "runId": run_id,
                    "artifactId": artifact_id,
                    "path": str(plan_path),
                    "expectedSha256": plan_artifact.sha256,
                    "actualSha256": current_sha,
                },
            ))
        return plan_artifact

    def _block_apply(
        self,
        store: WorkflowStore,
        run_id: str,
        outcome: str,
        diagnostic: Diagnostic,
    ) -> WorkflowResult:
        run = store.read_run(run_id)
        run.diagnostics.append(diagnostic)
        store.write_run(run)
        if run.phase != WorkflowPhase.BLOCKED.value:
            store.transition_run(run_id, WorkflowPhase.BLOCKED)
        final_run = store.read_run(run_id)
        return WorkflowResult(
            ok=False,
            run_id=run_id,
            flow=WorkflowFlow.SOURCE_ROOT,
            phase=final_run.phase,
            outcome=outcome,
            artifacts=[final_run.artifacts[aid] for aid in final_run.artifact_ids],
            gates=[final_run.review_gates[gid] for gid in final_run.review_gate_ids],
            diagnostics=final_run.diagnostics,
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
