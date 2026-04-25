"""SourceRoot V2 workflow service skeleton."""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from video_pipeline.platform.pathscan_common import (
    canonicalize_windows_path,
    windows_to_wsl_path,
    wsl_to_windows_path,
)

from .models import (
    ArtifactRef,
    Diagnostic,
    DiagnosticSeverity,
    NextAction,
    WorkflowFlow,
    WorkflowPhase,
    WorkflowResult,
)
from .store import WorkflowStore

PythonRunner = Callable[[Path, list[str], str | None], str]
PowerShellRunner = Callable[[str, list[str]], dict[str, Any]]


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
                    params={"runId": run_id, "artifactId": plan_artifact.id},
                    requires_human_input=True,
                )
            ],
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
