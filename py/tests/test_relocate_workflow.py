import json
from pathlib import Path

from video_pipeline.workflows import (
    RelocateApplyConfig,
    RelocateDryRunConfig,
    RelocateWorkflowService,
    ReviewGateStatus,
    WorkflowFlow,
    WorkflowPhase,
    WorkflowStore,
)


def make_config(tmp_path: Path, *, run_id: str = "run_relocate", scenario: str = "plan_ready") -> RelocateDryRunConfig:
    ops_root = tmp_path / "ops"
    (ops_root / "scripts").mkdir(parents=True)
    db_path = ops_root / "db" / "mediaops.sqlite"
    db_path.parent.mkdir(parents=True)
    db_path.write_text("db", encoding="utf-8")
    return RelocateDryRunConfig(
        windows_ops_root=str(ops_root),
        dest_root=r"B:\VideoLibrary",
        db=str(db_path),
        roots=[r"B:\VideoLibrary"],
        extensions=[".mp4"],
        run_id=run_id,
        limit=20,
        queue_missing_metadata=True,
        write_metadata_queue_on_dry_run=True,
    )


class RelocateWorkflowHarness:
    def __init__(self, tmp_path: Path, *, run_id: str, scenario: str = "plan_ready") -> None:
        self.tmp_path = tmp_path
        self.cfg = make_config(tmp_path, run_id=run_id, scenario=scenario)
        self.scenario = scenario
        self.calls: list[tuple[str, list[str]]] = []

    def python_runner(self, script: Path, args: list[str], _cwd: str | None = None) -> str:
        self.calls.append((script.name, list(args)))
        if script.name == "relocate_existing_files.py":
            ops_root = Path(args[args.index("--windows-ops-root") + 1])
            move_dir = ops_root / "move"
            llm_dir = ops_root / "llm"
            move_dir.mkdir(parents=True, exist_ok=True)
            llm_dir.mkdir(parents=True, exist_ok=True)
            plan_path = move_dir / f"{self.scenario}_relocate_plan.jsonl"
            queue_path = llm_dir / f"{self.scenario}_relocate_queue.jsonl"

            summary = {
                "ok": True,
                "tool": "video_pipeline_relocate_existing_files",
                "apply": False,
                "planPath": str(plan_path),
                "metadataQueuePath": None,
                "plannedMoves": 0,
                "alreadyCorrect": 0,
                "metadataMissingSkipped": 0,
                "metadataQueuePlannedCount": 0,
                "suspiciousProgramTitleSkipped": 0,
                "needsReviewSkipped": 0,
                "unreviewedMetadataSkipped": 0,
                "outcomeType": "no_action_needed",
                "errors": [],
            }
            rows = [{"_meta": {"kind": "relocate_plan"}}]
            if self.scenario == "plan_ready":
                summary["plannedMoves"] = 1
                summary["outcomeType"] = "plan_ready_for_apply"
                rows.append(
                    {
                        "path_id": "p1",
                        "src": r"B:\VideoLibrary\Old\show.mp4",
                        "dst": r"B:\VideoLibrary\Show\show.mp4",
                        "status": "planned",
                        "reason": "recompute_destination",
                    }
                )
            elif self.scenario == "metadata_gap":
                summary["metadataQueuePath"] = str(queue_path)
                summary["metadataMissingSkipped"] = 1
                summary["metadataQueuePlannedCount"] = 1
                summary["requiresMetadataPreparation"] = True
                summary["outcomeType"] = "metadata_preparation_required"
                rows.append({"path_id": "p1", "src": r"B:\VideoLibrary\Unknown\show.mp4", "status": "skipped", "reason": "missing_metadata"})
                queue_path.write_text(
                    json.dumps({"_meta": {"kind": "relocate_metadata_queue"}}, ensure_ascii=False)
                    + "\n"
                    + json.dumps({"path_id": "p1", "path": r"B:\VideoLibrary\Unknown\show.mp4"}, ensure_ascii=False)
                    + "\n",
                    encoding="utf-8",
                )
            elif self.scenario == "suspicious":
                summary["suspiciousProgramTitleSkipped"] = 1
                summary["outcomeType"] = "metadata_preparation_required"
                rows.append(
                    {
                        "path_id": "p1",
                        "src": r"B:\VideoLibrary\Show\show.mp4",
                        "status": "skipped",
                        "reason": "suspicious_program_title",
                        "program_title": "Show▽Episode",
                    }
                )
            elif self.scenario == "already_correct":
                summary["alreadyCorrect"] = 1
                summary["outcomeType"] = "already_correct_or_no_action_needed"
                rows.append(
                    {
                        "path_id": "p1",
                        "src": r"B:\VideoLibrary\Show\show.mp4",
                        "dst": r"B:\VideoLibrary\Show\show.mp4",
                        "status": "skipped",
                        "reason": "already_correct",
                    }
                )

            plan_path.write_text(
                "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n",
                encoding="utf-8",
            )
            return json.dumps(summary, ensure_ascii=False)
        return json.dumps({"ok": True}, ensure_ascii=False)

    def dry_run(self):
        service = RelocateWorkflowService(python_runner=self.python_runner, py_root=self.tmp_path)
        return service.dry_run(self.cfg)

    def manifest(self) -> dict:
        manifest_path = Path(self.cfg.windows_ops_root) / "runs" / self.cfg.run_id / "run.json"
        return json.loads(manifest_path.read_text(encoding="utf-8"))


def assert_json_shape(payload: dict) -> None:
    json.dumps(payload, ensure_ascii=False)
    assert {"ok", "runId", "flow", "phase", "outcome", "artifacts", "gates", "nextActions", "diagnostics"} <= set(payload)
    for artifact in payload["artifacts"]:
        assert {"id", "type", "path", "sha256", "createdAt", "producer", "status", "inputArtifactIds", "metadata"} <= set(artifact)


def test_relocate_dry_run_plan_ready_registers_plan_and_next_action(tmp_path) -> None:
    harness = RelocateWorkflowHarness(tmp_path, run_id="run_relocate_plan", scenario="plan_ready")

    payload = harness.dry_run().to_dict()

    assert_json_shape(payload)
    assert payload["ok"] is True
    assert payload["flow"] == "relocate"
    assert payload["phase"] == "plan_ready"
    assert payload["outcome"] == "relocate_plan_ready"
    assert [artifact["id"] for artifact in payload["artifacts"]] == ["relocate_diagnostics", "relocate_plan"]
    assert payload["nextActions"] == [
        {
            "action": "review_plan",
            "label": "Review relocate move plan",
            "tool": "video_pipeline_resume",
            "params": {
                "runId": "run_relocate_plan",
                "artifactId": "relocate_plan",
                "resumeAction": "apply_relocate_move_plan",
            },
            "requiresHumanInput": True,
        }
    ]

    manifest = harness.manifest()
    assert manifest["artifacts"]["relocate_plan"]["inputArtifactIds"] == ["relocate_diagnostics"]


def test_relocate_dry_run_metadata_gap_registers_queue_without_already_correct(tmp_path) -> None:
    harness = RelocateWorkflowHarness(tmp_path, run_id="run_relocate_gap", scenario="metadata_gap")

    payload = harness.dry_run().to_dict()

    assert payload["ok"] is False
    assert payload["phase"] == "review_required"
    assert payload["outcome"] == "relocate_metadata_preparation_required"
    assert [artifact["id"] for artifact in payload["artifacts"]] == [
        "relocate_diagnostics",
        "relocate_plan",
        "relocate_metadata_queue",
    ]
    assert payload["outcome"] != "relocate_already_correct"
    assert payload["diagnostics"][-1]["code"] == "relocate_metadata_preparation_required"
    assert payload["nextActions"][0]["action"] == "prepare_relocate_metadata"


def test_relocate_dry_run_suspicious_title_creates_review_gate(tmp_path) -> None:
    harness = RelocateWorkflowHarness(tmp_path, run_id="run_relocate_suspicious", scenario="suspicious")

    payload = harness.dry_run().to_dict()

    assert payload["ok"] is False
    assert payload["phase"] == "review_required"
    assert payload["outcome"] == "relocate_review_required"
    assert payload["diagnostics"][-1]["code"] == "relocate_suspicious_program_titles"
    assert payload["gates"][0]["id"] == "relocate_metadata_review"
    assert payload["gates"][0]["artifactIds"] == ["relocate_diagnostics"]


def test_relocate_metadata_resume_actions_do_not_block_review_required_run(tmp_path) -> None:
    harness = RelocateWorkflowHarness(tmp_path, run_id="run_relocate_resume_gap", scenario="metadata_gap")
    harness.dry_run()
    service = RelocateWorkflowService(py_root=tmp_path)

    result = service.resume(
        RelocateApplyConfig(windows_ops_root=harness.cfg.windows_ops_root, run_id=harness.cfg.run_id),
        action="prepare_relocate_metadata",
    )

    payload = result.to_dict()
    assert payload["ok"] is False
    assert payload["phase"] == "review_required"
    assert payload["outcome"] == "relocate_metadata_preparation_pending"
    assert payload["nextActions"][0]["action"] == "prepare_relocate_metadata"
    manifest = harness.manifest()
    assert manifest["phase"] == "review_required"
    assert payload["diagnostics"][-1]["code"] == "relocate_metadata_preparation_required"


def test_relocate_review_resume_action_keeps_gate_open_without_blocking(tmp_path) -> None:
    harness = RelocateWorkflowHarness(tmp_path, run_id="run_relocate_resume_review", scenario="suspicious")
    harness.dry_run()
    service = RelocateWorkflowService(py_root=tmp_path)

    result = service.resume(
        RelocateApplyConfig(windows_ops_root=harness.cfg.windows_ops_root, run_id=harness.cfg.run_id),
        action="review_relocate_metadata",
    )

    payload = result.to_dict()
    assert payload["ok"] is False
    assert payload["phase"] == "review_required"
    assert payload["outcome"] == "relocate_metadata_review_pending"
    assert payload["nextActions"][0]["action"] == "review_relocate_metadata"
    assert payload["nextActions"][0]["params"]["gateId"] == "relocate_metadata_review"
    manifest = harness.manifest()
    assert manifest["phase"] == "review_required"
    assert manifest["reviewGates"]["relocate_metadata_review"]["status"] == "open"


def test_relocate_dry_run_already_correct_requires_explicit_count(tmp_path) -> None:
    harness = RelocateWorkflowHarness(tmp_path, run_id="run_relocate_already", scenario="already_correct")

    payload = harness.dry_run().to_dict()

    assert payload["ok"] is True
    assert payload["phase"] == "complete"
    assert payload["outcome"] == "relocate_already_correct"


def register_relocate_plan_ready_run(
    tmp_path: Path,
    run_id: str = "run_relocate_apply",
    *,
    on_dst_exists: str = "error",
) -> tuple[RelocateApplyConfig, Path]:
    ops_root = tmp_path / "ops"
    db_path = ops_root / "db" / "mediaops.sqlite"
    db_path.parent.mkdir(parents=True)
    db_path.write_text("db", encoding="utf-8")
    (ops_root / "scripts").mkdir(parents=True)
    store = WorkflowStore(ops_root)
    store.init_run(
        WorkflowFlow.RELOCATE,
        run_id=run_id,
        config_snapshot={"windowsOpsRoot": str(ops_root), "db": str(db_path), "onDstExists": on_dst_exists},
    )
    plan_path = ops_root / "runs" / run_id / "plan" / "relocate_plan.jsonl"
    plan_path.write_text(
        "\n".join([
            json.dumps({"_meta": {"kind": "relocate_plan"}}, ensure_ascii=False),
            json.dumps(
                {
                    "path_id": "p1",
                    "src": r"B:\VideoLibrary\Old\show.mp4",
                    "dst": r"B:\VideoLibrary\Show\show.mp4",
                    "status": "planned",
                    "reason": "recompute_destination",
                },
                ensure_ascii=False,
            ),
        ])
        + "\n",
        encoding="utf-8",
    )
    store.register_artifact(
        run_id,
        artifact_type="relocate_plan",
        path=plan_path,
        producer="relocate_existing_files.py",
        artifact_id="relocate_plan",
    )
    store.transition_run(run_id, WorkflowPhase.PLAN_READY)
    return RelocateApplyConfig(windows_ops_root=str(ops_root), run_id=run_id), plan_path


def test_relocate_apply_rejects_cross_run_plan(tmp_path) -> None:
    ops_root = tmp_path / "ops"
    store = WorkflowStore(ops_root)
    store.init_run(WorkflowFlow.RELOCATE, run_id="run_a", config_snapshot={"db": str(ops_root / "db.sqlite")})
    foreign_plan = ops_root / "runs" / "run_a" / "plan" / "foreign.jsonl"
    foreign_plan.write_text("{}\n", encoding="utf-8")
    store.register_artifact("run_a", artifact_type="relocate_plan", path=foreign_plan, producer="test", artifact_id="foreign_plan")
    store.init_run(WorkflowFlow.RELOCATE, run_id="run_b", config_snapshot={"db": str(ops_root / "db.sqlite")})
    store.transition_run("run_b", WorkflowPhase.PLAN_READY)

    service = RelocateWorkflowService(py_root=tmp_path)
    result = service.apply(RelocateApplyConfig(windows_ops_root=str(ops_root), run_id="run_b", artifact_id="foreign_plan"))

    payload = result.to_dict()
    assert payload["ok"] is False
    assert payload["phase"] == "blocked"
    assert payload["diagnostics"][-1]["code"] == "relocate_apply_plan_not_in_run"


def test_relocate_apply_rejects_open_review_gate(tmp_path) -> None:
    cfg, _plan_path = register_relocate_plan_ready_run(tmp_path, run_id="run_relocate_gate")
    store = WorkflowStore(Path(cfg.windows_ops_root))
    store.create_review_gate(
        cfg.run_id,
        gate_type="relocate_metadata_review",
        artifact_ids=["relocate_diagnostics"],
        gate_id="relocate_metadata_review",
    )

    service = RelocateWorkflowService(py_root=tmp_path)
    result = service.apply(cfg)

    payload = result.to_dict()
    assert payload["ok"] is False
    assert payload["phase"] == "blocked"
    assert payload["diagnostics"][-1]["code"] == "relocate_apply_review_gate_blocked"


def test_relocate_apply_rejects_checksum_mismatch(tmp_path) -> None:
    cfg, plan_path = register_relocate_plan_ready_run(tmp_path, run_id="run_relocate_checksum")
    plan_path.write_text("{}\n", encoding="utf-8")

    service = RelocateWorkflowService(py_root=tmp_path)
    result = service.apply(cfg)

    payload = result.to_dict()
    assert payload["ok"] is False
    assert payload["phase"] == "blocked"
    assert payload["diagnostics"][-1]["code"] == "relocate_apply_plan_checksum_mismatch"


def test_relocate_apply_rejects_other_flow_without_blocking_run(tmp_path) -> None:
    ops_root = tmp_path / "ops"
    store = WorkflowStore(ops_root)
    store.init_run(WorkflowFlow.SOURCE_ROOT, run_id="run_source_root", config_snapshot={"db": str(ops_root / "db.sqlite")})
    store.transition_run("run_source_root", WorkflowPhase.PLAN_READY)

    service = RelocateWorkflowService(py_root=tmp_path)
    result = service.apply(RelocateApplyConfig(windows_ops_root=str(ops_root), run_id="run_source_root"))

    payload = result.to_dict()
    assert payload["ok"] is False
    assert payload["flow"] == "source_root"
    assert payload["phase"] == "plan_ready"
    assert payload["diagnostics"][-1]["code"] == "relocate_apply_wrong_flow"


def test_relocate_apply_registers_artifacts_after_backup_and_completes(tmp_path) -> None:
    cfg, _plan_path = register_relocate_plan_ready_run(tmp_path, run_id="run_relocate_apply_success")
    calls: list[tuple[str, list[str]]] = []

    def fake_python_runner(script: Path, args: list[str], _cwd: str | None = None) -> str:
        calls.append((script.name, list(args)))
        if script.name == "backup_mediaops_db.py" and args[args.index("--action") + 1] == "backup":
            backup_path = Path(cfg.windows_ops_root) / "db" / "mediaops.sqlite.bak_20260426_pre_relocate_apply"
            backup_path.write_text("db backup", encoding="utf-8")
            return json.dumps({"ok": True, "action": "backup", "backup_path": str(backup_path), "error": ""}, ensure_ascii=False)
        if script.name == "backup_mediaops_db.py" and args[args.index("--action") + 1] == "rotate":
            return json.dumps({"ok": True, "action": "rotate", "deleted_count": 0}, ensure_ascii=False)
        if script.name == "update_db_paths_from_move_apply.py":
            return json.dumps({"updated": 1, "events": 1, "run_kind": "relocate"}, ensure_ascii=False)
        return json.dumps({"ok": True}, ensure_ascii=False)

    def fake_powershell_runner(script: str, args: list[str]) -> dict:
        assert script.endswith(r"\apply_move_plan.ps1")
        internal_plan = Path(args[args.index("-PlanJsonl") + 1])
        assert internal_plan.read_text(encoding="utf-8").count('"status"') == 0
        out_path = Path(cfg.windows_ops_root) / "move" / "relocate_apply.jsonl"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            "\n".join([
                json.dumps({"_meta": {"kind": "move_apply"}}, ensure_ascii=False),
                json.dumps(
                    {
                        "op": "move",
                        "ts": "2026-04-26T00:00:00Z",
                        "path_id": "p1",
                        "src": r"B:\VideoLibrary\Old\show.mp4",
                        "dst": r"B:\VideoLibrary\Show\show.mp4",
                        "ok": True,
                    },
                    ensure_ascii=False,
                ),
            ])
            + "\n",
            encoding="utf-8",
        )
        return {"out_jsonl": str(out_path), "run_id": "apply_test"}

    service = RelocateWorkflowService(
        python_runner=fake_python_runner,
        powershell_runner=fake_powershell_runner,
        py_root=tmp_path,
    )

    result = service.resume(cfg, action="apply_relocate_move_plan")

    payload = result.to_dict()
    assert payload["ok"] is True
    assert payload["phase"] == "complete"
    assert payload["outcome"] == "relocate_apply_complete"
    assert [name for name, _args in calls] == [
        "backup_mediaops_db.py",
        "backup_mediaops_db.py",
        "update_db_paths_from_move_apply.py",
    ]
    manifest = json.loads((Path(cfg.windows_ops_root) / "runs" / cfg.run_id / "run.json").read_text(encoding="utf-8"))
    assert manifest["artifactIds"] == [
        "relocate_plan",
        "relocate_db_backup",
        "relocate_internal_move_plan",
        "relocate_move_apply_log",
        "relocate_db_update",
        "relocate_move_apply_stats",
    ]
    assert manifest["artifacts"]["relocate_internal_move_plan"]["inputArtifactIds"] == [
        "relocate_plan",
        "relocate_db_backup",
    ]


def test_relocate_apply_uses_run_scoped_on_dst_exists_policy(tmp_path) -> None:
    cfg, _plan_path = register_relocate_plan_ready_run(
        tmp_path,
        run_id="run_relocate_apply_rename_suffix",
        on_dst_exists="rename_suffix",
    )

    def fake_python_runner(script: Path, args: list[str], _cwd: str | None = None) -> str:
        if script.name == "backup_mediaops_db.py" and args[args.index("--action") + 1] == "backup":
            backup_path = Path(cfg.windows_ops_root) / "db" / "mediaops.sqlite.bak_20260426_pre_relocate_apply"
            backup_path.write_text("db backup", encoding="utf-8")
            return json.dumps({"ok": True, "action": "backup", "backup_path": str(backup_path), "error": ""}, ensure_ascii=False)
        if script.name == "backup_mediaops_db.py" and args[args.index("--action") + 1] == "rotate":
            return json.dumps({"ok": True, "action": "rotate", "deleted_count": 0}, ensure_ascii=False)
        if script.name == "update_db_paths_from_move_apply.py":
            return json.dumps({"updated": 1, "events": 1, "run_kind": "relocate"}, ensure_ascii=False)
        return json.dumps({"ok": True}, ensure_ascii=False)

    def fake_powershell_runner(_script: str, args: list[str]) -> dict:
        assert args[args.index("-OnDstExists") + 1] == "rename_suffix"
        out_path = Path(cfg.windows_ops_root) / "move" / "relocate_apply_rename_suffix.jsonl"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            "\n".join([
                json.dumps({"_meta": {"kind": "move_apply"}}, ensure_ascii=False),
                json.dumps(
                    {
                        "op": "move",
                        "ts": "2026-04-26T00:00:00Z",
                        "path_id": "p1",
                        "src": r"B:\VideoLibrary\Old\show.mp4",
                        "dst": r"B:\VideoLibrary\Show\show.mp4",
                        "ok": True,
                    },
                    ensure_ascii=False,
                ),
            ])
            + "\n",
            encoding="utf-8",
        )
        return {"out_jsonl": str(out_path), "run_id": "apply_test"}

    service = RelocateWorkflowService(
        python_runner=fake_python_runner,
        powershell_runner=fake_powershell_runner,
        py_root=tmp_path,
    )

    result = service.apply(cfg)

    assert result.to_dict()["outcome"] == "relocate_apply_complete"
