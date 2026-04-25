import json
from pathlib import Path

from video_pipeline.workflows import (
    SourceRootApplyConfig,
    SourceRootDryRunConfig,
    SourceRootWorkflowService,
    ReviewGateStatus,
    WorkflowFlow,
    WorkflowPhase,
    WorkflowStore,
)


def make_config(tmp_path: Path, run_id: str = "run_source_root", db: str | None = None) -> SourceRootDryRunConfig:
    ops_root = tmp_path / "ops"
    scripts_root = ops_root / "scripts"
    scripts_root.mkdir(parents=True)
    return SourceRootDryRunConfig(
        windows_ops_root=str(ops_root),
        source_root=r"B:\Unwatched",
        dest_root=r"B:\VideoLibrary",
        db=db or str(ops_root / "db" / "mediaops.sqlite"),
        drive_routes=str(tmp_path / "drive_routes.yaml"),
        max_files_per_run=20,
        run_id=run_id,
    )


class SourceRootWorkflowHarness:
    def __init__(self, tmp_path: Path, *, run_id: str, needs_review: bool = False) -> None:
        self.tmp_path = tmp_path
        self.cfg = make_config(tmp_path, run_id=run_id)
        self.needs_review = needs_review
        self.calls: list[tuple[str, list[str]]] = []

    def powershell_runner(self, _script: str, args: list[str]) -> dict:
        out_path = Path(args[args.index("-OutJsonl") + 1])
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            "\n".join([
                json.dumps({"_meta": {"kind": "unwatched_inventory"}}, ensure_ascii=False),
                json.dumps(
                    {
                        "path": r"B:\Unwatched\show.mp4",
                        "name": "show.mp4",
                        "mtimeUtc": "2026-04-25T00:00:00Z",
                    },
                    ensure_ascii=False,
                ),
            ])
            + "\n",
            encoding="utf-8",
        )
        return {"out_jsonl": str(out_path), "warning_count": 0}

    def python_runner(self, script: Path, args: list[str], _cwd: str | None = None) -> str:
        self.calls.append((script.name, list(args)))
        if script.name == "make_metadata_queue_from_inventory.py":
            out_path = Path(args[args.index("--out") + 1])
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(
                "\n".join([
                    json.dumps({"_meta": {"kind": "metadata_queue"}}, ensure_ascii=False),
                    json.dumps({"path_id": "p1", "path": r"B:\Unwatched\show.mp4", "name": "show.mp4"}, ensure_ascii=False),
                ])
                + "\n",
                encoding="utf-8",
            )
            return json.dumps({"ok": True, "queueRows": 1}, ensure_ascii=False)
        if script.name == "run_metadata_batches_promptv1.py":
            outdir = Path(args[args.index("--outdir") + 1])
            output_path = outdir / "llm_filename_extract_output_0001_0001.jsonl"
            row = {
                "path_id": "p1",
                "path": r"B:\Unwatched\show.mp4",
                "program_title": "UNKNOWN" if self.needs_review else "Show",
                "air_date": "2026-04-25",
                "needs_review": self.needs_review,
            }
            if self.needs_review:
                row["needs_review_reason"] = "unknown_program_title"
            output_path.write_text(json.dumps(row, ensure_ascii=False) + "\n", encoding="utf-8")
            return json.dumps(
                {
                    "ok": True,
                    "outputJsonlPaths": [str(output_path)],
                    "latestOutputJsonlPath": str(output_path),
                    "processed": 1,
                },
                ensure_ascii=False,
            )
        if script.name == "export_program_yaml.py":
            if not self.needs_review:
                return json.dumps(
                    {
                        "ok": True,
                        "outputPath": None,
                        "reviewSummary": {"rowsNeedingReview": 0},
                        "reviewCandidates": [],
                        "reviewCandidatesTruncated": False,
                        "skippedReason": "no_reviewable_rows",
                    },
                    ensure_ascii=False,
                )
            out_path = Path(args[args.index("--output") + 1])
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text("hints: []\n", encoding="utf-8")
            return json.dumps(
                {
                    "ok": True,
                    "sourceJsonlPath": args[args.index("--source-jsonl") + 1],
                    "outputPath": str(out_path),
                    "reviewSummary": {
                        "rowsNeedingReview": 1,
                        "needsReviewFlagRows": 1,
                        "reasonCounts": {"unknown_program_title": 1},
                    },
                    "reviewCandidates": [{"pathId": "p1", "reasons": ["unknown_program_title"]}],
                    "reviewCandidatesTruncated": False,
                },
                ensure_ascii=False,
            )
        if script.name == "make_move_plan_from_inventory.py":
            if self.needs_review:
                raise AssertionError("move plan must not be generated while metadata review gate is open")
            out_path = Path(args[args.index("--out") + 1])
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(
                "\n".join([
                    json.dumps({"_meta": {"kind": "move_plan_from_inventory"}}, ensure_ascii=False),
                    json.dumps(
                        {
                            "path_id": "p1",
                            "src": r"B:\Unwatched\show.mp4",
                            "dst": r"B:\VideoLibrary\Show\show.mp4",
                        },
                        ensure_ascii=False,
                    ),
                ])
                + "\n",
                encoding="utf-8",
            )
            return json.dumps({"out": str(out_path), "planned": 1}, ensure_ascii=False)
        return json.dumps({"ok": True}, ensure_ascii=False)

    def service(self) -> SourceRootWorkflowService:
        return SourceRootWorkflowService(
            python_runner=self.python_runner,
            powershell_runner=self.powershell_runner,
            py_root=self.tmp_path,
        )

    def dry_run(self):
        return self.service().dry_run(self.cfg)

    def manifest(self) -> dict:
        manifest_path = Path(self.cfg.windows_ops_root) / "runs" / self.cfg.run_id / "run.json"
        return json.loads(manifest_path.read_text(encoding="utf-8"))


def assert_json_serializable_for_typescript(payload: dict) -> None:
    json.dumps(payload, ensure_ascii=False)
    assert {"ok", "runId", "flow", "phase", "outcome", "artifacts", "gates", "nextActions", "diagnostics"} <= set(payload)
    for artifact in payload["artifacts"]:
        assert {"id", "type", "path", "sha256", "createdAt", "producer", "status", "inputArtifactIds", "metadata"} <= set(artifact)
    for gate in payload["gates"]:
        assert {"id", "type", "status", "artifactIds", "requiresHumanReview", "openedAt", "resolvedAt", "resolution"} <= set(gate)
    for action in payload["nextActions"]:
        assert {"action", "label", "tool", "params", "requiresHumanInput"} <= set(action)


def test_source_root_fixture_no_review_result_json_shape(tmp_path) -> None:
    harness = SourceRootWorkflowHarness(tmp_path, run_id="run_fixture_no_review")

    payload = harness.dry_run().to_dict()

    assert_json_serializable_for_typescript(payload)
    assert payload["ok"] is True
    assert payload["flow"] == "source_root"
    assert payload["phase"] == "plan_ready"
    assert payload["outcome"] == "source_root_dry_run_complete"
    assert [artifact["id"] for artifact in payload["artifacts"]] == [
        "source_root_inventory",
        "metadata_queue",
        "metadata_extract_output_0001",
        "source_root_move_plan",
    ]
    assert payload["nextActions"] == [
        {
            "action": "review_plan",
            "label": "Review sourceRoot move plan",
            "tool": "video_pipeline_resume",
            "params": {
                "runId": "run_fixture_no_review",
                "artifactId": "source_root_move_plan",
                "resumeAction": "apply_source_root_move_plan",
            },
            "requiresHumanInput": True,
        }
    ]

    manifest = harness.manifest()
    assert manifest["artifactIds"] == [artifact["id"] for artifact in payload["artifacts"]]
    assert manifest["artifacts"]["source_root_move_plan"]["inputArtifactIds"] == [
        "source_root_inventory",
        "metadata_queue",
        "metadata_extract_output_0001",
    ]


def test_source_root_fixture_review_yaml_handoff_round_trip(tmp_path) -> None:
    harness = SourceRootWorkflowHarness(tmp_path, run_id="run_fixture_review", needs_review=True)

    payload = harness.dry_run().to_dict()

    assert_json_serializable_for_typescript(payload)
    assert payload["ok"] is False
    assert payload["phase"] == "review_required"
    assert payload["outcome"] == "source_root_metadata_review_required"
    assert payload["gates"][0]["artifactIds"] == ["metadata_review_yaml_0001"]
    assert payload["nextActions"][0]["params"]["artifactIds"] == ["metadata_review_yaml_0001"]

    manifest = harness.manifest()
    review_artifact = manifest["artifacts"]["metadata_review_yaml_0001"]
    review_path = str(Path(harness.cfg.windows_ops_root) / "runs" / "run_fixture_review" / "review" / "metadata_review_0001.yaml")
    assert review_artifact["path"] == review_path
    assert review_artifact["metadata"]["sourceJsonlPath"] == manifest["artifacts"]["metadata_extract_output_0001"]["path"]
    assert manifest["reviewGates"]["metadata_review"]["artifactIds"] == ["metadata_review_yaml_0001"]
    assert payload["nextActions"][0]["params"]["reviewYamlPaths"] == [review_path]
    assert Path(review_path).read_text(encoding="utf-8") == "hints: []\n"


def test_source_root_dry_run_registers_core_artifacts(tmp_path) -> None:
    cfg = make_config(tmp_path)
    calls: list[tuple[str, list[str]]] = []

    def fake_powershell_runner(_script: str, args: list[str]) -> dict:
        out_path = Path(args[args.index("-OutJsonl") + 1])
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            "\n".join([
                json.dumps({"_meta": {"kind": "unwatched_inventory"}}, ensure_ascii=False),
                json.dumps({"path": r"B:\Unwatched\show.mp4", "name": "show.mp4", "mtimeUtc": "2026-04-25T00:00:00Z"}, ensure_ascii=False),
            ])
            + "\n",
            encoding="utf-8",
        )
        return {"out_jsonl": str(out_path), "warning_count": 0}

    def fake_python_runner(script: Path, args: list[str], _cwd: str | None = None) -> str:
        calls.append((script.name, list(args)))
        if script.name == "make_metadata_queue_from_inventory.py":
            out_path = Path(args[args.index("--out") + 1])
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(
                "\n".join([
                    json.dumps({"_meta": {"kind": "metadata_queue"}}, ensure_ascii=False),
                    json.dumps({"path_id": "p1", "path": r"B:\Unwatched\show.mp4", "name": "show.mp4"}, ensure_ascii=False),
                ])
                + "\n",
                encoding="utf-8",
            )
            return "OK queue_rows=1\n"
        if script.name == "run_metadata_batches_promptv1.py":
            outdir = Path(args[args.index("--outdir") + 1])
            output_path = outdir / "llm_filename_extract_output_0001_0001.jsonl"
            output_path.write_text(
                json.dumps(
                    {"path_id": "p1", "program_title": "Show", "air_date": "2026-04-25", "needs_review": False},
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )
            return json.dumps(
                {
                    "ok": True,
                    "outputJsonlPaths": [str(output_path)],
                    "latestOutputJsonlPath": str(output_path),
                    "processed": 1,
                },
                ensure_ascii=False,
            )
        if script.name == "export_program_yaml.py":
            return json.dumps(
                {
                    "ok": True,
                    "outputPath": None,
                    "reviewSummary": {"rowsNeedingReview": 0},
                    "reviewCandidates": [],
                    "reviewCandidatesTruncated": False,
                    "skippedReason": "no_reviewable_rows",
                },
                ensure_ascii=False,
            )
        if script.name == "make_move_plan_from_inventory.py":
            out_path = Path(args[args.index("--out") + 1])
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(
                "\n".join([
                    json.dumps({"_meta": {"kind": "move_plan_from_inventory"}}, ensure_ascii=False),
                    json.dumps({"path_id": "p1", "src": r"B:\Unwatched\show.mp4", "dst": r"B:\VideoLibrary\Show\show.mp4"}, ensure_ascii=False),
                ])
                + "\n",
                encoding="utf-8",
            )
            return json.dumps({"out": str(out_path), "planned": 1}, ensure_ascii=False)
        return json.dumps({"ok": True}, ensure_ascii=False)

    service = SourceRootWorkflowService(
        python_runner=fake_python_runner,
        powershell_runner=fake_powershell_runner,
        py_root=tmp_path,
    )

    result = service.dry_run(cfg)

    assert result.ok is True
    payload = result.to_dict()
    assert payload["runId"] == "run_source_root"
    assert payload["flow"] == "source_root"
    assert payload["phase"] == "plan_ready"
    assert payload["outcome"] == "source_root_dry_run_complete"
    assert payload["gates"] == []
    assert payload["nextActions"] == [
        {
            "action": "review_plan",
            "label": "Review sourceRoot move plan",
            "tool": "video_pipeline_resume",
            "params": {
                "runId": "run_source_root",
                "artifactId": "source_root_move_plan",
                "resumeAction": "apply_source_root_move_plan",
            },
            "requiresHumanInput": True,
        }
    ]

    manifest_path = Path(cfg.windows_ops_root) / "runs" / "run_source_root" / "run.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["phase"] == "plan_ready"
    assert manifest["artifactIds"] == [
        "source_root_inventory",
        "metadata_queue",
        "metadata_extract_output_0001",
        "source_root_move_plan",
    ]
    artifacts = manifest["artifacts"]
    assert artifacts["source_root_inventory"]["type"] == "source_root_inventory"
    assert artifacts["metadata_queue"]["inputArtifactIds"] == ["source_root_inventory"]
    assert artifacts["metadata_extract_output_0001"]["inputArtifactIds"] == ["metadata_queue"]
    assert artifacts["source_root_move_plan"]["inputArtifactIds"] == [
        "source_root_inventory",
        "metadata_queue",
        "metadata_extract_output_0001",
    ]
    assert [name for name, _args in calls] == [
        "ingest_inventory_jsonl.py",
        "make_metadata_queue_from_inventory.py",
        "run_metadata_batches_promptv1.py",
        "export_program_yaml.py",
        "make_move_plan_from_inventory.py",
    ]


def test_source_root_dry_run_registers_review_yaml_and_gate_when_review_required(tmp_path) -> None:
    cfg = make_config(tmp_path, run_id="run_source_root_review")
    calls: list[tuple[str, list[str]]] = []

    def fake_powershell_runner(_script: str, args: list[str]) -> dict:
        out_path = Path(args[args.index("-OutJsonl") + 1])
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            "\n".join([
                json.dumps({"_meta": {"kind": "unwatched_inventory"}}, ensure_ascii=False),
                json.dumps({"path": r"B:\Unwatched\show.mp4", "name": "show.mp4"}, ensure_ascii=False),
            ])
            + "\n",
            encoding="utf-8",
        )
        return {"out_jsonl": str(out_path), "warning_count": 0}

    def fake_python_runner(script: Path, args: list[str], _cwd: str | None = None) -> str:
        calls.append((script.name, list(args)))
        if script.name == "make_metadata_queue_from_inventory.py":
            out_path = Path(args[args.index("--out") + 1])
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(
                "\n".join([
                    json.dumps({"_meta": {"kind": "metadata_queue"}}, ensure_ascii=False),
                    json.dumps({"path_id": "p1", "path": r"B:\Unwatched\show.mp4", "name": "show.mp4"}, ensure_ascii=False),
                ])
                + "\n",
                encoding="utf-8",
            )
        elif script.name == "run_metadata_batches_promptv1.py":
            outdir = Path(args[args.index("--outdir") + 1])
            output_path = outdir / "llm_filename_extract_output_0001_0001.jsonl"
            output_path.write_text(
                json.dumps(
                    {
                        "path_id": "p1",
                        "path": r"B:\Unwatched\show.mp4",
                        "program_title": "UNKNOWN",
                        "air_date": "2026-04-25",
                        "needs_review": True,
                        "needs_review_reason": "unknown_program_title",
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )
            return json.dumps({"ok": True, "outputJsonlPaths": [str(output_path)]}, ensure_ascii=False)
        elif script.name == "export_program_yaml.py":
            out_path = Path(args[args.index("--output") + 1])
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text("hints: []\n", encoding="utf-8")
            return json.dumps(
                {
                    "ok": True,
                    "sourceJsonlPath": args[args.index("--source-jsonl") + 1],
                    "outputPath": str(out_path),
                    "reviewSummary": {
                        "rowsNeedingReview": 1,
                        "needsReviewFlagRows": 1,
                        "fieldCounts": {"needs_review": 1},
                        "reasonCounts": {"unknown_program_title": 1},
                    },
                    "reviewCandidates": [{"pathId": "p1", "reasons": ["unknown_program_title"]}],
                    "reviewCandidatesTruncated": False,
                },
                ensure_ascii=False,
            )
        elif script.name == "make_move_plan_from_inventory.py":
            raise AssertionError("move plan must not be generated while metadata review gate is open")
        return json.dumps({"ok": True}, ensure_ascii=False)

    service = SourceRootWorkflowService(
        python_runner=fake_python_runner,
        powershell_runner=fake_powershell_runner,
        py_root=tmp_path,
    )

    result = service.dry_run(cfg)

    assert result.ok is False
    payload = result.to_dict()
    assert payload["phase"] == "review_required"
    assert payload["outcome"] == "source_root_metadata_review_required"
    assert payload["gates"] == [
        {
            "id": "metadata_review",
            "type": "metadata_review",
            "status": "open",
            "artifactIds": ["metadata_review_yaml_0001"],
            "requiresHumanReview": True,
            "openedAt": payload["gates"][0]["openedAt"],
            "resolvedAt": None,
            "resolution": {},
        }
    ]
    assert payload["nextActions"] == [
        {
            "action": "review_metadata",
            "label": "Review extracted metadata YAML",
            "tool": "video_pipeline_resume",
            "params": {
                "runId": "run_source_root_review",
                "gateId": "metadata_review",
                "artifactIds": ["metadata_review_yaml_0001"],
                "reviewYamlPaths": [
                    str(Path(cfg.windows_ops_root) / "runs" / "run_source_root_review" / "review" / "metadata_review_0001.yaml")
                ],
                "resumeAction": "apply_reviewed_metadata",
            },
            "requiresHumanInput": True,
        }
    ]

    manifest_path = Path(cfg.windows_ops_root) / "runs" / "run_source_root_review" / "run.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["phase"] == "review_required"
    assert manifest["artifactIds"] == [
        "source_root_inventory",
        "metadata_queue",
        "metadata_extract_output_0001",
        "metadata_review_yaml_0001",
    ]
    assert manifest["reviewGateIds"] == ["metadata_review"]
    review_artifact = manifest["artifacts"]["metadata_review_yaml_0001"]
    assert review_artifact["type"] == "metadata_review_yaml"
    assert review_artifact["inputArtifactIds"] == ["metadata_extract_output_0001"]
    assert review_artifact["metadata"]["reviewSummary"]["rowsNeedingReview"] == 1
    assert manifest["reviewGates"]["metadata_review"]["artifactIds"] == ["metadata_review_yaml_0001"]
    assert [name for name, _args in calls] == [
        "ingest_inventory_jsonl.py",
        "make_metadata_queue_from_inventory.py",
        "run_metadata_batches_promptv1.py",
        "export_program_yaml.py",
    ]


def test_source_root_dry_run_normalizes_windows_db_path_for_python_stages(tmp_path) -> None:
    cfg = make_config(tmp_path, run_id="run_source_root_windows_db", db=r"B:\_AI_WORK\db\mediaops.sqlite")
    calls: list[tuple[str, list[str]]] = []

    def fake_powershell_runner(_script: str, args: list[str]) -> dict:
        out_path = Path(args[args.index("-OutJsonl") + 1])
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            "\n".join([
                json.dumps({"_meta": {"kind": "unwatched_inventory"}}, ensure_ascii=False),
                json.dumps({"path": r"B:\Unwatched\show.mp4", "name": "show.mp4"}, ensure_ascii=False),
            ])
            + "\n",
            encoding="utf-8",
        )
        return {"out_jsonl": str(out_path), "warning_count": 0}

    def fake_python_runner(script: Path, args: list[str], _cwd: str | None = None) -> str:
        calls.append((script.name, list(args)))
        if script.name == "make_metadata_queue_from_inventory.py":
            out_path = Path(args[args.index("--out") + 1])
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(
                "\n".join([
                    json.dumps({"_meta": {"kind": "metadata_queue"}}, ensure_ascii=False),
                    json.dumps({"path_id": "p1", "path": r"B:\Unwatched\show.mp4", "name": "show.mp4"}, ensure_ascii=False),
                ])
                + "\n",
                encoding="utf-8",
            )
        elif script.name == "run_metadata_batches_promptv1.py":
            outdir = Path(args[args.index("--outdir") + 1])
            output_path = outdir / "llm_filename_extract_output_0001_0001.jsonl"
            output_path.write_text("{}\n", encoding="utf-8")
            return json.dumps({"ok": True, "outputJsonlPaths": [str(output_path)]}, ensure_ascii=False)
        elif script.name == "export_program_yaml.py":
            return json.dumps({"ok": True, "outputPath": None, "reviewSummary": {"rowsNeedingReview": 0}}, ensure_ascii=False)
        elif script.name == "make_move_plan_from_inventory.py":
            out_path = Path(args[args.index("--out") + 1])
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps({"_meta": {"kind": "move_plan_from_inventory"}}) + "\n", encoding="utf-8")
            return json.dumps({"out": str(out_path), "planned": 0}, ensure_ascii=False)
        return json.dumps({"ok": True}, ensure_ascii=False)

    service = SourceRootWorkflowService(
        python_runner=fake_python_runner,
        powershell_runner=fake_powershell_runner,
        py_root=tmp_path,
    )

    result = service.dry_run(cfg)

    assert result.ok is True
    expected_db = "/mnt/b/_AI_WORK/db/mediaops.sqlite"
    assert calls
    assert all(args[args.index("--db") + 1] == expected_db for _name, args in calls if "--db" in args)

    manifest_path = Path(cfg.windows_ops_root) / "runs" / "run_source_root_windows_db" / "run.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["configSnapshot"]["db"] == expected_db


def test_source_root_dry_run_failure_returns_failed_result_and_diagnostic(tmp_path) -> None:
    cfg = make_config(tmp_path, run_id="run_source_root_failed")

    def fake_powershell_runner(_script: str, args: list[str]) -> dict:
        out_path = Path(args[args.index("-OutJsonl") + 1])
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps({"_meta": {"kind": "unwatched_inventory"}}) + "\n", encoding="utf-8")
        return {"out_jsonl": str(out_path)}

    def fake_python_runner(script: Path, _args: list[str], _cwd: str | None = None) -> str:
        if script.name == "make_metadata_queue_from_inventory.py":
            raise RuntimeError("queue generation failed")
        return json.dumps({"ok": True}, ensure_ascii=False)

    service = SourceRootWorkflowService(
        python_runner=fake_python_runner,
        powershell_runner=fake_powershell_runner,
        py_root=tmp_path,
    )

    result = service.dry_run(cfg)

    assert result.ok is False
    payload = result.to_dict()
    assert payload["phase"] == "failed"
    assert payload["outcome"] == "source_root_dry_run_failed"
    assert payload["diagnostics"][0]["code"] == "source_root_dry_run_failed"
    assert payload["diagnostics"][0]["message"] == "queue generation failed"

    manifest_path = Path(cfg.windows_ops_root) / "runs" / "run_source_root_failed" / "run.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["phase"] == "failed"
    assert manifest["diagnostics"][0]["message"] == "queue generation failed"


def register_plan_ready_run(tmp_path: Path, run_id: str = "run_source_root_apply") -> tuple[SourceRootApplyConfig, Path]:
    ops_root = tmp_path / "ops"
    store = WorkflowStore(ops_root)
    store.init_run(
        WorkflowFlow.SOURCE_ROOT,
        run_id=run_id,
        config_snapshot={"windowsOpsRoot": str(ops_root), "db": str(ops_root / "db" / "mediaops.sqlite")},
    )
    plan_path = ops_root / "runs" / run_id / "plan" / "move_plan_from_inventory.jsonl"
    plan_path.write_text(
        "\n".join([
            json.dumps({"_meta": {"kind": "move_plan_from_inventory"}}, ensure_ascii=False),
            json.dumps(
                {
                    "op": "move",
                    "path_id": "p1",
                    "src": r"B:\Unwatched\show.mp4",
                    "dst": r"B:\VideoLibrary\Show\show.mp4",
                },
                ensure_ascii=False,
            ),
        ])
        + "\n",
        encoding="utf-8",
    )
    store.register_artifact(
        run_id,
        artifact_type="source_root_move_plan",
        path=plan_path,
        producer="make_move_plan_from_inventory.py",
        artifact_id="source_root_move_plan",
    )
    store.transition_run(run_id, WorkflowPhase.PLAN_READY)
    return SourceRootApplyConfig(windows_ops_root=str(ops_root), run_id=run_id), plan_path


def test_source_root_apply_rejects_plan_artifact_from_another_run(tmp_path) -> None:
    ops_root = tmp_path / "ops"
    store = WorkflowStore(ops_root)
    store.init_run(WorkflowFlow.SOURCE_ROOT, run_id="run_a", config_snapshot={"db": str(ops_root / "db.sqlite")})
    foreign_plan = ops_root / "runs" / "run_a" / "plan" / "foreign.jsonl"
    foreign_plan.write_text("{}\n", encoding="utf-8")
    store.register_artifact(
        "run_a",
        artifact_type="source_root_move_plan",
        path=foreign_plan,
        producer="test",
        artifact_id="foreign_plan",
    )
    store.init_run(WorkflowFlow.SOURCE_ROOT, run_id="run_b", config_snapshot={"db": str(ops_root / "db.sqlite")})
    store.transition_run("run_b", WorkflowPhase.PLAN_READY)

    service = SourceRootWorkflowService(py_root=tmp_path)
    result = service.apply(SourceRootApplyConfig(windows_ops_root=str(ops_root), run_id="run_b", artifact_id="foreign_plan"))

    payload = result.to_dict()
    assert payload["ok"] is False
    assert payload["phase"] == "blocked"
    assert payload["outcome"] == "source_root_apply_rejected"
    assert payload["diagnostics"][-1]["code"] == "source_root_apply_plan_not_in_run"


def test_source_root_apply_rejects_open_review_gate(tmp_path) -> None:
    cfg, _plan_path = register_plan_ready_run(tmp_path, run_id="run_source_root_gate_blocked")
    store = WorkflowStore(Path(cfg.windows_ops_root))
    store.create_review_gate(
        cfg.run_id,
        gate_type="metadata_review",
        artifact_ids=["metadata_review_yaml_0001"],
        gate_id="metadata_review",
    )

    service = SourceRootWorkflowService(py_root=tmp_path)
    result = service.apply(cfg)

    payload = result.to_dict()
    assert payload["ok"] is False
    assert payload["phase"] == "blocked"
    assert payload["diagnostics"][-1]["code"] == "source_root_apply_review_gate_blocked"
    assert payload["diagnostics"][-1]["details"]["blockingGateIds"] == ["metadata_review"]


def test_source_root_apply_rejects_rejected_review_gate(tmp_path) -> None:
    cfg, _plan_path = register_plan_ready_run(tmp_path, run_id="run_source_root_gate_rejected")
    store = WorkflowStore(Path(cfg.windows_ops_root))
    store.create_review_gate(
        cfg.run_id,
        gate_type="metadata_review",
        artifact_ids=["metadata_review_yaml_0001"],
        gate_id="metadata_review",
    )
    store.update_review_gate(
        cfg.run_id,
        "metadata_review",
        status=ReviewGateStatus.REJECTED,
        resolution={"reason": "bad metadata"},
    )

    service = SourceRootWorkflowService(py_root=tmp_path)
    result = service.apply(cfg)

    payload = result.to_dict()
    assert payload["ok"] is False
    assert payload["phase"] == "blocked"
    assert payload["diagnostics"][-1]["code"] == "source_root_apply_review_gate_blocked"
    assert payload["diagnostics"][-1]["details"]["blockingGateStatuses"] == {"metadata_review": "rejected"}


def test_source_root_apply_rejects_checksum_mismatch(tmp_path) -> None:
    cfg, plan_path = register_plan_ready_run(tmp_path, run_id="run_source_root_stale_plan")
    plan_path.write_text("{}\n", encoding="utf-8")

    service = SourceRootWorkflowService(py_root=tmp_path)
    result = service.apply(cfg)

    payload = result.to_dict()
    assert payload["ok"] is False
    assert payload["phase"] == "blocked"
    assert payload["diagnostics"][-1]["code"] == "source_root_apply_plan_checksum_mismatch"


def test_source_root_apply_rejects_missing_run_without_crashing(tmp_path) -> None:
    cfg = SourceRootApplyConfig(windows_ops_root=str(tmp_path / "ops"), run_id="run_missing")
    service = SourceRootWorkflowService(py_root=tmp_path)

    result = service.apply(cfg)

    payload = result.to_dict()
    assert payload["ok"] is False
    assert payload["phase"] == "failed"
    assert payload["outcome"] == "source_root_apply_rejected"
    assert payload["diagnostics"][-1]["code"] == "source_root_apply_rejected"
    assert payload["diagnostics"][-1]["details"]["exceptionType"] == "FileNotFoundError"


def test_source_root_apply_rejects_terminal_run_without_blocked_transition(tmp_path) -> None:
    cfg, _plan_path = register_plan_ready_run(tmp_path, run_id="run_source_root_terminal")
    store = WorkflowStore(Path(cfg.windows_ops_root))
    store.transition_run(cfg.run_id, WorkflowPhase.COMPLETE)

    service = SourceRootWorkflowService(py_root=tmp_path)
    result = service.apply(cfg)

    payload = result.to_dict()
    assert payload["ok"] is False
    assert payload["phase"] == "complete"
    assert payload["outcome"] == "source_root_apply_rejected"
    assert payload["diagnostics"][-1]["code"] == "source_root_apply_wrong_phase"
    manifest_path = Path(cfg.windows_ops_root) / "runs" / cfg.run_id / "run.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["phase"] == "complete"
    assert manifest["diagnostics"][-1]["code"] == "source_root_apply_wrong_phase"


def test_source_root_apply_rejects_other_flow_without_blocking_run(tmp_path) -> None:
    ops_root = tmp_path / "ops"
    store = WorkflowStore(ops_root)
    store.init_run(WorkflowFlow.RELOCATE, run_id="run_relocate", config_snapshot={"db": str(ops_root / "db.sqlite")})
    store.transition_run("run_relocate", WorkflowPhase.PLAN_READY)

    service = SourceRootWorkflowService(py_root=tmp_path)
    result = service.apply(SourceRootApplyConfig(windows_ops_root=str(ops_root), run_id="run_relocate"))

    payload = result.to_dict()
    assert payload["ok"] is False
    assert payload["flow"] == "relocate"
    assert payload["phase"] == "plan_ready"
    assert payload["outcome"] == "source_root_apply_rejected"
    assert payload["diagnostics"][-1]["code"] == "source_root_apply_wrong_flow"
    manifest_path = ops_root / "runs" / "run_relocate" / "run.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["flow"] == "relocate"
    assert manifest["phase"] == "plan_ready"
    assert manifest["diagnostics"][-1]["code"] == "source_root_apply_wrong_flow"


def test_source_root_apply_registers_apply_artifacts_and_completes(tmp_path) -> None:
    cfg, _plan_path = register_plan_ready_run(tmp_path, run_id="run_source_root_apply_success")
    calls: list[tuple[str, list[str]]] = []

    def fake_powershell_runner(script: str, args: list[str]) -> dict:
        assert script.endswith(r"\apply_move_plan.ps1")
        assert "-PlanJsonl" in args
        out_path = Path(cfg.windows_ops_root) / "move" / "move_apply_test.jsonl"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            "\n".join([
                json.dumps({"_meta": {"kind": "move_apply"}}, ensure_ascii=False),
                json.dumps(
                    {
                        "op": "move",
                        "ts": "2026-04-25T00:00:00Z",
                        "path_id": "p1",
                        "src": r"B:\Unwatched\show.mp4",
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

    def fake_python_runner(script: Path, args: list[str], _cwd: str | None = None) -> str:
        calls.append((script.name, list(args)))
        assert script.name == "update_db_paths_from_move_apply.py"
        assert "--db" in args
        assert "--applied" in args
        return json.dumps({"updated": 1, "events": 1, "run_kind": "source_root_apply"}, ensure_ascii=False)

    service = SourceRootWorkflowService(
        python_runner=fake_python_runner,
        powershell_runner=fake_powershell_runner,
        py_root=tmp_path,
    )

    result = service.resume(cfg, action="apply_source_root_move_plan")

    payload = result.to_dict()
    assert payload["ok"] is True
    assert payload["phase"] == "complete"
    assert payload["outcome"] == "source_root_apply_complete"
    assert [name for name, _args in calls] == ["update_db_paths_from_move_apply.py"]

    manifest_path = Path(cfg.windows_ops_root) / "runs" / cfg.run_id / "run.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["phase"] == "complete"
    assert manifest["status"] == "complete"
    assert manifest["artifactIds"] == [
        "source_root_move_plan",
        "source_root_move_apply_log",
        "source_root_db_update",
        "source_root_move_apply_stats",
    ]
    assert manifest["artifacts"]["source_root_move_apply_log"]["inputArtifactIds"] == ["source_root_move_plan"]
    assert manifest["artifacts"]["source_root_db_update"]["inputArtifactIds"] == ["source_root_move_apply_log"]
    assert manifest["artifacts"]["source_root_move_apply_stats"]["metadata"]["summary"]["succeeded"] == 1
