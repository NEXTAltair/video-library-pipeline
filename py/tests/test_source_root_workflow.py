import json
from pathlib import Path

from video_pipeline.workflows import SourceRootDryRunConfig, SourceRootWorkflowService


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
                json.dumps({"path_id": "p1", "program_title": "Show", "air_date": "2026-04-25"}, ensure_ascii=False)
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
            "tool": None,
            "params": {"runId": "run_source_root", "artifactId": "source_root_move_plan"},
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
        "make_move_plan_from_inventory.py",
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
    assert all(args[args.index("--db") + 1] == expected_db for _name, args in calls)

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
