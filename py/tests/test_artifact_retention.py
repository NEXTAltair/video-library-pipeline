import json
import os
from pathlib import Path

import pytest

from artifact_cli import main
from video_pipeline.platform import artifact_retention as retention


def completed(root, name="job_1", days=0):
    job = retention.create_workspace(root, name, cache_days=days)
    (job / "cache" / "frame.jpg").write_bytes(b"generated frame")
    (job / "evidence" / "sheet.jpg").write_bytes(b"reviewed contact sheet")
    (job / "evidence" / "before.sqlite").write_bytes(b"required recovery backup")
    retention.finish_workspace(job)
    return job


def test_exact_plan_moves_only_cache_and_restore_preserves_all_bytes(tmp_path):
    job = completed(tmp_path)
    before = {str(file.relative_to(job)): file.read_bytes() for file in job.rglob("*") if file.is_file()}
    plan = retention.plan_cleanup(tmp_path)
    assert (plan["jobs"], plan["files"], plan["bytes"]) == (1, 1, len(b"generated frame"))
    result = retention.apply_cleanup(tmp_path, plan["planId"])
    assert result["permanentlyDeleted"] == 0
    assert not (job / "cache").exists()
    assert (job / "evidence" / "before.sqlite").read_bytes() == b"required recovery backup"
    preview = retention.restore_cleanup(tmp_path, plan["planId"])
    assert preview["preview"] and preview["restoreJobs"] == 1
    assert not (job / "cache").exists()
    retention.restore_cleanup(tmp_path, plan["planId"], apply=True)
    assert before == {str(file.relative_to(job)): file.read_bytes() for file in job.rglob("*") if file.is_file()}
    with pytest.raises(ValueError, match="already restored"):
        retention.restore_cleanup(tmp_path, plan["planId"], apply=True)


def test_old_unknown_data_failed_jobs_active_jobs_and_retention_are_protected(tmp_path):
    legacy = tmp_path / "legacy"
    legacy.mkdir()
    (legacy / "frame.jpg").write_bytes(b"unknown provenance")
    active = retention.create_workspace(tmp_path, "active", cache_days=0)
    (active / "cache" / "frame.jpg").write_bytes(b"active")
    failed = retention.create_workspace(tmp_path, "failed", cache_days=0)
    (failed / "cache" / "frame.jpg").write_bytes(b"failed")
    retention.finish_workspace(failed, status="failed")
    completed(tmp_path, "recent", days=7)
    plan = retention.plan_cleanup(tmp_path)
    assert plan["jobs"] == 0
    assert {row["job"]: row["reason"] for row in plan["protected"]} == {
        "legacy": "unknown_legacy_data", "active": "active_or_unresolved",
        "failed": "active_or_unresolved", "recent": "retention_not_elapsed",
    }


@pytest.mark.parametrize("body,reason", [
    ({"status": "active", "reviewGates": {}}, "workflow_or_review_open"),
    ({"status": "complete", "reviewGates": {"g": {"status": "open"}}}, "workflow_or_review_open"),
    ({"status": "complete", "artifacts": {"a": {"path": "../cache/frame.jpg", "sha256": "x"}}}, "cache_referenced_by_evidence"),
])
def test_review_and_checksum_evidence_override_completion(tmp_path, body, reason):
    job = completed(tmp_path)
    retention.write_json(job / "evidence" / "run.json", body)
    plan = retention.plan_cleanup(tmp_path)
    assert plan["jobs"] == 0
    assert plan["protected"][0]["reason"] == reason


@pytest.mark.parametrize("mutation", ["same_size_bytes", "new_file", "policy", "new_review"])
def test_all_sources_revalidated_before_any_move(tmp_path, mutation):
    first = completed(tmp_path, "a")
    second = completed(tmp_path, "z")
    plan = retention.plan_cleanup(tmp_path)
    if mutation == "same_size_bytes":
        file = second / "cache" / "frame.jpg"
        info = file.stat()
        file.write_bytes(b"different frame")
        os.utime(file, ns=(info.st_atime_ns, info.st_mtime_ns))
    elif mutation == "new_file":
        (second / "cache" / "new.jpg").write_bytes(b"new")
    elif mutation == "policy":
        policy = retention.read_json(second / retention.POLICY)
        policy["status"] = "active"
        retention.write_json(second / retention.POLICY, policy)
    else:
        retention.write_json(second / "evidence" / "run.json", {"status": "active"})
    with pytest.raises(ValueError, match="stale cleanup plan"):
        retention.apply_cleanup(tmp_path, plan["planId"])
    assert (first / "cache" / "frame.jpg").exists()
    assert (second / "cache" / "frame.jpg").exists()


@pytest.mark.parametrize("unsafe", ["file_link", "directory_link", "hardlink", "recording"])
def test_unsafe_cache_is_held(tmp_path, unsafe):
    job = completed(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    file = outside / "source"
    file.write_bytes(b"protected")
    if unsafe == "file_link":
        (job / "cache" / "unsafe").symlink_to(file)
    elif unsafe == "directory_link":
        (job / "cache" / "unsafe").symlink_to(outside, target_is_directory=True)
    elif unsafe == "hardlink":
        os.link(file, job / "cache" / "unsafe")
    else:
        (job / "cache" / "recording.mp4").write_bytes(b"recording")
    plan = retention.plan_cleanup(tmp_path)
    assert plan["jobs"] == 0
    assert any(row["job"] == job.name and row["reason"] == "inspection_failed" for row in plan["protected"])
    assert file.read_bytes() == b"protected"


def test_restore_does_not_overwrite_and_apply_cannot_replay(tmp_path):
    job = completed(tmp_path)
    plan = retention.plan_cleanup(tmp_path)
    retention.apply_cleanup(tmp_path, plan["planId"])
    with pytest.raises(ValueError, match="already started"):
        retention.apply_cleanup(tmp_path, plan["planId"])
    (job / "cache").mkdir()
    (job / "cache" / "new.jpg").write_bytes(b"new generation")
    with pytest.raises(ValueError, match="occupied"):
        retention.restore_cleanup(tmp_path, plan["planId"], apply=True)
    assert (job / "cache" / "new.jpg").read_bytes() == b"new generation"


def test_restore_recovers_rename_before_receipt_flush(tmp_path, monkeypatch):
    job = completed(tmp_path)
    plan = retention.plan_cleanup(tmp_path)
    write_json = retention.write_json

    def interrupted(path, body):
        if body.get("moved"):
            raise OSError("simulated process failure after rename")
        return write_json(path, body)

    monkeypatch.setattr(retention, "write_json", interrupted)
    with pytest.raises(OSError, match="simulated process failure"):
        retention.apply_cleanup(tmp_path, plan["planId"])
    assert not (job / "cache").exists()
    monkeypatch.setattr(retention, "write_json", write_json)
    assert retention.restore_cleanup(tmp_path, plan["planId"], apply=True)["restoreJobs"] == 1
    assert (job / "cache" / "frame.jpg").read_bytes() == b"generated frame"


def test_inventory_and_cli_are_readonly_and_paths_cannot_escape(tmp_path, capsys):
    job = completed(tmp_path)
    before = sorted(str(item) for item in tmp_path.rglob("*"))
    assert main(["--root", str(tmp_path)]) == 0
    body = json.loads(capsys.readouterr().out)
    assert body["files"] == 4 and body["groups"][job.name]["managed"]
    assert before == sorted(str(item) for item in tmp_path.rglob("*"))
    assert main(["--root", str(tmp_path), "--complete", "../escape"]) == 1
    assert "invalid" in json.loads(capsys.readouterr().out)["error"]
    assert main(["--root", str(tmp_path), "--apply"]) == 1
    assert "only valid" in json.loads(capsys.readouterr().out)["error"]


def test_root_symlink_and_malformed_policy_do_not_authorize_cleanup(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    link = tmp_path / "link"
    link.symlink_to(root, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        retention.create_workspace(link)
    job = completed(root)
    (job / retention.POLICY).write_text("{broken")
    plan = retention.plan_cleanup(root)
    assert plan["jobs"] == 0 and plan["protected"][0]["reason"] == "inspection_failed"


def test_modified_plan_cannot_restore_unrelated_job(tmp_path):
    completed(tmp_path)
    plan = retention.plan_cleanup(tmp_path)
    retention.apply_cleanup(tmp_path, plan["planId"])
    path = Path(plan["planPath"])
    body = retention.read_json(path)
    body["candidates"][0]["job"] = "unrelated"
    retention.write_json(path, body)
    with pytest.raises(ValueError, match="intent changed"):
        retention.restore_cleanup(tmp_path, plan["planId"], apply=True)


@pytest.mark.parametrize("filename,content", [
    ("review.yaml", "image: ../cache/frame.jpg\n"),
    ("review.jsonl", '{"image":"../cache/frame.jpg"}\n'),
    ("README.md", "Review [frame](../cache/frame.jpg)."),
])
def test_other_evidence_formats_protect_referenced_cache(tmp_path, filename, content):
    job = completed(tmp_path)
    (job / "evidence" / filename).write_text(content)
    plan = retention.plan_cleanup(tmp_path)
    assert plan["jobs"] == 0 and plan["protected"][0]["reason"] == "cache_referenced_by_evidence"


def test_absent_root_inventory_and_symlinks_are_readonly_not_failures(tmp_path):
    root = tmp_path / "absent"
    assert retention.inventory(root)["files"] == 0
    assert not root.exists()
    root.mkdir()
    (root / "link").symlink_to(tmp_path / "unavailable")
    result = retention.inventory(root)
    assert result["ok"] and result["excludedLinks"] == 1 and result["files"] == 0


def test_completed_preview_provenance_does_not_pin_a_disposable_db(tmp_path):
    job = completed(tmp_path)
    retention.write_json(job / "evidence" / "run.json", {
        "status": "complete", "configSnapshot": {"db": str(job / "cache" / "mediaops.sqlite")},
        "artifacts": {}, "reviewGates": {},
    })
    assert retention.plan_cleanup(tmp_path)["jobs"] == 1


def test_explicit_completion_after_a_hold_never_approves_an_open_gate(tmp_path):
    job = retention.create_workspace(tmp_path, "held", cache_days=0)
    (job / "cache" / "frame.jpg").write_bytes(b"cached")
    retention.finish_workspace(job, status="blocked")
    retention.write_json(job / "evidence" / "run.json", {"status": "complete", "reviewGates": {"g": {"status": "open"}}})
    retention.finish_workspace(job, status="complete")
    assert retention.plan_cleanup(tmp_path)["jobs"] == 0
    run = retention.read_json(job / "evidence" / "run.json")
    assert run["reviewGates"]["g"]["status"] == "open"
