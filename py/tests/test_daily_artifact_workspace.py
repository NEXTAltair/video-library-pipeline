import argparse
import json

import pytest

import daily_cli
from video_pipeline.platform.artifact_retention import POLICY


@pytest.mark.parametrize("phase,status", [("complete", "complete"), ("plan_ready", "blocked"), ("review_required", "blocked")])
def test_preview_owns_a_separate_managed_workspace_and_keeps_review_held(tmp_path, monkeypatch, phase, status):
    args = argparse.Namespace(apply=False, windows_ops_root=str(tmp_path))

    def preview(args, *, preview_workspace):
        assert preview_workspace.parent == tmp_path / "artifacts"
        assert (preview_workspace / "cache").is_dir()
        assert (preview_workspace / "evidence").is_dir()
        return {"ok": phase != "review_required", "phase": phase}, 0

    monkeypatch.setattr(daily_cli, "_execute", preview)
    result, code = daily_cli.execute(args)
    workspace = tmp_path / "artifacts" / result["artifactWorkspace"].split("/")[-1]
    assert code == 0
    assert json.loads((workspace / POLICY).read_text())["status"] == status


def test_failed_preview_is_not_cleanup_eligible(tmp_path, monkeypatch):
    args = argparse.Namespace(apply=False, windows_ops_root=str(tmp_path))

    def interrupted(*args, **kwargs):
        raise RuntimeError("preview interrupted")

    monkeypatch.setattr(daily_cli, "_execute", interrupted)
    with pytest.raises(RuntimeError, match="interrupted"):
        daily_cli.execute(args)
    policy = next((tmp_path / "artifacts").glob("*/" + POLICY))
    assert json.loads(policy.read_text())["status"] == "failed"


def test_production_apply_does_not_create_cache_or_change_approval_semantics(tmp_path, monkeypatch):
    args = argparse.Namespace(apply=True, windows_ops_root=str(tmp_path))
    monkeypatch.setattr(daily_cli, "_execute", lambda value: ({"ok": True}, 0))
    assert daily_cli.execute(args) == ({"ok": True}, 0)
    assert not (tmp_path / "artifacts").exists()
