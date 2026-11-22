import json
import sys

import pytest

import daily_cli
import dedup_cli
from video_pipeline.platform.writer_lock import WriterBusy, workflow_writer


def hold(ops):
    import multiprocessing, time
    ready = multiprocessing.Event(); release = multiprocessing.Event()
    proc = multiprocessing.Process(target=_holder, args=(str(ops), ready, release))
    proc.start(); assert ready.wait(10)
    return proc, release


def _holder(ops, ready, release):
    with workflow_writer(ops):
        ready.set(); release.wait(30)


def test_busy_scope_raises_writer_busy_and_stays_a_timeout(tmp_path):
    proc, release = hold(tmp_path)
    try:
        with pytest.raises(WriterBusy) as caught:
            with workflow_writer(tmp_path, timeout=0.2):
                pass
        assert isinstance(caught.value, TimeoutError)
    finally:
        release.set(); proc.join(10)


@pytest.mark.parametrize("module,argv,result_file,label", [
    (daily_cli, ["--source-root", "S", "--dest-root", "D", "--ts-root", "T", "--drive-routes", "R"], "daily_result.json", "未視聴整理"),
    (dedup_cli, ["--source-root", "S", "--dest-root", "D"], "duplicates/last_result.json", "重複番組整理"),
])
def test_scheduled_run_yields_to_a_busy_writer_without_failing(tmp_path, monkeypatch, capsys, module, argv, result_file, label):
    proc, release = hold(tmp_path)
    try:
        monkeypatch.setattr(module, "SCHEDULED_LOCK_WAIT_SECONDS", 0.2)
        base = ["cli", "--db", str(tmp_path / "db.sqlite"), "--windows-ops-root", str(tmp_path), *argv, "--apply"]
        monkeypatch.setattr(sys, "argv", base + ["--scheduled"])
        assert module.main() == 0
        out = capsys.readouterr().out
        assert label in out and "見送り" in out
        saved = json.loads((tmp_path / result_file).read_text())
        assert saved["ok"] is True and saved["skipped"] == "writer_busy"
    finally:
        release.set(); proc.join(10)
