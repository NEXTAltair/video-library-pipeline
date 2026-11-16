"""Native planning remains bounded and conservative on stale NTFS inventory."""
import errno
import subprocess
from pathlib import Path

import pytest

from video_pipeline.domain.companion_bundle import companion_paths

from video_pipeline.domain import recording_dedup as policy
from video_pipeline.platform.native_file_signatures import native_path_key
from .test_recording_dedup import library, add_recording, quality_probe


@pytest.mark.parametrize("bundle_mode", ["empty", "sidecar", "unverified", "reparse", "collision"])
def test_native_planning_batches_and_keeps_unsafe_files_out(library, monkeypatch, bundle_mode):
    from video_pipeline.platform import native_file_signatures as native
    con, _, root, _ = library
    for i in range(515):
        _, pid = add_recording(library, f"{i}.mp4", False, observe=False)
        con.execute("UPDATE paths SET path=? WHERE path_id=?", (f"Z:\\library\\{i}.mp4", pid))
    con.commit()
    original_stat, original_resolve = Path.stat, Path.resolve
    def stat(path, *args, **kwargs):
        if str(path).startswith("/mnt/z/library/"):
            raise OSError(errno.EIO, "native-only NTFS path")
        return original_stat(path, *args, **kwargs)
    def resolve(path, *args, **kwargs):
        if str(path).startswith("/mnt/z/library/"):
            raise OSError(errno.EIO, "native-only NTFS path")
        if str(path) == "/mnt/z/library":
            return path
        return original_resolve(path, *args, **kwargs)
    monkeypatch.setattr(Path, "stat", stat)
    monkeypatch.setattr(Path, "resolve", resolve)
    calls = []
    def scan(paths):
        calls.append(list(paths))
        result = {}
        for path in paths:
            index = int(path.stem)
            item = {"exists": False, "ancestors_checked": True}
            if index < 2:
                item.update(exists=True, signature={"size_bytes": 200,
                    "mtime_ns": 1700000000000000000, "mtime_utc": "2023-11-14T22:13:20+00:00"})
            if index == 2:
                item.update(error="native_reparse_ancestor")
            result[native_path_key(path)] = item
        return result
    monkeypatch.setattr(native, "read_native_signatures", scan)
    # The planning cache proves video signatures only. PR161 additionally needs
    # an independent closed bundle observation, including sidecars and ancestors.
    from video_pipeline.platform import native_companion_probe
    bundle_calls = []
    def bundle_probe(src, dst):
        bundle_calls.append((src, dst))
        if bundle_mode == "unverified":
            return {"ok": True, "source_exists": True}
        if bundle_mode in {"reparse", "collision"}:
            return {"ok": False, "bundle_error": "reparse_point" if bundle_mode == "reparse" else "destination_exists"}
        signature = {"size_bytes": 200, "mtime_ns": 1700000000000000000,
                     "mtime_utc": "2023-11-14T22:13:20+00:00"}
        members = [{"kind": "video", "src": src, "dst": dst, "precondition": signature}]
        cohort = []
        if bundle_mode == "sidecar":
            cohort.append("psc")
            members.append({"kind": "psc", "src": companion_paths(src)["psc"],
                            "dst": companion_paths(dst)["psc"],
                            "precondition": {**signature, "size_bytes": 10}})
        return {"ok": True, "bundle_verified": True, "bundle_version": 1,
                "members": members, "companion_cohort": cohort}
    monkeypatch.setattr(native_companion_probe, "read_native_bundle", bundle_probe)
    before = con.total_changes
    def plan_native():
        return policy.plan_recordings(con, [Path("/mnt/z/library")],
            probe=quality_probe, min_age_seconds=0)
    if bundle_mode in {"unverified", "reparse", "collision"}:
        error = {"unverified": "native_companion_capture_unavailable",
                 "reparse": "reparse_point", "collision": "destination_exists"}[bundle_mode]
        with pytest.raises(ValueError, match=error):
            plan_native()
        assert con.total_changes == before
        assert len(bundle_calls) == 1
        assert [len(call) for call in calls] == [512, 3, 1, 1]
        return
    plan = plan_native()
    assert [len(call) for call in calls[:2]] == [512, 3]
    # Fresh post-probe identity checks must not reuse the planning observation.
    assert len(calls) == 4 and all(len(call) == 1 for call in calls[2:])
    assert len(bundle_calls) == 1
    assert len(plan["moves"]) == 1
    move = plan["moves"][0]
    assert bundle_calls == [(move["src"], move["dst"])]
    assert move["bundle_version"] == 1
    assert move["companion_cohort"] == (["psc"] if bundle_mode == "sidecar" else [])
    assert [m["kind"] for m in move["members"]] == ["video", *move["companion_cohort"]]
    if bundle_mode == "sidecar":
        assert move["members"][1]["precondition"]["size_bytes"] == 10
    assert len(plan["groups"][0]["candidates"]) == 2
    assert plan["excluded"]["outside_roots_or_quarantine"] == 513
    assert con.total_changes == before


def test_readable_drvfs_planning_does_not_require_powershell(library, monkeypatch):
    from video_pipeline.platform import native_file_signatures as native
    con, _, root, _ = library
    recording, pid = add_recording(library, "readable.mp4", False, observe=False)
    con.execute("UPDATE paths SET path=? WHERE path_id=?", (r"Z:\library\readable.mp4", pid))
    con.commit()
    original_stat, original_resolve = Path.stat, Path.resolve
    fake_root = Path("/mnt/z/library")
    fake_file = fake_root / recording.name
    def stat(path, *args, **kwargs):
        if path == fake_file:
            return original_stat(recording, *args, **kwargs)
        return original_stat(path, *args, **kwargs)
    def resolve(path, *args, **kwargs):
        if path in (fake_root, fake_file):
            return path
        return original_resolve(path, *args, **kwargs)
    monkeypatch.setattr(Path, "stat", stat)
    monkeypatch.setattr(Path, "resolve", resolve)
    def no_native(_):
        pytest.fail("locally readable DrvFS files must not require PowerShell")
    monkeypatch.setattr(native, "read_native_signatures", no_native)
    before = con.total_changes
    plan = policy.plan_recordings(con, [fake_root], probe=quality_probe, min_age_seconds=0)
    assert plan["moves"] == []
    assert plan["excluded"] == {}
    assert con.total_changes == before


@pytest.mark.parametrize("failure", [ValueError("native scan failed"),
                                    OSError(errno.EIO, "native scan IO failure"),
                                    subprocess.TimeoutExpired("pwsh", 120)])
def test_failed_native_batch_keeps_readable_recordings(library, monkeypatch, failure):
    from video_pipeline.platform import native_file_signatures as native
    con, _, root, _ = library
    add_recording(library, "best.mp4", False, size=400)
    add_recording(library, "duplicate.mp4", False, size=200)
    _, pid = add_recording(library, "stale.mp4", False, observe=False)
    con.execute("UPDATE paths SET path=? WHERE path_id=?", (r"Z:\library\stale.mp4", pid))
    con.commit()
    fake_root = Path("/mnt/z/library")
    stale = fake_root / "stale.mp4"
    original_stat, original_resolve = Path.stat, Path.resolve
    def stat(path, *args, **kwargs):
        if path == stale:
            raise OSError(errno.EIO, "stale DrvFS path")
        return original_stat(path, *args, **kwargs)
    def resolve(path, *args, **kwargs):
        if path in (fake_root, stale):
            return path
        return original_resolve(path, *args, **kwargs)
    monkeypatch.setattr(Path, "stat", stat)
    monkeypatch.setattr(Path, "resolve", resolve)
    calls = []
    def failed_scan(paths):
        calls.append(list(paths))
        raise failure
    monkeypatch.setattr(native, "read_native_signatures", failed_scan)
    before = con.total_changes
    plan = policy.plan_recordings(con, [root, fake_root], probe=quality_probe, min_age_seconds=0)
    assert calls == [[stale]]
    assert plan["excluded"]["missing_empty_or_symlink"] == 1
    assert len(plan["moves"]) == 1
    assert len(plan["groups"][0]["candidates"]) == 2
    assert plan["moves"][0]["bundle_version"] == 1
    assert con.total_changes == before
