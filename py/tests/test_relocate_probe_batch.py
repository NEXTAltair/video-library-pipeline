import json
from pathlib import PureWindowsPath
from types import SimpleNamespace

import pytest

import relocate_existing_files as relocate
from video_pipeline.domain import relocate_probe as probe
from video_pipeline.domain.companion_bundle import companion_paths
from video_pipeline.platform import windows_probe_batch as transport
from .test_routing_inventory import Preview
from .test_relocate_metadata_scope import scope_file

SRC, DST = r"B:\shows\録画'＃1.mp4", r"E:\未作成\録画'＃1.mp4"
STAMP, TICKS = "2024-01-01T00:00:00.1234567Z", 133485408001234567


def entries(paths):
    result = {p: {"kind": "missing"} for p in paths}
    for p in (r"B:\shows", "B:\\", "E:\\"):
        if p in result:
            result[p] = {"kind": "directory"}
    if SRC in result:
        result[SRC] = {"kind": "file", "size": 456, "ticks": TICKS}
    return result


def inspect(**updates):
    def reader(paths):
        observed = entries(paths)
        assert set(updates) <= set(paths)
        observed.update(updates)
        return observed
    return list(probe.inspect_windows_moves([(SRC, DST, 456, STAMP)], reader=reader))[0]


def test_capture_keeps_exact_ntfs_ticks_and_closed_cohort():
    paths = companion_paths(SRC)
    bundle, reason = inspect(**{paths["vtt"]: {"kind": "file", "size": 0, "ticks": TICKS + 1}})
    assert not reason
    assert bundle["companion_cohort"] == ["vtt"]
    video, caption = bundle["members"]
    assert video == dict(kind="video", src=SRC, dst=DST, precondition=dict(
        size_bytes=456, mtime_utc="2024-01-01T00:00:00.1234567+00:00", mtime_ns=1704067200123456700))
    assert caption["precondition"]["mtime_ns"] == video["precondition"]["mtime_ns"] + 100
    assert caption["src"] == paths["vtt"] and caption["dst"] == companion_paths(DST)["vtt"]


@pytest.mark.parametrize("path,entry,reason", [
    (SRC, {"kind": "file", "size": 455, "ticks": TICKS}, "source_changed"),
    (SRC, {"kind": "file", "size": 456, "ticks": TICKS + 1}, "source_changed"),
    (SRC, {"kind": "file", "size": 456, "ticks": TICKS - 90000}, "source_changed"),
    (SRC, {"kind": "file", "size": 456}, "invalid_native_companion_signature"),
    (SRC, {"kind": "missing"}, "source_missing"),
    (DST, {"kind": "directory"}, "destination_exists"),
    (r"B:\shows", {"kind": "reparse"}, "reparse_point"),
    ("E:\\", {"kind": "missing"}, "destination_parent_unavailable"),
    (str(PureWindowsPath(DST).parent), {"kind": "file"}, "destination_parent_unavailable"),
    (SRC[:-4] + ".mkv", {"kind": "directory"}, "ambiguous_companion_stem"),
    (DST[:-4] + ".ts", {"kind": "file"}, "ambiguous_companion_stem"),
    (companion_paths(SRC)["psc"], {"kind": "directory"}, "non_regular_bundle_member"),
    (companion_paths(SRC)["recording_json"], {"kind": "unknown"}, "unverified_windows_entry"),
    (companion_paths(DST)["program_txt"], {"kind": "file"}, "destination_exists"),
    (companion_paths(DST)["vtt"], {"kind": "reparse"}, "reparse_point"),
])
def test_never_plans_on_unsafe_or_unknown_evidence(path, entry, reason):
    assert inspect(**{path: entry}) == (None, reason)


def test_deduplicated_exact_reads_bounded_per_chunk_no_cross_plan_cache(monkeypatch):
    calls = []
    rows = [(SRC, DST, 456, STAMP)] * 5
    monkeypatch.setattr(probe, "BATCH_SIZE", 2)
    def read(paths):
        calls.append(paths)
        assert len(set(paths)) == len(paths)
        return entries(paths)
    assert all(bundle and not refusal for bundle, refusal in probe.inspect_windows_moves(rows, reader=read))
    assert len(calls) == 3 and calls[0] == calls[1] == calls[2]
    assert SRC in calls[0] and DST in calls[0]
    assert not list(probe.inspect_windows_moves([], reader=lambda _: pytest.fail("no candidates")))
    with pytest.raises(ValueError, match="incomplete"):
        list(probe.inspect_windows_moves(rows, reader=lambda _: {}))


@pytest.mark.parametrize("reply", ["[]", '[{"path":"wrong","kind":"missing"}]', "{}", "garbage"])
def test_transport_never_uses_truncated_crosswired_or_malformed_observations(monkeypatch, reply):
    def run(argv, **kwargs):
        assert SRC not in " ".join(argv)
        assert json.loads(kwargs["input"]) == [SRC]
        return SimpleNamespace(returncode=0, stdout=reply, stderr="")
    monkeypatch.setattr(transport.subprocess, "run", run)
    with pytest.raises(ValueError):
        transport.read_windows_entries([SRC], "pwsh.exe")


def test_windows_preview_only_qualifying_candidates_are_batched(tmp_path, monkeypatch, capsys):
    preview = Preview(tmp_path, monkeypatch)
    first = preview.add(r"B:\Library\Show one.mp4")
    preview.add(r"B:\Library\Show held.mp4", hold=True)
    second = preview.add(r"B:\Library\Show two.mp4")
    preview.add(r"N:\Show\Show correct.mp4")
    monkeypatch.setattr(relocate, "scan_files", lambda **_: (preview.files, [], [], {"windowsFallbackUsed": True}))
    monkeypatch.setattr(relocate, "inspect_move", lambda *_a, **_kw: pytest.fail("per-file probe"))
    monkeypatch.setattr(relocate, "capture_bundle", lambda *_a, **_kw: pytest.fail("per-file bundle"))
    def batch(rows):
        assert [r[0] for r in rows] == [first.win_path, second.win_path]
        (src, dst, size, time), _ = rows
        yield {"bundle_version": 1, "members": [dict(kind="video", src=src, dst=dst,
                precondition=dict(size_bytes=size, mtime_utc=time))], "companion_cohort": []}, None
        yield None, "destination_exists"
    monkeypatch.setattr(relocate, "inspect_windows_moves", batch)
    selected = [f.win_path for f in preview.files]
    excluded = preview.add(r"B:\Library\Show partition held.mp4").win_path
    scope = scope_file(tmp_path, selected, [excluded])
    summary, rows, _ = preview.run(monkeypatch, capsys, "--metadata-scope-file", scope)
    assert [r["reason"] for r in rows] == ["recompute_destination", "recording_hold", "destination_exists", "already_correct"]
    assert summary["plannedMoves"] == summary["preflightSkipped"] == 1


@pytest.mark.parametrize("path", [r"C:\a\..\b", r"C:\x:stream", "C:\\x\0y", r"relative"])
def test_invalid_native_literals_do_not_reach_transport(monkeypatch, path):
    monkeypatch.setattr(transport.subprocess, "run", lambda *_a, **_kw: pytest.fail("invalid path"))
    with pytest.raises(ValueError, match="invalid_windows_probe_path"):
        transport.read_windows_entries([path], "pwsh.exe")


def test_real_windows_transport_preserves_ntfs_metadata_readonly():
    """Opt-in transport smoke test; never create a Windows fixture or execute cmd.exe."""
    import os
    import uuid
    from pathlib import Path

    if os.environ.get("VLP_TEST_WINDOWS_READONLY_PROBE") != "1":
        pytest.skip("opt in on a WSL host with Windows PowerShell 7 installed")
    path = Path("/mnt/c/Windows/System32/cmd.exe")
    stat = path.stat()
    root = "C:\\"
    filename = r"C:\Windows\System32\cmd.exe"
    absent = root + "probe-does-not-create-" + uuid.uuid4().hex
    result = transport.windows_entry_reader()([root, filename, absent])
    assert result[root]["kind"] == "directory"
    assert result[absent]["kind"] == "missing"
    assert result[filename]["kind"] == "file"
    signature = probe._signature(result[filename])
    assert signature["size_bytes"] == stat.st_size and signature["mtime_ns"] == stat.st_mtime_ns
