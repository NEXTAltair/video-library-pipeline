"""Execute the shipped mover with a disposable Linux filesystem adapter.

No installation, production recordings or Windows scripts are changed. The
adapter replaces only Windows long-path primitives, not the bundle algorithm.
"""
import json
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(not shutil.which("pwsh"), reason="PowerShell not available")
KINDS = {"program_txt": ".mp4.program.txt", "recording_json": ".mp4.recording.json", "psc": ".psc", "vtt": ".vtt", "video": ".mp4"}


def fixture(tmp_path, *, companions=True, stem="番組_" + "長" * 40, legacy=False):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    for name in ("apply_move_plan.ps1", "_move_bundle.ps1"):
        shutil.copy(ROOT / "assets/windows-scripts" / name, scripts / name)
    # Drive-rooted plan paths exercise production validation; .NET calls receive
    # actual temporary host paths through the existing long-path seam.
    adapter = r"""
New-PSDrive -Name X -PSProvider FileSystem -Root $env:BUNDLE_ROOT | Out-Null
function Convert-ToLongPathLiteral([string]$Path) {
  if ($Path -match '^X:[\\/]') { return $env:BUNDLE_ROOT + '/' + $Path.Substring(3).Replace('\','/') }
  return $Path
}
function Test-PathFileLong([string]$Path) { [IO.File]::Exists((Convert-ToLongPathLiteral $Path)) }
function Test-PathDirLong([string]$Path) { [IO.Directory]::Exists((Convert-ToLongPathLiteral $Path)) }
function Test-PathAnyLong([string]$Path) { (Test-PathFileLong $Path) -or (Test-PathDirLong $Path) }
function Ensure-DirectoryLong([string]$Path) { [void][IO.Directory]::CreateDirectory((Convert-ToLongPathLiteral $Path)) }
function Move-FileLong([string]$Src,[string]$Dst,[bool]$Overwrite) {
  $script:counter++
  if ($env:BUNDLE_FAIL -and $script:counter -eq [int]$env:BUNDLE_FAIL) { throw 'injected_move_failure' }
  if ($env:BUNDLE_ROLLBACK_FAIL -and $script:counter -gt [int]$env:BUNDLE_FAIL) { throw 'injected_rollback_failure' }
  [IO.File]::Move((Convert-ToLongPathLiteral $Src),(Convert-ToLongPathLiteral $Dst),$false)
  if ($env:BUNDLE_LATE_SOURCE -and $Src.EndsWith('.mp4')) {
    [IO.File]::WriteAllText((Convert-ToLongPathLiteral ($Src + '.program.txt')), 'late metadata')
  }
  if ($env:BUNDLE_LATE_DESTINATION -and $Src.EndsWith('.mp4')) {
    [IO.File]::WriteAllText((Convert-ToLongPathLiteral ($Dst + '.program.txt')), 'late metadata')
  }
}
"""
    (scripts / "_long_path_utils.ps1").write_text(adapter)
    (tmp_path / "src").mkdir()
    op = {"src": rf"X:\src\{stem}.mp4", "dst": rf"X:\dst\{stem}.mp4", "path_id": "video-id"}
    kinds = list(KINDS) if companions else ["video"]
    for kind in kinds:
        (tmp_path / "src" / (stem + KINDS[kind])).write_bytes(kind.encode())
    # Use PowerShell's exact timestamp serialization to avoid fixture precision drift.
    members = []
    from datetime import datetime, timezone
    for kind in kinds:
        st = (tmp_path / "src" / (stem + KINDS[kind])).stat()
        members.append({"kind": kind, "src": rf"X:\src\{stem}{KINDS[kind]}", "dst": rf"X:\dst\{stem}{KINDS[kind]}", "precondition": {"size_bytes": st.st_size, "mtime_utc": datetime.fromtimestamp(st.st_mtime_ns // 1_000_000_000, timezone.utc).strftime('%Y-%m-%dT%H:%M:%S') + f'.{st.st_mtime_ns % 1_000_000_000 // 100:07d}+00:00' }})
    op["precondition"] = members[-1]["precondition"]
    if not legacy:
        op.update(companion_cohort=[k for k in kinds if k != "video"], members=members)
    return scripts, op, stem


def run(tmp_path, scripts, op, *args, fail=None, rollback_fail=False):
    import os
    plan = tmp_path / "plan.jsonl"
    plan.write_text(json.dumps(op, ensure_ascii=False) + "\n")
    out = tmp_path / "receipt.jsonl"
    env = {**os.environ, "BUNDLE_ROOT": str(tmp_path), "BUNDLE_FAIL": str(fail or ""), "BUNDLE_ROLLBACK_FAIL": "1" if rollback_fail else ""}
    result = subprocess.run(["pwsh", "-NoProfile", "-File", str(scripts / "apply_move_plan.ps1"), "-PlanJsonl", str(plan), "-OutJsonl", str(out), "-OpsRoot", str(tmp_path / "ops"), *args], capture_output=True, text=True, env=env)
    assert result.returncode == 0, result.stderr
    records = [json.loads(line) for line in out.read_text().splitlines()]
    rows = [r for r in records if r.get("op") == "move"]
    assert len(rows) == 1, (records, result.stderr)
    return rows[0], records


@pytest.mark.parametrize("companions,legacy", [(True, False), (False, False), (False, True)])
def test_complete_bundle_and_legacy(tmp_path, companions, legacy):
    scripts, op, stem = fixture(tmp_path, companions=companions, legacy=legacy)
    row, records = run(tmp_path, scripts, op)
    assert row["ok"] and row["bundle_complete"]
    assert len([r for r in records if r.get("op") == "move_member"]) == (5 if companions else 1)
    assert not list((tmp_path / "src").iterdir())
    assert (tmp_path / "dst" / (stem + ".mp4")).exists()


@pytest.mark.parametrize("rename", [False, True])
def test_companion_collision_and_shared_suffix(tmp_path, rename):
    scripts, op, stem = fixture(tmp_path)
    (tmp_path / "dst").mkdir()
    collision = tmp_path / "dst" / (stem + ".psc")
    collision.write_text("unrelated")
    row, _ = run(tmp_path, scripts, op, *( ["-OnDstExists", "rename_suffix"] if rename else []))
    assert row["ok"] is rename
    assert collision.read_text() == "unrelated"
    if rename:
        assert sorted(p.name for p in (tmp_path / "dst").iterdir() if "__dup1" in p.name) == sorted(stem + "__dup1" + suffix for suffix in KINDS.values())
    else:
        assert len(list((tmp_path / "src").iterdir())) == 5


@pytest.mark.parametrize("change", ["modify", "remove", "add", "ambiguous", "reparse", "dangling", "legacy"])
def test_reject_changed_cohort_or_member(tmp_path, change):
    scripts, op, stem = fixture(tmp_path, companions=change != "add", legacy=change == "legacy")
    member = tmp_path / "src" / (stem + ".psc")
    if change == "modify": member.write_text("changed")
    if change == "remove": member.unlink()
    if change == "add": member.write_text("new")
    if change == "ambiguous": (tmp_path / "src" / (stem + ".mkv")).write_text("another video")
    if change in {"reparse", "dangling"}:
        member.unlink()
        member.symlink_to(tmp_path / "missing" if change == "dangling" else tmp_path / "src" / (stem + ".mp4"))
    row, records = run(tmp_path, scripts, op)
    assert not row["ok"]
    assert not [r for r in records if r.get("op") == "move_member"]
    assert not (tmp_path / "dst").exists()


@pytest.mark.parametrize("rollback_fail", [False, True])
def test_partial_failure_rolls_back_and_reports_remaining_effects(tmp_path, rollback_fail):
    scripts, op, stem = fixture(tmp_path)
    row, records = run(tmp_path, scripts, op, fail=2, rollback_fail=rollback_fail)
    assert not row["ok"] and not row["bundle_complete"]
    assert row["error"] == "injected_move_failure"
    assert bool(row["rollback_errors"]) is rollback_fail
    assert len(row["partial_effects"]) == 5
    assert (tmp_path / "src" / (stem + ".mp4")).exists()
    assert (tmp_path / "src" / (stem + ".mp4.program.txt")).exists() is not rollback_fail
    assert any(r.get("op") == "rollback_member" for r in records)


def test_dry_run_lists_bundle_without_creating_destination(tmp_path):
    scripts, op, stem = fixture(tmp_path)
    row, records = run(tmp_path, scripts, op, "-DryRun")
    assert row["ok"] and row["dry_run"] and len(row["members"]) == 5
    assert len(list((tmp_path / "src").iterdir())) == 5
    assert not (tmp_path / "dst").exists()
    assert not [r for r in records if r.get("op") == "move_member"]


def test_shipped_scripts_are_identical():
    for name in ("apply_move_plan.ps1", "_move_bundle.ps1"):
        assert (ROOT / "assets/windows-scripts" / name).read_bytes() == (ROOT / "templates/windows-scripts" / name).read_bytes()


def test_destination_foreign_video_stem_renames_bundle(tmp_path):
    scripts, op, stem = fixture(tmp_path)
    (tmp_path / "dst").mkdir()
    (tmp_path / "dst" / (stem + ".mkv")).write_text("foreign video")
    row, _ = run(tmp_path, scripts, op, "-OnDstExists", "rename_suffix")
    assert row["ok"] and "__dup1.mp4" in row["dst"]
    assert not (tmp_path / "dst" / (stem + ".psc")).exists()


def test_total_path_longer_than_260_characters(tmp_path):
    scripts, op, stem = fixture(tmp_path, stem="日本語_" + "長" * 65)
    # Individual UTF-8 components stay within Linux NAME_MAX, total path exceeds
    # Windows MAX_PATH. Only the Windows long-path primitive itself is adapted.
    nested = "nested_" + "n" * 190
    (tmp_path / "src" / nested).mkdir()
    for p in list((tmp_path / "src").iterdir()):
        if p.is_file(): p.rename(tmp_path / "src" / nested / p.name)
    for member in op["members"]:
        member["src"] = member["src"].replace("src\\", "src\\" + nested + "\\")
    op["src"] = op["src"].replace("src\\", "src\\" + nested + "\\")
    assert len(op["src"]) > 260
    row, _ = run(tmp_path, scripts, op)
    assert row["ok"] and len(row["members"]) == 5


def test_native_capture_round_trip_preserves_signatures(tmp_path):
    import errno
    import os
    from video_pipeline.domain.companion_bundle import capture_bundle
    scripts, op, stem = fixture(tmp_path)
    shutil.copy(ROOT / "templates/windows-scripts/inspect_move.ps1", scripts / "inspect_move.ps1")
    result = subprocess.run(["pwsh", "-NoProfile", "-File", str(scripts / "inspect_move.ps1"), "-Src", op["src"], "-Dst", op["dst"]], capture_output=True, text=True, env={**os.environ, "BUNDLE_ROOT": str(tmp_path)})
    assert result.returncode == 0, result.stderr
    observed = json.loads(result.stdout)
    assert observed["bundle_verified"] and len(observed["members"]) == 5
    for m in observed["members"]:
        assert isinstance(m["precondition"]["size_bytes"], int)
        assert m["precondition"]["mtime_utc"]
    def too_long(_):
        raise OSError(errno.ENAMETOOLONG, "Windows long name")
    fields = capture_bundle(op["src"], op["dst"], localize=too_long, windows_probe=lambda *_: observed)
    assert len(fields["members"]) == 5 and set(fields["companion_cohort"]) == {"psc", "vtt", "program_txt", "recording_json"}


def test_reparse_destination_parent_is_rejected_before_mkdir(tmp_path):
    scripts, op, stem = fixture(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / "dst").symlink_to(outside, target_is_directory=True)
    row, _ = run(tmp_path, scripts, op)
    assert not row["ok"] and row["error"] == "path_reparse_point"
    assert not list(outside.iterdir())


def test_same_size_submillisecond_member_change_is_rejected(tmp_path):
    import os
    scripts, op, stem = fixture(tmp_path)
    p = tmp_path / "src" / (stem + ".psc")
    stat = p.stat()
    os.utime(p, ns=(stat.st_atime_ns, stat.st_mtime_ns + 100_000))
    row, _ = run(tmp_path, scripts, op)
    assert not row["ok"] and row["error"] == "source_changed"


def test_default_native_stdin_transport_round_trip(tmp_path, monkeypatch):
    import errno
    from video_pipeline.platform import native_companion_probe as native
    from video_pipeline.domain.companion_bundle import capture_bundle
    scripts, op, stem = fixture(tmp_path)
    shutil.copy(ROOT / "templates/windows-scripts/inspect_move.ps1", scripts / "inspect_move.ps1")
    (tmp_path / "templates").mkdir()
    (tmp_path / "templates/windows-scripts").symlink_to(scripts, target_is_directory=True)
    monkeypatch.setattr(native, "__file__", str(tmp_path / "py/video_pipeline/platform/native_companion_probe.py"))
    monkeypatch.setattr(native, "find_pwsh7_executable", lambda: shutil.which("pwsh"))
    monkeypatch.setenv("BUNDLE_ROOT", str(tmp_path))
    def too_long(_):
        raise OSError(errno.ENAMETOOLONG, "Windows long name")
    fields = capture_bundle(op["src"], op["dst"], localize=too_long)
    assert len(fields["members"]) == 5
    assert all(isinstance(m["precondition"]["size_bytes"], int) for m in fields["members"])
    assert len(list((tmp_path / "src").iterdir())) == 5
    assert not (tmp_path / "dst").exists()


@pytest.mark.parametrize("side", ["SOURCE", "DESTINATION"])
def test_late_companion_during_final_video_move_prevents_parent_success(tmp_path, monkeypatch, side):
    scripts, op, stem = fixture(tmp_path, companions=False)
    monkeypatch.setenv("BUNDLE_LATE_" + side, "1")
    row, records = run(tmp_path, scripts, op)
    assert not row["ok"] and not row["bundle_complete"]
    assert row["error"] == "companion_cohort_changed_after_move"
    assert any(r.get("op") == "rollback_member" for r in records)
    assert (tmp_path / "src" / (stem + ".mp4")).exists()
    assert not (tmp_path / "dst" / (stem + ".mp4")).exists()
    assert any(e["kind"] == "program_txt" and (e["source_exists"] or e["destination_exists"]) for e in row["partial_effects"])


def test_m4v_stem_owner_is_not_stolen(tmp_path):
    scripts, op, stem = fixture(tmp_path)
    (tmp_path / "src" / (stem + ".m4v")).write_text("another recording")
    row, _ = run(tmp_path, scripts, op)
    assert not row["ok"] and row["error"] == "ambiguous_companion_owner"


@pytest.mark.parametrize("suffix", [".mp4", ".psc"])
def test_native_collision_remains_nonfatal_preflight_result(tmp_path, monkeypatch, suffix):
    import errno
    import os
    from video_pipeline.domain.move_preflight import inspect_move
    scripts, op, stem = fixture(tmp_path)
    shutil.copy(ROOT / "templates/windows-scripts/inspect_move.ps1", scripts / "inspect_move.ps1")
    (tmp_path / "dst").mkdir()
    (tmp_path / "dst" / (stem + suffix)).write_text("occupied")
    cp = subprocess.run(["pwsh", "-NoProfile", "-File", str(scripts / "inspect_move.ps1"), "-Src", op["src"], "-Dst", op["dst"]], capture_output=True, text=True, env={**os.environ, "BUNDLE_ROOT": str(tmp_path)})
    assert cp.returncode == 0, cp.stderr
    observed = json.loads(cp.stdout)
    assert observed["ok"] and observed["destination_exists"] and not observed["bundle_verified"]
    assert observed["bundle_error"] == "destination_exists"
    def too_long(_self, **_kwargs):
        raise OSError(errno.ENAMETOOLONG, "native long name")
    monkeypatch.setattr(Path, "stat", too_long)
    assert inspect_move(op["src"], op["dst"], {}, windows_probe=lambda *_: observed) == (None, "destination_exists")
