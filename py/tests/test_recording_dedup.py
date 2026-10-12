"""User policy and the real failure boundaries of automated duplicate quarantine."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path

import pytest

from video_pipeline.db.mediaops_schema import connect_db, create_schema_if_needed
from video_pipeline.db.file_identity import ensure_identity_schema, ensure_location, observe_file
from video_pipeline.domain.recording_dedup import broadcast_kind, plan_recordings
from video_pipeline.workflows.dedup import make_plan, apply_plan, restore_plan, recover_quarantines


@pytest.fixture
def library(tmp_path, monkeypatch):
    import video_pipeline.domain.recording_dedup as policy
    monkeypatch.setattr(policy, "wsl_to_windows_path", str)
    root = tmp_path / "library"
    root.mkdir()
    db = tmp_path / "mediaops.sqlite"
    con = connect_db(str(db))
    create_schema_if_needed(con); ensure_identity_schema(con)
    con.execute("INSERT INTO runs(run_id,kind,started_at) VALUES('fixture','inventory','old')")
    con.commit()
    yield con, db, root, tmp_path / "ops"
    con.close()


def add_recording(library, name, flag, *, title="Example Show", ep="1", subtitle="Opening",
                  confidence=0.95, review=False, pixels=1920*1080, size=200, observe=True):
    con, _, root, _ = library
    path = root / name
    path.write_bytes(bytes(size))
    os.utime(path, (1_700_000_000, 1_700_000_000))
    pid = ensure_location(con, str(path))
    metadata = {"confidence": confidence, "pixels": pixels}
    if flag is not None:
        metadata["is_rebroadcast_flag"] = flag
    con.execute("""INSERT INTO path_metadata(path_id,source,data_json,updated_at,program_title,
        episode_no,subtitle,needs_review) VALUES(?,'rule_based',?,'old',?,?,?,?)""",
        (pid, json.dumps(metadata), title, ep, subtitle, int(review)))
    if observe:
        stamp = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()
        observe_file(con, pid, size, stamp, "fixture")
    con.commit()
    return path, pid


def quality_probe(path):
    return {"duration": 1500.0, "pixels": 1920*1080}


def planner(library, **kwargs):
    con, _, root, _ = library
    return plan_recordings(con, [root], probe=quality_probe, **kwargs)


def test_original_and_repeat_are_both_retained_even_if_bytes_match(library):
    add_recording(library, "original.mp4", False)
    add_recording(library, "repeat.mp4", True)
    plan = planner(library)
    assert plan["moves"] == []
    assert plan["decisions"] == {"keep": 2}


def test_each_broadcast_kind_is_deduplicated_independently(library):
    original, _ = add_recording(library, "original-best.mp4", False, size=400)
    add_recording(library, "original-low.mp4", False, size=200)
    repeat, _ = add_recording(library, "repeat-best.mp4", True, size=400)
    add_recording(library, "repeat-low.mp4", True, size=200)
    plan = planner(library)
    assert len(plan["moves"]) == 2
    assert {m["keep"]["path"] for m in plan["moves"]} == {str(original), str(repeat)}
    assert {m["broadcast_kind"] for m in plan["moves"]} == {"original", "repeat"}
    assert all(not Path(m["dst"]).is_relative_to(library[2]) for m in plan["moves"])


def test_unknown_is_not_original_or_inferred_from_recording_date(library):
    add_recording(library, "first.mp4", None)
    add_recording(library, "second.mp4", None)
    assert planner(library)["moves"] == []
    assert broadcast_kind({}, [{"is_rebroadcast_flag": True}, {"is_rebroadcast_flag": False}])[0] == "unknown"
    assert broadcast_kind({"is_rebroadcast_flag": "false"}, [])[0] == "unknown"
    assert broadcast_kind({}, [{"is_rebroadcast_flag": None}])[0] == "unknown"


def test_conflicting_epg_and_metadata_is_held(library):
    assert broadcast_kind({"is_rebroadcast_flag": False}, [{"is_rebroadcast_flag": True}])[1] == "conflicting_broadcast_flags"
    assert broadcast_kind({"path": "show[再].mp4", "is_rebroadcast_flag": False}, [])[0] == "unknown"
    # Explicit repeat markers must never override an explicit original flag.


@pytest.mark.parametrize("epg_flag,epg_title", [(True, "Example Show"), (False, "Example Show Season 2"), (None, "Example Show")])
def test_linked_epg_conflicts_are_not_overridden_by_metadata(library, epg_flag, epg_title):
    add_recording(library, "a.mp4", False)
    _, pid = add_recording(library, "b.mp4", False)
    con = library[0]
    con.execute("INSERT INTO programs(program_id,program_key,canonical_title,created_at) VALUES('pg','key',?,'old')", (epg_title,))
    con.execute("INSERT INTO broadcasts(broadcast_id,program_id,is_rebroadcast_flag,created_at) VALUES('bc','pg',?,'old')", (epg_flag,))
    con.execute("INSERT INTO path_programs VALUES(?,'pg','bc','epg','old')", (pid,))
    con.commit()
    assert planner(library)["moves"] == []


@pytest.mark.parametrize("change", ["low_confidence", "needs_review", "held", "changed", "recent"])
def test_uncertain_cohort_does_not_discard_any_member(library, change):
    add_recording(library, "safe.mp4", False)
    path, pid = add_recording(library, "uncertain.mp4", False,
        confidence=0.4 if change == "low_confidence" else 0.95, review=change == "needs_review")
    con = library[0]
    if change == "held":
        con.execute("INSERT INTO recording_holds VALUES(?,'audit','{}','old',NULL)", (pid,)); con.commit()
    elif change == "changed":
        path.write_bytes(b"replaced")
    elif change == "recent":
        os.utime(path, None)
    assert planner(library)["moves"] == []


def test_do_not_collapse_series_seasons_or_different_episodes(library):
    for name, title, ep in [("a", "Example Show", "1"), ("b", "Example Show Season 2", "1"), ("c", "Example Show", "2")]:
        add_recording(library, name + ".mp4", False, title=title, ep=ep)
    assert planner(library)["groups_total"] == 0


def test_subtitle_conflict_and_duration_difference_are_not_automated(library):
    add_recording(library, "a.mp4", False)
    add_recording(library, "b.mp4", False, subtitle="Different edition")
    assert planner(library)["moves"] == []
    library[0].execute("UPDATE path_metadata SET subtitle='Opening'"); library[0].commit()
    lengths = iter([1500, 1300])
    result = plan_recordings(library[0], [library[2]], probe=lambda p: {"duration": next(lengths), "pixels": 100})
    assert result["moves"] == []
    assert "duration_or_edition_mismatch" in result["defer_reasons"]


def test_resolution_uses_probe_not_metadata_or_filesize(library):
    high, _ = add_recording(library, "high.mp4", False, size=100)
    add_recording(library, "low.mp4", False, size=4000)
    plan = plan_recordings(library[0], [library[2]], probe=lambda p: {"duration": 1500, "pixels": 200 if "high" in p else 100})
    assert plan["moves"][0]["keep"]["path"] == str(high)


def test_preview_is_read_only_and_rotation_reaches_later_groups(library):
    for ep in (1, 2, 3):
        add_recording(library, f"{ep}-a.mp4", False, ep=str(ep))
        add_recording(library, f"{ep}-b.mp4", False, ep=str(ep))
    con, db, root, ops = library
    before = db.read_bytes()
    plan, _ = make_plan(db, ops, [root], max_groups=1, probe=quality_probe)
    assert plan["groups_checked"] == 1 and plan["groups_remaining"] == 2
    assert db.read_bytes() == before
    second = plan_recordings(con, [root], max_groups=1, group_offset=plan["next_group_offset"], probe=quality_probe)
    assert second["groups"][0]["episode_key"] != plan["groups"][0]["episode_key"]
    assert not (ops / "duplicates" / "checkpoint.json").exists()


@pytest.mark.parametrize("change", ["keeper", "source", "metadata", "destination", "plan"])
def test_apply_rechecks_plan_keeper_source_metadata_and_destination(library, change):
    keeper, _ = add_recording(library, "keep.mp4", False, size=400)
    source, pid = add_recording(library, "drop.mp4", False)
    con, db, root, ops = library
    plan, directory = make_plan(db, ops, [root], probe=quality_probe)
    if change == "keeper":
        keeper.unlink()
    elif change == "source":
        source.write_bytes(b"changed")
    elif change == "metadata":
        con.execute("UPDATE path_metadata SET episode_no='2' WHERE path_id=?", (pid,)); con.commit()
    elif change == "destination":
        from video_pipeline.platform.pathscan_common import windows_to_wsl_path
        destination = Path(windows_to_wsl_path(plan["moves"][0]["dst"]))
        destination.parent.mkdir(parents=True); destination.write_bytes(b"existing")
    else:
        (directory / "plan.json").write_text("{}")
    with pytest.raises(ValueError):
        apply_plan(db, ops, directory, runner=lambda *_: pytest.fail("must not execute Windows moves"))


def simulated_windows_move(_script, args):
    from video_pipeline.platform.pathscan_common import windows_to_wsl_path
    # POSIX test paths are backslash-shaped by the Windows boundary. Fixtures only.
    def local(value):
        return Path(windows_to_wsl_path(value).replace("\\", "/"))
    plan = local(args[args.index("-PlanJsonl") + 1])
    receipt = local(args[args.index("-OutJsonl") + 1])
    rows = []
    for line in plan.read_text().splitlines():
        row = json.loads(line)
        src, dst = local(row["src"]), local(row["dst"])
        dst.parent.mkdir(parents=True, exist_ok=True)
        assert not dst.exists()
        src.rename(dst)
        rows.append({"op": "move", "src": row["src"], "dst": row["dst"], "file_id": row["file_id"], "ok": True, "ts": "2026-10-06T00:00:00Z"})
    receipt.write_text("".join(json.dumps(r) + "\n" for r in rows))


def test_apply_restore_and_receipt_recovery_preserve_file_identity(library, monkeypatch):
    # Use native-shaped paths throughout this POSIX fixture only.
    import video_pipeline.domain.recording_dedup as policy
    monkeypatch.setattr(policy, "wsl_to_windows_path", str)
    keeper, _ = add_recording(library, "keep.mp4", False, size=400)
    source, pid = add_recording(library, "drop.mp4", False)
    con, db, root, ops = library
    fid = con.execute("SELECT file_id FROM path_file_state WHERE path_id=?", (pid,)).fetchone()[0]
    plan, directory = make_plan(db, ops, [root], probe=quality_probe)
    result = apply_plan(db, ops, directory, runner=simulated_windows_move)
    assert result["ok"] and result["moved"] == 1
    assert keeper.exists() and not source.exists()
    assert con.execute("SELECT path FROM paths WHERE path_id=?", (pid,)).fetchone()[0] == str(source)
    assert con.execute("SELECT needs_review FROM path_metadata WHERE path_id=?", (pid,)).fetchone()[0] == 0
    assert con.execute("SELECT count(*) FROM move_receipts").fetchone()[0] == 1
    assert planner(library)["groups_total"] == 0  # quarantine is never a new candidate
    (directory / "db_committed.json").unlink()  # simulate interruption after commit
    assert recover_quarantines(db, ops) == [plan["run_id"]]
    assert con.execute("SELECT count(*) FROM move_receipts").fetchone()[0] == 1
    assert restore_plan(db, ops, directory)["restore_operations"] == 1
    assert restore_plan(db, ops, directory, apply=True, runner=simulated_windows_move)["ok"]
    assert source.exists() and keeper.exists()
    assert con.execute("SELECT file_id FROM path_file_state WHERE path_id=?", (pid,)).fetchone()[0] == fid
    assert con.execute("PRAGMA foreign_key_check").fetchall() == []


@pytest.mark.parametrize("changed", ["utc_rounding", "size_bytes", "mtime_ns"])
def test_restore_uses_exact_integer_signature_not_rounded_utc(library, monkeypatch, changed):
    import video_pipeline.workflows.dedup as workflow
    add_recording(library, "keep.mp4", False, size=400)
    add_recording(library, "drop.mp4", False)
    _, db, root, ops = library
    _, directory = make_plan(db, ops, [root], probe=quality_probe)
    assert apply_plan(db, ops, directory, runner=simulated_windows_move)["ok"]
    real_signature = workflow.file_signature

    def native_rounding(path):
        sig = real_signature(path)
        if changed == "utc_rounding":
            sig["mtime_utc"] = "2023-11-14T22:13:20.000001+00:00"
        else:
            sig[changed] += 1
        return sig

    monkeypatch.setattr(workflow, "file_signature", native_rounding)
    if changed == "utc_rounding":
        assert restore_plan(db, ops, directory)["restore_operations"] == 1
    else:
        with pytest.raises(ValueError, match="quarantined recording changed"):
            restore_plan(db, ops, directory)


@pytest.mark.parametrize("unsafe", ["occupied", "source_reparse", "target_reparse", "unchecked", "escape"])
def test_native_restore_never_accepts_unproven_ancestry_or_overwrite(unsafe):
    from video_pipeline.workflows.dedup import native_restore_signature
    from video_pipeline.platform.native_file_signatures import native_path_key
    local = Path("/mnt/z/_AI_WORK/duplicates/quarantine_v3/group/repeat/id/show.mp4")
    target = Path("/mnt/z/media/show.mp4")
    signature = {"size_bytes":200,"mtime_ns":1700000000000000000,"mtime_utc":"2023-11-14T22:13:20+00:00"}
    cache = {
        native_path_key(local): {"exists":True,"ancestors_checked":True,"signature":signature},
        native_path_key(target): {"exists":False,"ancestors_checked":True},
    }
    assert native_restore_signature(local,target,local,[Path("/mnt/z/media")],cache) == signature
    roots = [Path("/mnt/z/media")]
    if unsafe == "occupied":
        cache[native_path_key(target)]["exists"] = True
    elif unsafe == "unchecked":
        cache[native_path_key(target)].pop("ancestors_checked")
    elif unsafe == "escape":
        roots = [Path("/mnt/z/other")]
    else:
        entry = local if unsafe == "source_reparse" else target
        cache[native_path_key(entry)]["error"] = "native_reparse_ancestor"
    with pytest.raises(ValueError):
        native_restore_signature(local,target,local,roots,cache)
