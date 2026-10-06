"""Failures observed in production, plus receipt/restart boundaries."""
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
import pytest
from mediaops_schema import connect_db, create_schema_if_needed
from pathscan_common import path_id_for
from video_pipeline.db.file_identity import ensure_identity_schema, ensure_location, observe_file, record_move
from video_pipeline.domain.move_preflight import inspect_move
from title_resolution import CanonicalTitleSources, suggest_canonical_title
from run_metadata_batches_promptv1 import parse_episode


@pytest.fixture
def db(tmp_path):
    con = connect_db(str(tmp_path / "db.sqlite"))
    create_schema_if_needed(con); ensure_identity_schema(con)
    con.execute("INSERT INTO runs(run_id,kind,started_at) VALUES('test','inventory','2026-10-06')")
    con.commit()
    yield con
    con.close()


def test_move_then_source_reappears_cannot_erase_destination(db):
    src, dst = r"B:\未視聴\show.mp4", r"B:\VideoLibrary\Show\show.mp4"
    pid = ensure_location(db,src)
    fid = observe_file(db,pid,100,"2026-10-06T00:00:00Z","test")
    db.execute("INSERT INTO path_metadata(path_id,source,data_json,updated_at,program_title) VALUES(?,'rule_based','{}','2026-10-06','Show')",(pid,))
    row = {"src":src,"dst":dst,"file_id":fid,"ts":"2026-10-06T00:10:00Z"}
    assert record_move(db,row,"receipt1","test")
    assert not record_move(db,row,"receipt1","test")
    assert ensure_location(db,src) == pid
    replacement = observe_file(db,pid,100,"2026-10-06T00:00:00Z","test")
    assert replacement != fid  # equal size/date is not proof of equal content
    assert db.execute("SELECT path FROM paths WHERE path_id=?",(pid,)).fetchone()[0] == src
    dp = ensure_location(db,dst)
    assert db.execute("SELECT file_id FROM path_file_state WHERE path_id=?",(dp,)).fetchone()[0] == fid
    assert db.execute("SELECT needs_review FROM path_metadata WHERE path_id=?",(pid,)).fetchone()[0] == 1
    assert db.execute("SELECT count(*) FROM events").fetchone()[0] == 1
    assert db.execute("SELECT content_hash FROM files WHERE file_id=?",(fid,)).fetchone()[0] is None


def test_legacy_id_at_destination_is_not_stolen_by_inventory(db):
    src,dst = r"B:\未視聴\show.mp4",r"B:\VideoLibrary\Show\show.mp4"
    pid = ensure_location(db,src)
    db.execute("UPDATE paths SET path=? WHERE path_id=?",(dst,pid))
    new = ensure_location(db,src)
    assert new != pid
    assert db.execute("SELECT path FROM paths WHERE path_id=?",(pid,)).fetchone()[0] == dst


def test_destination_file_identity_never_merged(db):
    src,dst = r"B:\未視聴\a.mp4",r"B:\VideoLibrary\a.mp4"
    for path in [src,dst]:
        observe_file(db,ensure_location(db,path),100,"same","test")
    with pytest.raises(ValueError,match="refusing to merge"):
        record_move(db,{"src":src,"dst":dst},"receipt","test")


def test_dry_run_move_receipt_cannot_write_db(db,tmp_path):
    db.commit()
    log = tmp_path / "dry.jsonl"
    log.write_text(json.dumps({"_meta":{"dry_run":True}})+"\n"+json.dumps({"op":"move","src":"x","dst":"y","ok":True})+"\n")
    result = subprocess.run([sys.executable,str(Path(__file__).parents[1]/"update_db_paths_from_move_apply.py"),"--db",str(tmp_path/"db.sqlite"),"--applied",str(log)],capture_output=True,text=True)
    assert result.returncode == 0, result.stderr
    assert db.execute("SELECT count(*) FROM move_receipts").fetchone()[0] == 0


def test_preflight_excludes_collision_and_changed_source(tmp_path):
    src,dst = tmp_path/"src.mp4",tmp_path/"dst.mp4"
    src.write_bytes(b"recording")
    snapshot = {"size":src.stat().st_size,"mtimeUtc":datetime.fromtimestamp(src.stat().st_mtime,timezone.utc).isoformat()}
    pre,reason = inspect_move(str(src),str(dst),snapshot)
    assert pre and reason is None
    snapshot["mtimeUtc"] = snapshot["mtimeUtc"].replace("+00:00","4+00:00")
    assert inspect_move(str(src),str(dst),snapshot)[1] is None
    dst.write_bytes(b"different")
    assert inspect_move(str(src),str(dst),snapshot)[1] == "destination_exists"
    src.write_bytes(b"new")
    assert inspect_move(str(src),str(dst),snapshot)[1] == "source_changed"


@pytest.mark.parametrize("title,base",[("機動戦士ガンダムSEED DESTINY 17. PHASE-17","機動戦士ガンダム"),("機動戦士ガンダムユニコーン RE:0096","機動戦士ガンダム"),("仮面ライダーマイス","仮面ライダー")])
def test_production_work_titles_not_collapsed_to_franchise(title,base):
    sources = CanonicalTitleSources((base,),(base,),frozenset(),(base,))
    suggestion,_ = suggest_canonical_title(title,sources,min_extra_chars=3)
    assert suggestion != base


def test_seed_destiny_episode():
    assert parse_episode('機動戦士ガンダムSEED DESTINY 17. PHASE-17') == 17


def test_missing_explicit_routes_fails_before_plan_creation(tmp_path):
    out = tmp_path / "plan.jsonl"
    result = subprocess.run([sys.executable,str(Path(__file__).parents[1]/"make_move_plan_from_inventory.py"),"--db","unused","--inventory","unused","--source-root",r"B:\未視聴","--dest-root",r"B:\VideoLibrary","--drive-routes",str(tmp_path/"missing.yaml"),"--out",str(out)],capture_output=True,text=True)
    assert result.returncode != 0
    assert "drive routes file not found" in result.stderr
    assert not out.exists()


def test_repair_uses_canonical_columns_and_keeps_equal_sized_files_separate(db,tmp_path,monkeypatch):
    import repair_legacy_records as repair_mod
    src,dst = r"B:\未視聴\show.mp4",r"B:\VideoLibrary\show.mp4"
    wrong = r"B:\VideoLibrary\BS11ガンダムアワー 機動戦士ガンダムSEED DESTINY 17. PHASE-17.mp4"
    local = tmp_path / "present.mp4"; local.write_bytes(b"recording")
    monkeypatch.setattr(repair_mod,"windows_to_wsl_path",lambda _p:str(local))
    pid = ensure_location(db,src); fid = observe_file(db,pid,9,"old","test")
    wp = ensure_location(db,wrong)
    db.execute("INSERT INTO path_metadata(path_id,source,data_json,updated_at,program_title) VALUES(?,'rule_based','{}','old','機動戦士ガンダム')",(wp,))
    db.commit()
    title = {"filename":wrong.split('\\')[-1],"reproducedInitial":{"program_title":"機動戦士ガンダムSEED DESTINY 17. PHASE-17"},"saved":{"program_title":"機動戦士ガンダム"}}
    result = repair_mod.repair(tmp_path / "db.sqlite",[{"src":src,"dst":dst}],[title])
    assert result["ok"]
    assert len(result["changes"]) == 2
    dp = ensure_location(db,dst)
    assert db.execute("SELECT file_id FROM path_file_state WHERE path_id=?",(dp,)).fetchone()[0] != fid
    md = db.execute("SELECT program_title,episode_no FROM path_metadata WHERE path_id=?",(wp,)).fetchone()
    assert tuple(md) == ("機動戦士ガンダムSEED DESTINY","17")
    again = repair_mod.repair(tmp_path / "db.sqlite",[{"src":src,"dst":dst}],[title])
    assert again["changes"] == []


def test_interrupted_apply_recovers_complete_receipts_without_guessing_or_repeating(db,tmp_path):
    from daily_cli import recover_receipts
    from video_pipeline.workflows.store import sha256_file
    src,dst = r"B:\未視聴\show.mp4",r"B:\VideoLibrary\Show\show.mp4"
    pid = ensure_location(db,src); fid = observe_file(db,pid,100,"old","test"); db.commit()
    apply = tmp_path/"runs"/"run_fixture"/"apply"; apply.mkdir(parents=True)
    plan = apply/"plan.jsonl"; plan.write_text('{"src":"first"}\n{"src":"second"}\n')
    log = apply/"receipts.jsonl"
    success = {"op":"move","src":src,"dst":dst,"file_id":fid,"ok":True,"ts":"2026-10-06T00:00:00Z"}
    log.write_text(json.dumps(success)+'\n{"op":')
    (apply/"apply_intent.json").write_text(json.dumps({"runId":"run_fixture","plan":str(plan),"planChecksum":sha256_file(plan),"receiptPath":str(log),"db":str(tmp_path/"db.sqlite")}))
    for _ in range(2):
        with pytest.raises(RuntimeError,match="途中で切れ"):
            recover_receipts(tmp_path,tmp_path/"db.sqlite")
    assert db.execute("SELECT count(*) FROM move_receipts").fetchone()[0] == 1
    assert db.execute("SELECT count(*) FROM events WHERE kind='move'").fetchone()[0] == 1
    assert not (apply/"db_committed.json").exists()
    log.write_text(json.dumps(success)+'\n'+json.dumps({"op":"move","ok":False,"error":"src_not_found"})+'\n')
    assert recover_receipts(tmp_path,tmp_path/"db.sqlite") == ["run_fixture"]
    assert recover_receipts(tmp_path,tmp_path/"db.sqlite") == []


def test_orphan_archival_preserves_exact_history_and_repairs_foreign_keys(db,tmp_path):
    from repair_legacy_records import repair
    db.commit(); db.execute("PRAGMA foreign_keys=OFF")
    db.execute("INSERT INTO observations VALUES('test','missing',100,'old','video','[]')")
    db.execute("INSERT INTO events(run_id,ts,kind,src_path_id,detail_json) VALUES('test','old','backfill_register','missing','{\"path\":\"unknown\"}')")
    originals = {table:dict(db.execute(f"SELECT * FROM {table}").fetchone()) for table in ("events","observations")}
    db.commit(); db.execute("PRAGMA foreign_keys=ON")
    result = repair(tmp_path/"db.sqlite",[],[],strict=True,archive_orphans=True)
    assert result["ok"]
    assert list(db.execute("PRAGMA foreign_key_check")) == []
    archive = {r[0]:json.loads(r[1]) for r in db.execute("SELECT table_name,record_json FROM legacy_orphan_history")}
    assert archive == originals
    assert db.execute("SELECT count(*) FROM observations").fetchone()[0] == 0
    assert db.execute("SELECT src_path_id FROM events").fetchone()[0] is None


def test_public_package_default_does_not_require_private_reviewed_dictionary(tmp_path):
    from video_pipeline.platform.resources import default_program_aliases
    from run_metadata_batches_promptv1 import _load_hints_with_status
    root = tmp_path/"py"; root.mkdir()
    rules = tmp_path/"rules"; rules.mkdir()
    public = rules/"program_aliases.example.yaml"; public.write_text("hints: []\n")
    assert default_program_aliases(root) == public
    _,status = _load_hints_with_status(str(default_program_aliases(root)))
    assert status["hintsFilePresent"] and status["hintsLoadable"]
    assert status["hintsLoadError"] is None and status["hintsAliasCount"] == 0
    private = rules/"program_aliases.yaml"; private.write_text("hints: []\n")
    assert default_program_aliases(root) == private
