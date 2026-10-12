"""Cross-drive work routing, reviewed holds, conflicts and non-mutating previews."""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

import relocate_existing_files as relocate
from mediaops_schema import connect_db, create_schema_if_needed
from video_pipeline.db.file_identity import ensure_location
from video_pipeline.domain.move_preflight import inspect_move
from video_pipeline.domain.path_placement_rules import DriveRoute, DriveRoutes
from video_pipeline.domain.routing_inventory import default_relocate_roots, title_in_scope
from video_pipeline.platform.pathscan_common import ScannedFile, scan_files, canonicalize_windows_path, windows_to_wsl_path, wsl_to_windows_path


def routes():
    return DriveRoutes(r"B:\VideoLibrary", "by_title", [
        DriveRoute({"genre": "tokusatsu", "dest_root": "N:\\", "layout": "by_title", "title_patterns": ["戦隊"]}),
        DriveRoute({"genre": "anime", "dest_root": r"D:\Anime", "layout": "by_title", "epg_genre_match": ["アニメ/特撮 - *"]}),
    ])


def test_specific_tokusatsu_beats_shared_anime_epg():
    route, reason = routes().resolve({"program_title": "爆上戦隊ブンブンジャー", "epg_genres": ["アニメ/特撮 - 特撮"]})
    assert route.genre == "tokusatsu"
    assert reason == "title_pattern:戦隊"
    assert routes().resolve({"program_title": "別のアニメ", "epg_genres": ["アニメ/特撮 - 国内アニメ"]})[0].genre == "anime"
    assert routes().resolve({"genre": "anime", "program_title": "戦隊", "epg_genres": []})[0].genre == "anime"


def test_registration_only_persists_unknown_path_without_moving_known_or_unknown(tmp_path, monkeypatch, capsys):
    preview = Preview(tmp_path, monkeypatch)
    preview.add(r"B:\Library\Show known.mp4")
    unknown = preview.add(r"N:\Show untracked.mp4", registered=False)
    monkeypatch.setattr(relocate, "run_pwsh_json", lambda *_a, **_kw: pytest.fail("registration must never move files"))
    summary, rows, inventory = preview.run(monkeypatch, capsys, "--register-unregistered-only", readonly=False)
    assert summary["autoRegisteredPaths"] == summary["autoRegisteredObservations"] == 1
    assert summary["movedFiles"] == summary["dbUpdatedPaths"] == 0
    assert preview.locations[unknown.win_path].read_bytes() == b"recording"
    with connect_db(str(preview.db)) as con:
        assert con.execute("SELECT COUNT(*) FROM paths WHERE path=?", (unknown.win_path,)).fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM events WHERE kind='relocate_register'").fetchone()[0] == 1


def test_tokusatsu_movie_keeps_movie_route():
    config = routes()
    config.routes.append(DriveRoute({"genre": "movie", "dest_root": "L:\\", "epg_genre_match": ["映画 - *"]}))
    assert config.resolve({"program_title": "劇場版 スーパー戦隊", "epg_genres": ["映画 - 邦画"]})[0].genre == "movie"


def test_default_roots_use_configured_locations_and_deduplicate(tmp_path):
    route_file = tmp_path / "routes.yaml"
    route_file.write_text('default_dest: "B:\\\\VideoLibrary"\nroutes:\n  - genre: anime\n    dest_root: "D:\\\\Anime"\n')
    assert default_relocate_roots("/mnt/b/未視聴", r"B:\VideoLibrary", r"J:\TVFile", str(route_file)) == [
        r"B:\未視聴", r"B:\VideoLibrary", r"D:\Anime", r"J:\TVFile"]
    with pytest.raises(FileNotFoundError):
        default_relocate_roots("", r"B:\VideoLibrary", "", str(tmp_path / "missing.yaml"))


@pytest.mark.parametrize("windows", ["N:\\", "L:\\", r"B:\VideoLibrary"])
def test_windows_fallback_roundtrip_preserves_drive_roots(windows):
    local = windows_to_wsl_path(windows)
    assert canonicalize_windows_path(local) == windows
    assert wsl_to_windows_path(local) == windows


def test_default_roots_deduplicate_bare_mounts_and_windows_drive_roots(tmp_path):
    route_file = tmp_path / "routes.yaml"
    route_file.write_text('default_dest: "N:\\\\"\nroutes:\n  - genre: movie\n    dest_root: "L:\\\\"\n')
    assert default_relocate_roots("/mnt/n", "N:\\", "/mnt/l/", str(route_file)) == ["N:\\", "L:\\"]


def test_title_filter_does_not_merge_seasons_or_ignore_metadata_conflicts():
    assert title_in_scope(["Ｓｈｏｗ"], "Show", "anything.mp4") == (True, False)
    assert title_in_scope(["Show"], "Show 2", "Show 2 episode.mp4") == (True, True)
    assert title_in_scope(["Show"], "Other", "other.mp4") == (False, False)
    assert title_in_scope(["Show"], None, "Show episode.mp4") == (True, False)


class Preview:
    def __init__(self, tmp_path, monkeypatch):
        self.root = tmp_path
        self.db = tmp_path / "db.sqlite"
        self.ops = tmp_path / "ops"
        self.files = []
        self.locations = {}
        self.con = connect_db(str(self.db))
        create_schema_if_needed(self.con)
        self.con.execute("CREATE TABLE recording_holds(path_id TEXT, reason TEXT, resolved_at TEXT)")
        monkeypatch.setattr(relocate, "scan_files", lambda **_kw: (self.files, [], [], {}))
        monkeypatch.setattr(relocate, "inspect_move", lambda src, dst, inv, **kw: inspect_move(src, dst, inv, localize=self.localize, **kw))

    def localize(self, path):
        return str(self.locations.get(path, self.root / "absent" / path.replace("\\", "_")))

    def add(self, src, title="Show", *, hold=False, wrong_size=False, registered=True):
        file = self.root / f"file{len(self.files)}.mp4"
        file.write_bytes(b"recording")
        self.locations[src] = file
        stat = file.stat()
        sf = ScannedFile(str(file), src, src[0], None, src.split("\\")[-1], ".mp4", stat.st_size + int(wrong_size),
                         datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(), False, None)
        self.files.append(sf)
        if not registered:
            return sf
        pid = ensure_location(self.con, src)
        if title:
            self.con.execute("INSERT INTO path_metadata(path_id,source,data_json,updated_at,program_title,air_date,needs_review,human_reviewed) VALUES(?,'human_reviewed','{}','now',?,'2026-01-01',0,1)", (pid, title))
        if hold:
            self.con.execute("INSERT INTO recording_holds VALUES(?,'manual_hold',NULL)", (pid,))
        return sf

    def run(self, monkeypatch, capsys, *extra, readonly=True):
        self.con.commit()
        self.con.close()
        before = hashlib.sha256(self.db.read_bytes()).hexdigest()
        monkeypatch.setattr("sys.argv", ["relocate", "--db", str(self.db), "--windows-ops-root", str(self.ops),
                                        "--dest-root", "N:\\", "--roots-json", '["B:\\\\Library","N:\\\\"]',
                                        "--program-titles-json", '["Show"]', "--queue-missing-metadata", "true",
                                        "--write-metadata-queue-on-dry-run", "true", *extra])
        route_file = self.root / "routes.yaml"
        route_file.write_text('default_dest: "N:\\\\"\ndefault_layout: by_title\nroutes: []\n')
        monkeypatch.setattr("sys.argv", [*__import__("sys").argv, "--drive-routes", str(route_file)])
        assert relocate.main() == 0
        summary = json.loads(capsys.readouterr().out)
        if readonly:
            assert hashlib.sha256(self.db.read_bytes()).hexdigest() == before
        rows = [json.loads(line) for line in Path(summary["planPath"]).read_text().splitlines()][1:]
        inventory = json.loads(Path(summary["routingInventoryPath"]).read_text())
        return summary, rows, inventory


def test_cross_drive_preview_preserves_holds_excludes_quarantine_and_selects_work(tmp_path, monkeypatch, capsys):
    preview = Preview(tmp_path, monkeypatch)
    preview.add(r"B:\Library\Show one.mp4")
    preview.add(r"N:\Show\Show two.mp4")
    preview.add(r"B:\Library\Show held.mp4", hold=True)
    preview.add(r"B:\Library\Show conflict.mp4", title="Other")
    preview.add(r"B:\Library\Show unknown.mp4", title=None)
    preview.add(r"D:\Anime\Unrelated.mp4", title="Unrelated")
    preview.add(r"N:\_AI_WORK\duplicates\quarantine_v3\Show excluded.mp4")
    summary, rows, inventory = preview.run(monkeypatch, capsys)
    assert (summary["scannedFiles"], summary["selectedFiles"], summary["plannedMoves"], summary["alreadyCorrect"]) == (6, 5, 1, 1)
    assert summary["heldSkipped"] == summary["metadataMismatchSkipped"] == summary["metadataMissingSkipped"] == 1
    assert {r["reason"] for r in rows} == {"recompute_destination", "already_correct", "recording_hold", "program_title_mismatch", "missing_metadata"}
    assert not any("_AI_WORK" in r["src"] for r in rows)
    assert inventory["selectedSizeBytes"] == 5 * 9
    assert inventory["scannedByDrive"] == {"B": 4, "D": 1, "N": 1}
    planned = next(r for r in rows if r["status"] == "planned")
    assert planned["dst"] == r"N:\Show\Show one.mp4"
    assert planned["precondition"]["size_bytes"] == 9


def test_in_plan_destination_collision_holds_both_sources(tmp_path, monkeypatch, capsys):
    preview = Preview(tmp_path, monkeypatch)
    preview.add(r"B:\Library\Show same.mp4")
    preview.add(r"D:\Library\Show same.mp4")
    summary, rows, _inventory = preview.run(monkeypatch, capsys)
    assert summary["plannedMoves"] == 0
    assert summary["preflightSkipped"] == 2
    assert all(row["reason"] == "destination_collision_in_plan" for row in rows)


@pytest.mark.parametrize("existing_destination", [True, False])
def test_preview_excludes_existing_destinations_and_changed_sources(tmp_path, monkeypatch, capsys, existing_destination):
    preview = Preview(tmp_path, monkeypatch)
    preview.add(r"B:\Library\Show one.mp4", wrong_size=not existing_destination)
    if existing_destination:
        target = tmp_path / "destination.mp4"
        target.write_bytes(b"different recording")
        preview.locations[r"N:\Show\Show one.mp4"] = target
    summary, rows, _inventory = preview.run(monkeypatch, capsys)
    assert summary["plannedMoves"] == 0
    assert rows[0]["reason"] == ("destination_exists" if existing_destination else "source_changed")


def test_limit_applies_after_work_selection(tmp_path, monkeypatch, capsys):
    preview = Preview(tmp_path, monkeypatch)
    preview.add(r"B:\Library\Unrelated.mp4", title="Unrelated")
    preview.add(r"B:\Library\Show one.mp4")
    preview.add(r"B:\Library\Show two.mp4")
    summary, rows, _inventory = preview.run(monkeypatch, capsys, "--limit", "1")
    assert summary["scannedFiles"] == 3
    assert summary["selectedFiles"] == summary["plannedMoves"] == len(rows) == 1
    assert rows[0]["src"].endswith("Show one.mp4")


def test_missing_routes_fail_before_scanning_or_writing_db(tmp_path, monkeypatch):
    monkeypatch.setattr("sys.argv", ["relocate", "--db", "unused", "--windows-ops-root", str(tmp_path / "ops"),
                                    "--dest-root", "N:\\", "--drive-routes", str(tmp_path / "missing.yaml")])
    with pytest.raises(FileNotFoundError):
        relocate.main()
    assert not (tmp_path / "ops").exists()


def test_python_scan_skips_reserved_directories_and_reparse_equivalents(tmp_path):
    (tmp_path / "Show.mp4").write_bytes(b"recording")
    for directory in ["_AI_WORK", "QUARANTINE_V3", "$RECYCLE.BIN", "System Volume Information"]:
        path = tmp_path / directory
        path.mkdir()
        (path / "hidden.mp4").write_bytes(b"recording")
    (tmp_path / "loop").symlink_to(tmp_path, target_is_directory=True)
    files, warnings, errors, _stats = scan_files([str(tmp_path)], {".mp4"}, False, 4096, 0)
    assert [sf.name for sf in files] == ["Show.mp4"]
    assert not warnings and not errors
