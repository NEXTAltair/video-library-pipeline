from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
import sqlite3
import threading

import pytest

from backup_mediaops_db import backup_db, list_backups, rotate_backups, restore_db
from video_pipeline.platform.sqlite_snapshot import online_snapshot


@pytest.mark.parametrize("mode", ["WAL", "DELETE"])
def test_backup_includes_commits_even_with_open_wal_connection(tmp_path, mode):
    db = tmp_path / "source ?日本語.sqlite"
    with closing(sqlite3.connect(db)) as con:
        con.execute(f"PRAGMA journal_mode={mode}")
        con.execute("PRAGMA wal_autocheckpoint=0")
        con.execute("CREATE TABLE records (value INTEGER)")
        con.commit()
        con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        con.execute("INSERT INTO records VALUES (1)")
        con.commit()
        ok, path, error = backup_db(str(db), "review")
        assert ok, error
        with closing(sqlite3.connect(path)) as snapshot:
            assert snapshot.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            assert snapshot.execute("SELECT * FROM records").fetchall() == [(1,)]


def test_concurrent_snapshots_have_distinct_names_and_consistent_transactions(tmp_path):
    db = tmp_path / "source.sqlite"
    with closing(sqlite3.connect(db)) as con:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("CREATE TABLE records (id INTEGER PRIMARY KEY, group_id INTEGER)")
        con.commit()
    started = threading.Event()

    def write():
        with closing(sqlite3.connect(db)) as con:
            started.set()
            for batch in range(50):
                with con:
                    con.executemany("INSERT INTO records(group_id) VALUES (?)", [(batch,)] * 10)

    with ThreadPoolExecutor(max_workers=5) as pool:
        worker = pool.submit(write)
        assert started.wait(5)
        snapshots = list(pool.map(lambda _: backup_db(str(db), "same_second"), range(12)))
        worker.result()
    assert all(ok for ok, _, _ in snapshots)
    assert len({path for _, path, _ in snapshots}) == 12
    for _, path, _ in snapshots:
        with closing(sqlite3.connect(path)) as con:
            assert con.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            assert all(count == 10 for _, count in con.execute("SELECT group_id, COUNT(*) FROM records GROUP BY group_id"))
    before = {p: Path(p).read_bytes() for _, p, _ in snapshots}
    assert rotate_backups(str(db), keep=0) == (0, [])
    assert all(Path(p).read_bytes() == body for p, body in before.items())
    assert len(list_backups(str(db))) == 12


def test_snapshot_never_overwrites_existing_file(tmp_path):
    db, target = tmp_path / "source.sqlite", tmp_path / "before.sqlite"
    with closing(sqlite3.connect(db)) as con:
        con.execute("CREATE TABLE records (id INTEGER)")
    target.write_bytes(b"existing evidence")
    with pytest.raises(FileExistsError):
        online_snapshot(db, target)
    assert target.read_bytes() == b"existing evidence"


def test_missing_or_corrupt_database_does_not_report_success(tmp_path):
    assert backup_db(str(tmp_path / "missing.sqlite"))[0] is False
    db = tmp_path / "bad.sqlite"
    db.write_bytes(b"not sqlite")
    ok, path, error = backup_db(str(db))
    assert not ok and not path and "snapshot failed" in error
    assert db.read_bytes() == b"not sqlite"
    assert list_backups(str(db)) == []


def test_restore_uses_sqlite_not_file_copy_with_live_wal(tmp_path):
    db = tmp_path / "source.sqlite"
    with closing(sqlite3.connect(db)) as con:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("CREATE TABLE records (id INTEGER)")
        con.execute("INSERT INTO records VALUES (1)")
        con.commit()
        ok, path, _ = backup_db(str(db))
        assert ok
        con.execute("INSERT INTO records VALUES (2)")
        con.commit()
        assert restore_db(str(db), path) == (True, "")
        assert con.execute("SELECT * FROM records").fetchall() == [(1,)]
    snapshots = list_backups(str(db))
    assert len(snapshots) == 2
    preserved = next(p["path"] for p in snapshots if "pre_restore" in p["descriptor"])
    with closing(sqlite3.connect(preserved)) as con:
        assert con.execute("SELECT * FROM records").fetchall() == [(1,), (2,)]
