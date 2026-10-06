"""Locations are immutable; file instances move between locations.

Size/mtime is only an observation signature, never a content hash or dedup proof.
"""
from __future__ import annotations

import json
import uuid
from video_pipeline.platform.pathscan_common import now_iso, path_id_for, split_win


def ensure_identity_schema(con):
    con.execute("""CREATE TABLE IF NOT EXISTS recording_holds (
      path_id TEXT PRIMARY KEY REFERENCES paths(path_id), reason TEXT NOT NULL,
      detail_json TEXT NOT NULL, created_at TEXT NOT NULL, resolved_at TEXT)""")
    con.execute("""CREATE TABLE IF NOT EXISTS path_file_state (
      path_id TEXT PRIMARY KEY REFERENCES paths(path_id),
      file_id TEXT NOT NULL REFERENCES files(file_id),
      size_bytes INTEGER NOT NULL, mtime_utc TEXT)""")
    con.execute("""CREATE TABLE IF NOT EXISTS move_receipts (
      receipt_id TEXT PRIMARY KEY, src_path_id TEXT NOT NULL,
      dst_path_id TEXT NOT NULL, file_id TEXT NOT NULL, applied_at TEXT NOT NULL)""")


def ensure_location(con, path):
    row = con.execute("SELECT path_id FROM paths WHERE path=? COLLATE NOCASE", (path,)).fetchone()
    if row:
        return row[0]
    pid = path_id_for(path)
    # Legacy versions moved this ID to another location. Never steal it back.
    if con.execute("SELECT 1 FROM paths WHERE path_id=?", (pid,)).fetchone():
        pid = str(uuid.uuid5(uuid.NAMESPACE_URL, "video-pipeline:location:v2:" + path.replace("/", "\\").lower()))
    drive, parent, name, ext = split_win(path)
    ts = now_iso()
    con.execute("INSERT INTO paths VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (pid, path, drive, parent, name, ext, ts, ts))
    return pid


def observe_file(con, pid, size, mtime, run_id):
    old = con.execute("SELECT * FROM path_file_state WHERE path_id=?", (pid,)).fetchone()
    if old and old["size_bytes"] == size and old["mtime_utc"] == mtime:
        fid = old["file_id"]
    else:
        fid = str(uuid.uuid4())
        ts = now_iso()
        con.execute("INSERT INTO files(file_id,size_bytes,created_at,updated_at) VALUES(?,?,?,?)", (fid,size,ts,ts))
        if old or con.execute("SELECT 1 FROM file_paths WHERE path_id=?", (pid,)).fetchone():
            con.execute("UPDATE file_paths SET is_current=0 WHERE path_id=?", (pid,))
            # Metadata belongs to an observed recording, not every future file at this name.
            row = con.execute("SELECT data_json FROM path_metadata WHERE path_id=?", (pid,)).fetchone()
            if row:
                md = json.loads(row[0]); md["needs_review"] = True
                md["review_reason"] = "recording_replaced_at_same_location"
                con.execute("UPDATE path_metadata SET needs_review=1,data_json=? WHERE path_id=?", (json.dumps(md,ensure_ascii=False),pid))
        con.execute("INSERT OR REPLACE INTO path_file_state VALUES(?,?,?,?)", (pid,fid,size,mtime))
    con.execute("""INSERT INTO file_paths(file_id,path_id,is_current,first_seen_run_id,last_seen_run_id)
      VALUES(?,?,1,?,?) ON CONFLICT(file_id,path_id) DO UPDATE SET is_current=1,last_seen_run_id=excluded.last_seen_run_id""", (fid,pid,run_id,run_id))
    return fid


def record_move(con, row, receipt_id, run_id):
    if con.execute("SELECT 1 FROM move_receipts WHERE receipt_id=?", (receipt_id,)).fetchone():
        return False
    src = ensure_location(con, row["src"])
    dst = ensure_location(con, row["dst"])
    state = con.execute("SELECT * FROM path_file_state WHERE path_id=?", (src,)).fetchone()
    if state is None:
        obs = con.execute("SELECT size_bytes,mtime_utc FROM observations WHERE path_id=? ORDER BY rowid DESC LIMIT 1", (src,)).fetchone()
        if obs is None:
            raise ValueError("successful move has no source observation: " + row["src"])
        observe_file(con,src,obs[0],obs[1],run_id)
        state = con.execute("SELECT * FROM path_file_state WHERE path_id=?", (src,)).fetchone()
    fid = state["file_id"]
    if row.get("file_id") and row["file_id"] != fid:
        raise ValueError("move file identity no longer matches source observation")
    if con.execute("SELECT 1 FROM path_file_state WHERE path_id=?", (dst,)).fetchone():
        raise ValueError("destination has a current file identity; refusing to merge recordings")
    con.execute("UPDATE file_paths SET is_current=0 WHERE file_id=? AND path_id=?", (fid,src))
    con.execute("""INSERT INTO file_paths VALUES(?,?,1,?,?)
      ON CONFLICT(file_id,path_id) DO UPDATE SET is_current=1,last_seen_run_id=excluded.last_seen_run_id""", (fid,dst,run_id,run_id))
    con.execute("DELETE FROM path_file_state WHERE path_id=?", (src,))
    con.execute("INSERT INTO path_file_state VALUES(?,?,?,?)", (dst,fid,state["size_bytes"],state["mtime_utc"]))
    # Copy location metadata; keep source history and references intact.
    columns = [r[1] for r in con.execute("PRAGMA table_info(path_metadata)") if r[1] != "path_id"]
    names = ",".join(columns)
    con.execute(f"INSERT OR REPLACE INTO path_metadata(path_id,{names}) SELECT ?,{names} FROM path_metadata WHERE path_id=?", (dst,src))
    md_row = con.execute("SELECT data_json FROM path_metadata WHERE path_id=?",(dst,)).fetchone()
    if md_row:
        md = json.loads(md_row[0]); md.update(path_id=dst,path=row["dst"])
        con.execute("UPDATE path_metadata SET data_json=? WHERE path_id=?",(json.dumps(md,ensure_ascii=False),dst))
    con.execute("INSERT OR IGNORE INTO path_programs SELECT ?,program_id,broadcast_id,source,updated_at FROM path_programs WHERE path_id=?", (dst,src))
    con.execute("INSERT OR IGNORE INTO path_tags SELECT ?,tag_id,source,updated_at FROM path_tags WHERE path_id=?", (dst,src))
    ts = row.get("ts") or now_iso()
    con.execute("INSERT INTO move_receipts VALUES(?,?,?,?,?)", (receipt_id,src,dst,fid,ts))
    con.execute("INSERT INTO events(run_id,ts,kind,src_path_id,dst_path_id,detail_json,ok) VALUES(?,?,'move',?,?,?,1)", (run_id,ts,src,dst,json.dumps(row,ensure_ascii=False)))
    return True
