#!/usr/bin/env python3
"""Targeted audit repair; never moves, deletes or content-deduplicates videos."""
from __future__ import annotations
import argparse
import json
import re
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path, PureWindowsPath
from video_pipeline.platform.sqlite_snapshot import online_snapshot
from mediaops_schema import connect_db, create_schema_if_needed
from pathscan_common import now_iso, windows_to_wsl_path
from video_pipeline.db.file_identity import ensure_identity_schema, ensure_location, observe_file


def archive_orphan_history(con, run_id):
    """Preserve exact rows; never invent a missing path or discard audit evidence."""
    violations = [tuple(row) for row in con.execute("PRAGMA foreign_key_check")]
    if any(table not in {"events","observations"} or parent != "paths" for table,_,parent,_ in violations):
        raise ValueError("unsupported orphan relationship; no automatic archival")
    con.execute("""CREATE TABLE IF NOT EXISTS legacy_orphan_history (
      table_name TEXT NOT NULL, row_id INTEGER NOT NULL, record_json TEXT NOT NULL,
      reason TEXT NOT NULL, archived_at TEXT NOT NULL, repair_run_id TEXT NOT NULL,
      PRIMARY KEY(table_name,row_id))""")
    counts = {"events":0,"observations":0}
    for table,rowid in sorted({(v[0],v[1]) for v in violations}):
        original = dict(con.execute(f"SELECT * FROM {table} WHERE rowid=?",(rowid,)).fetchone())
        payload = json.dumps(original,ensure_ascii=False,sort_keys=True)
        con.execute("INSERT INTO legacy_orphan_history VALUES(?,?,?,?,?,?)",(table,rowid,payload,"missing_path_reference",now_iso(),run_id))
        archived = con.execute("SELECT record_json FROM legacy_orphan_history WHERE table_name=? AND row_id=?",(table,rowid)).fetchone()[0]
        if json.loads(archived) != original:
            raise ValueError("archive roundtrip failed; transaction will roll back")
        if table == "observations":
            con.execute("DELETE FROM observations WHERE rowid=?",(rowid,))
        else:
            for column in ("src_path_id","dst_path_id"):
                pid = original[column]
                if pid and not con.execute("SELECT 1 FROM paths WHERE path_id=?",(pid,)).fetchone():
                    con.execute(f"UPDATE events SET {column}=NULL WHERE rowid=?",(rowid,))
        counts[table] += 1
    if con.execute("PRAGMA foreign_key_check").fetchone():
        raise ValueError("foreign key violations remain; transaction will roll back")
    return counts


def repair(db, pairs, titles, *, strict=False, archive_orphans=False):
    con = connect_db(str(db))
    create_schema_if_needed(con); ensure_identity_schema(con); con.commit()
    run_id = "audit_repair_" + uuid.uuid4().hex
    changes = []; unresolved = []
    try:
        con.execute("BEGIN IMMEDIATE")
        con.execute("INSERT INTO runs(run_id,kind,started_at,notes) VALUES(?,'audit_repair',?,?)",(run_id,now_iso(),"Historical location repair; independent file instances, no content equivalence assumed"))
        for pair in pairs:
            dst = pair["dst"]
            file = Path(windows_to_wsl_path(dst))
            if not file.is_file():
                unresolved.append({"dst":dst,"reason":"destination_not_found"}); continue
            src_id = ensure_location(con,pair["src"])
            con.execute("INSERT OR IGNORE INTO recording_holds VALUES(?,'legacy_collision',?,?,NULL)",(src_id,json.dumps(pair,ensure_ascii=False),now_iso()))
            if con.execute("SELECT 1 FROM paths WHERE path=? COLLATE NOCASE",(dst,)).fetchone():
                continue
            pid = ensure_location(con,dst)
            stat = file.stat(); mtime = datetime.fromtimestamp(stat.st_mtime,timezone.utc).isoformat()
            fid = observe_file(con,pid,stat.st_size,mtime,run_id)
            con.execute("INSERT INTO observations VALUES(?,?,?,?,?,?)",(run_id,pid,stat.st_size,mtime,"file",None))
            # Filename is evidence for a title, not evidence for file equality.
            md = {"path_id":pid,"path":dst,"program_title":None,"air_date":None,"needs_review":True,"review_reason":"legacy_destination_registration_requires_metadata","human_reviewed":False,"file_id":fid}
            con.execute("INSERT INTO path_metadata(path_id,source,data_json,updated_at,needs_review) VALUES(?,'audit_repair',?,?,1)",(pid,json.dumps(md,ensure_ascii=False),now_iso()))
            detail = {"dst":dst,"src":pair["src"],"file_id":fid,"content_identity":"not_established"}
            con.execute("INSERT INTO events(run_id,ts,kind,dst_path_id,detail_json,ok) VALUES(?,?,'legacy_location_restored',?,?,1)",(run_id,now_iso(),pid,json.dumps(detail,ensure_ascii=False)))
            changes.append({"kind":"missing_destination_registered",**detail})
        for item in titles:
            name = item["filename"]
            candidates = con.execute("SELECT p.path_id,p.path,m.data_json,m.human_reviewed,m.program_title,m.episode_no FROM paths p JOIN path_metadata m ON p.path_id=m.path_id WHERE p.name=?",(name,)).fetchall()
            if not candidates:
                unresolved.append({"filename":name,"reason":"no_exact_filename_match"}); continue
            title = item["reproducedInitial"]["program_title"]
            title = re.sub(r"\s*\d+\.\s*(?:PHASE|STAGE|ACT)[-\s]*\d+.*$","",title,flags=re.I).strip()
            from run_metadata_batches_promptv1 import parse_episode
            episode = parse_episode(name)
            for row in candidates:
                if row["human_reviewed"]:
                    unresolved.append({"path":row["path"],"reason":"human_reviewed_preserved"}); continue
                if not Path(windows_to_wsl_path(row["path"])).is_file():
                    unresolved.append({"path":row["path"],"reason":"file_not_present"}); continue
                md = json.loads(row["data_json"])
                before = row["program_title"]
                if before == title and (str(row["episode_no"]) if row["episode_no"] is not None else None) == (str(episode) if episode is not None else None):
                    continue
                # Only repair the demonstrated franchise-collapse, not unrelated data.
                if before != item["saved"]["program_title"]:
                    unresolved.append({"path":row["path"],"reason":"metadata_changed_since_audit"}); continue
                from epg_common import normalize_program_key
                program_key = normalize_program_key(title)
                pr = con.execute("SELECT program_id FROM programs WHERE program_key=?",(program_key,)).fetchone()
                program_id = pr[0] if pr else str(uuid.uuid4())
                if not pr:
                    con.execute("INSERT INTO programs(program_id,program_key,canonical_title,created_at) VALUES(?,?,?,?)",(program_id,program_key,title,now_iso()))
                previous_links = [dict(link) for link in con.execute("SELECT * FROM path_programs WHERE path_id=?",(row["path_id"],))]
                con.execute("DELETE FROM path_programs WHERE path_id=? AND source != 'human_reviewed'",(row["path_id"],))
                con.execute("INSERT OR REPLACE INTO path_programs VALUES(?,?,NULL,'audit_repair',?)",(row["path_id"],program_id,now_iso()))
                md.update(path=row["path"],path_id=row["path_id"],program_title=title,episode_no=episode,program_id=program_id,model="rule_based/title-v2",title_architecture="RULE_BASED_TITLE_V2",extraction_version="rule_based_title_v2_20261006")
                md.setdefault("source_history",[]).append({"source":"audit_repair","reason":"franchise_collapse_regression","at":now_iso(),"previous_title":before,"previous_links":previous_links})
                con.execute("UPDATE path_metadata SET data_json=?,program_title=?,episode_no=?,updated_at=? WHERE path_id=?",(json.dumps(md,ensure_ascii=False),title,str(episode) if episode is not None else None,now_iso(),row["path_id"]))
                con.execute("INSERT INTO events(run_id,ts,kind,src_path_id,detail_json,ok) VALUES(?,?,'metadata_audit_repair',?,?,1)",(run_id,now_iso(),row["path_id"],json.dumps({"before":before,"after":title,"episode_no":episode},ensure_ascii=False)))
                changes.append({"kind":"work_title_restored","path":row["path"],"before":before,"after":title,"episode_no":episode})
        con.execute("UPDATE runs SET finished_at=? WHERE run_id=?",(now_iso(),run_id))
        if archive_orphans:
            counts = archive_orphan_history(con,run_id)
            changes.append({"kind":"orphan_history_preserved","archived":counts,"foreignKeyViolationsRemaining":0})
        if strict and unresolved:
            raise ValueError("repair prerequisites changed; transaction rolled back: " + json.dumps(unresolved,ensure_ascii=False))
        con.commit()
    except Exception:
        con.rollback(); raise
    finally:
        con.close()
    return {"ok":not unresolved,"runId":run_id,"changes":changes,"unresolved":unresolved}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db",required=True)
    parser.add_argument("--pairs-json",required=True)
    parser.add_argument("--titles-json",required=True)
    parser.add_argument("--outdir",required=True)
    parser.add_argument("--apply",action="store_true")
    parser.add_argument("--archive-orphans",action="store_true",help="Preserve exact historical orphan rows before clearing invalid references")
    args = parser.parse_args()
    out = Path(args.outdir); out.mkdir(parents=True,exist_ok=True)
    target = Path(args.db) if args.apply else out / "repair-preview.sqlite"
    backup = out / ("before-repair.sqlite" if args.apply else "repair-preview.sqlite")
    if args.apply and backup.exists():
        raise SystemExit("repair backup already exists; use a new outdir")
    online_snapshot(args.db, backup)
    pairs = json.loads(Path(args.pairs_json).read_text())["pairs"]
    result = repair(target,pairs,json.loads(Path(args.titles_json).read_text()),strict=args.apply,archive_orphans=args.archive_orphans)
    result.update(apply=args.apply,db=str(target),backup=str(backup))
    (out / "repair-result.json").write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n")
    print(json.dumps(result,ensure_ascii=False))
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
