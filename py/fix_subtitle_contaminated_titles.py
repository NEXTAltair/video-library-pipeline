#!/usr/bin/env python3
"""path_metadata の program_title からサブタイトル汚染を除去する。

対象: program_title に ▽▼◇「 を含むレコード。
処理: 最初の区切り文字より前の部分をクリーンタイトルとして採用。

Usage:
  python fix_subtitle_contaminated_titles.py --db mediaops.sqlite --dry-run
  python fix_subtitle_contaminated_titles.py --db mediaops.sqlite
"""

from __future__ import annotations

import argparse
import json
import sys

from db_helpers import reconstruct_path_metadata, split_path_metadata
from mediaops_schema import begin_immediate, connect_db
from path_placement_rules import SUBTITLE_SEPARATORS, TITLE_RELATED_REASONS, clean_program_title


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    con = connect_db(args.db)
    rows = con.execute(
        """SELECT path_id, source, data_json, program_title, air_date, needs_review,
                  episode_no, subtitle, broadcaster, human_reviewed
           FROM path_metadata"""
    ).fetchall()



    updates: list[tuple] = []
    for r in rows:
        # Use promoted column first, fall back to data_json
        pt = r["program_title"]
        if pt is None:
            # Try data_json
            raw = r["data_json"]
            if not raw:
                continue
            try:
                data = json.loads(str(raw))
            except Exception:
                continue
            pt = data.get("program_title", "")

        if not isinstance(pt, str) or not SUBTITLE_SEPARATORS.search(pt):
            continue

        cleaned = clean_program_title(pt)
        if not cleaned or cleaned == pt:
            continue

        # Reconstruct full dict, modify, then split
        md = reconstruct_path_metadata(r)
        md["program_title"] = cleaned

        # needs_review をクリア (タイトル起因の reason のみ残っている場合)
        if md.get("needs_review") is True:
            reason = str(md.get("needs_review_reason", ""))
            remaining = [
                rv.strip()
                for rv in reason.split(",")
                if rv.strip() and rv.strip() not in TITLE_RELATED_REASONS
            ]
            if not remaining:
                md["needs_review"] = False
                md["needs_review_reason"] = ""

        promoted, data_json = split_path_metadata(md)
        updates.append((
            data_json,
            promoted.get("program_title"),
            promoted.get("needs_review", 0),
            str(r["path_id"]),
        ))

    if args.dry_run:
        samples = []
        for row_tuple in updates[:20]:
            samples.append({"path_id": row_tuple[3], "program_title": row_tuple[1]})
        print(
            json.dumps(
                {
                    "ok": True,
                    "dryRun": True,
                    "affectedRows": len(updates),
                    "samples": samples,
                },
                ensure_ascii=False,
            )
        )
        con.close()
        return 0

    begin_immediate(con)
    con.executemany(
        """UPDATE path_metadata SET
             data_json=?, program_title=?,
             needs_review=?, updated_at=datetime('now')
           WHERE path_id=?""",
        updates,
    )
    con.commit()
    con.close()
    print(
        json.dumps(
            {"ok": True, "dryRun": False, "updatedRows": len(updates)},
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
