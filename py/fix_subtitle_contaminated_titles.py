#!/usr/bin/env python3
"""path_metadata の program_title からサブタイトル汚染を除去する。

対象: data_json.program_title に ▽▼◇「 を含むレコード。
処理: 最初の区切り文字より前の部分をクリーンタイトルとして採用。

Usage:
  python fix_subtitle_contaminated_titles.py --db mediaops.sqlite --dry-run
  python fix_subtitle_contaminated_titles.py --db mediaops.sqlite
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata

from mediaops_schema import begin_immediate, connect_db

SEPARATOR_RE = re.compile(r"[▽▼◇「]")

# normalized_program_key 再計算用 (run_metadata_batches_promptv1.norm_key 相当)
_BAD = re.compile(r'[<>:"/\\|?*]')
_UND = re.compile(r"_+")
_WS = re.compile(r"\s+")


def _norm_key(title: str) -> str:
    t = unicodedata.normalize("NFKC", title)
    t = _WS.sub(" ", t).strip().replace(" ", "_")
    t = _BAD.sub("", t)
    t = _UND.sub("_", t).strip("_")
    return t or "UNKNOWN"


def clean_title(title: str) -> str:
    """最初のサブタイトル区切り文字で分割し、前半を返す。"""
    return SEPARATOR_RE.split(title, maxsplit=1)[0].strip()


TITLE_RELATED_REASONS = {
    "needs_review_flagged",
    "program_title_may_include_description",
    "relocate_suspicious_program_title",
    "relocate_suspicious_program_title_shortened",
    "subtitle_separator_in_program_title",
    "relocate_subtitle_separator_in_program_title",
}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    con = connect_db(args.db)
    rows = con.execute(
        "SELECT path_id, source, data_json FROM path_metadata"
    ).fetchall()

    updates: list[tuple[str, str]] = []
    for r in rows:
        raw = r["data_json"]
        if not raw:
            continue
        try:
            data = json.loads(str(raw))
        except Exception:
            continue
        pt = data.get("program_title", "")
        if not isinstance(pt, str) or not SEPARATOR_RE.search(pt):
            continue
        cleaned = clean_title(pt)
        if not cleaned or cleaned == pt:
            continue
        data["program_title"] = cleaned
        # normalized_program_key も再計算
        data["normalized_program_key"] = _norm_key(cleaned)
        # needs_review をクリア (タイトル起因の reason のみ残っている場合)
        if data.get("needs_review") is True:
            reason = str(data.get("needs_review_reason", ""))
            remaining = [
                r.strip()
                for r in reason.split(",")
                if r.strip() and r.strip() not in TITLE_RELATED_REASONS
            ]
            if not remaining:
                data["needs_review"] = False
                data["needs_review_reason"] = ""
        updates.append(
            (json.dumps(data, ensure_ascii=False), str(r["path_id"]))
        )

    if args.dry_run:
        samples = []
        for new_json, pid in updates[:20]:
            d = json.loads(new_json)
            samples.append({"path_id": pid, "program_title": d["program_title"]})
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
        "UPDATE path_metadata SET data_json=?, updated_at=datetime('now') WHERE path_id=?",
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
