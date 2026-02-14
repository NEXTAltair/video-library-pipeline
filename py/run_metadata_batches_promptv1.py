"""Re-extract metadata using the current prompt_v1 policy (LLM-style parsing rules)."""

from __future__ import annotations

import argparse
import json
import os
import re
import unicodedata
from pathlib import Path

WS = re.compile(r"[\s\u3000]+")
BAD = re.compile(r"[<>:\"/\\\\|?*]")
UND = re.compile(r"_+")

PAT_US = re.compile(r"(\d{4})_(\d{2})_(\d{2})_(\d{2})_(\d{2})(?=\.[^.]+$)")
PAT_SP = re.compile(r"(\d{4}) (\d{2}) (\d{2}) (\d{2}) (\d{2})(?=\.[^.]+$)")
PAT_COL = re.compile(r"(\d{4}) (\d{2}) (\d{2}) (\d{2})[：:](\d{2})(?=\.[^.]+$)")
PAT_US_DUP = re.compile(r"(\d{4})_(\d{2})_(\d{2})_(\d{2})_(\d{2})[-_]\(\d+\)(?=\.[^.]+$)")
PAT_US_PAREN = re.compile(r"(\d{4})_(\d{2})_(\d{2})_(\d{2})_(\d{2})\(\d+\)(?=\.[^.]+$)")
PAT_HHMM = re.compile(r"(\d{4})_(\d{2})_(\d{2})_(\d{4})(?=\.[^.]+$)")
PAT_COMPACT_14 = re.compile(r"(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(?=\.[^.]+$)")
PAT_COMPACT_12 = re.compile(r"(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(?=\.[^.]+$)")
PAT_HYPH_HHMM = re.compile(r"(\d{4})-(\d{2})-(\d{2})-(\d{4})(?=\.[^.]+$)")

REQUIRED = {
    "program_title",
    "episode_no",
    "subtitle",
    "air_date",
    "confidence",
    "needs_review",
    "model",
    "extraction_version",
    "normalized_program_key",
    "evidence",
}


def validate_rows(rows: list[dict]) -> list[str]:
    errs: list[str] = []
    for i, obj in enumerate(rows, start=1):
        if not (obj.get("path_id") or obj.get("path")):
            errs.append(f"row {i}: missing path_id/path")
        missing = sorted([k for k in REQUIRED if k not in obj])
        if missing:
            errs.append(f"row {i}: missing keys: {missing}")
        c = obj.get("confidence")
        if not (isinstance(c, (int, float)) and 0 <= float(c) <= 1):
            errs.append(f"row {i}: confidence must be 0..1")
        if not isinstance(obj.get("needs_review"), bool):
            errs.append(f"row {i}: needs_review must be bool")
    return errs


def norm_ws(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    return WS.sub(" ", s).strip()


def extract_air_date(name: str) -> str | None:
    n = norm_ws(name)
    for p in (PAT_US_DUP, PAT_US_PAREN, PAT_US, PAT_SP, PAT_COL, PAT_HHMM, PAT_COMPACT_14, PAT_COMPACT_12, PAT_HYPH_HHMM):
        m = p.search(n)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


def strip_suffix(name: str) -> str:
    n = norm_ws(name)
    for p in (PAT_US_DUP, PAT_US_PAREN, PAT_US, PAT_SP, PAT_COL, PAT_HHMM, PAT_COMPACT_14, PAT_COMPACT_12, PAT_HYPH_HHMM):
        m = p.search(n)
        if m:
            return n[: m.start()].rstrip(" _-")
    return os.path.splitext(n)[0]


def norm_key(title: str) -> str:
    t = norm_ws(title).replace(" ", "_")
    t = BAD.sub("", t)
    t = UND.sub("_", t).strip("_")
    return t or "UNKNOWN"


def drop_label_words(s: str) -> str:
    s = re.sub(r"(名作選|傑作選)", "", s)
    s = s.replace("選", "")
    return s.strip(" _-")


def parse_episode(base: str) -> int | None:
    m = re.search(r"第\s*(\d+)\s*話", base)
    if m:
        return int(m.group(1))
    m = re.search(r"\((\d+)\)\s*$", base)
    if m:
        return int(m.group(1))
    return None


def parse_quoted_subtitle(s: str) -> str | None:
    m = re.search(r"「([^」]{1,120})」", s)
    return m.group(1) if m else None


def parse_broadcaster(base: str) -> str | None:
    s = norm_ws(base)
    table = [
        (r"\bNHK\b|ＮＨＫ", "NHK"),
        (r"\bEテレ\b", "NHK Eテレ"),
        (r"\bBS11\b", "BS11"),
        (r"\bBSフジ\b", "BSフジ"),
        (r"\bBS朝日\b", "BS朝日"),
        (r"\bBS-TBS\b", "BS-TBS"),
        (r"\bテレ東\b|テレビ東京", "テレビ東京"),
        (r"\b日テレ\b|日本テレビ", "日本テレビ"),
        (r"\bTBS\b", "TBS"),
        (r"\bフジテレビ\b", "フジテレビ"),
        (r"\bテレビ朝日\b", "テレビ朝日"),
    ]
    for pat, name in table:
        if re.search(pat, s, flags=re.IGNORECASE):
            return name
    return None


def infer_genre(program_title: str, subtitle: str | None) -> str | None:
    s = f"{program_title} {subtitle or ''}"
    rules = [
        (r"ニュース|報道|NEWS", "news"),
        (r"ドラマ|劇場|時代劇", "drama"),
        (r"映画|シネマ|ムービー", "movie"),
        (r"バラエティ|アンビリバボー", "variety"),
        (r"ドキュメンタリー|documentary", "documentary"),
        (r"スポーツ|野球|サッカー|駅伝", "sports"),
        (r"アニメ", "anime"),
        (r"音楽|ライブ|concert", "music"),
    ]
    for pat, genre in rules:
        if re.search(pat, s, flags=re.IGNORECASE):
            return genre
    return None


def _load_rules(path: str | None):
    if not path:
        return []
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            obj = json.load(f)
        return obj.get("rules", []) if isinstance(obj, dict) else []
    except FileNotFoundError:
        return []


def _split_segments(s: str, seps: list[str]):
    pat = "|".join([re.escape(x) for x in seps])
    parts = [p.strip(" _") for p in re.split(pat, s) if p.strip(" _")]
    return parts


def _apply_exception_rules(b: str, rules: list[dict]) -> tuple[str, str | None] | None:
    for r in rules:
        if not r or not r.get("enabled", True):
            continue
        m = r.get("match") or {}
        if m.get("field") != "base":
            continue
        rx = m.get("regex")
        if not rx:
            continue
        m2 = re.search(rx, b)
        if not m2:
            continue

        s = r.get("set") or {}
        prog_tmpl = s.get("program_title")
        if not prog_tmpl:
            continue

        prog = prog_tmpl
        for gi in range(1, 10):
            token = f"\\{gi}"
            if token in prog:
                prog = prog.replace(token, m2.group(gi) or "") if gi <= (m2.lastindex or 0) else prog.replace(token, "")
        prog = re.sub(r"\\[1-9]", "", prog).strip()

        subtitle = None
        sub_cfg = s.get("subtitle")
        if isinstance(sub_cfg, dict) and sub_cfg.get("from") == "segments":
            seps = sub_cfg.get("separators") or ["▽", "▼"]
            parts = _split_segments(b, seps)
            if parts and parts[0].startswith(prog):
                parts = parts[1:]
            take = int(sub_cfg.get("take", 1))
            if parts and take >= 1:
                subtitle = parts[0][:120]
        return prog, subtitle
    return None


def parse_program_and_subtitle(base: str) -> tuple[str, str | None]:
    b = base
    m = re.match(r"^【NHK地域局発】(.+)$", b)
    if m:
        rest = drop_label_words(m.group(1).strip("_ "))
        return "NHK地域局発", (rest or None)

    m = re.match(r"^【ハートネット(?:TV|tv|ｔｖ)】\s*虹クロ[_\s]*(.+)$", b)
    if m:
        rest = m.group(1).strip("_ ")
        q = parse_quoted_subtitle(rest)
        sub = q or rest.split("▼")[0].split("▽")[0].strip(" _")
        return "虹クロ", (sub[:120] if sub else None)

    if "アナザーストーリーズ" in b:
        m = re.search(r"アナザーストーリーズ(?:選)?(.+)$", b)
        sub = None
        if m:
            rest = m.group(1).lstrip("選")
            rest = rest.replace("_", " ")
            rest = rest.split("▼")[0].split("▽")[0].strip(" ")
            sub = rest[:120] if rest else None
        return "アナザーストーリーズ", sub

    m = re.match(r"^[『「]([^』」]+)[』」](.*)$", b)
    if m:
        title = drop_label_words(m.group(1).strip())
        rest = drop_label_words(m.group(2).strip("_ "))
        sub = parse_quoted_subtitle(rest) or parse_quoted_subtitle(b)
        if sub and sub == title:
            sub = None
        return title, sub

    m = re.match(r"^【([^】]+)】\s*(.+)?$", b)
    if m:
        tag = drop_label_words(m.group(1).strip())
        rest = drop_label_words((m.group(2) or "").strip("_ "))
        if tag.startswith("特選") and rest:
            tok = rest.split(" ")[0].split("_")[0]
            return tok, parse_quoted_subtitle(rest)
        if rest:
            tok = rest.split(" ")[0].split("_")[0]
            if len(tok) >= 2 and not tok.startswith("▼"):
                return tok, parse_quoted_subtitle(rest)
        return tag, None

    prog = b.split(" ")[0].split("_")[0].strip()
    return prog[:80], parse_quoted_subtitle(b)


def load_queue(path: str):
    with open(path, "r", encoding="utf-8-sig") as f:
        for i, line in enumerate(f):
            if not line.strip():
                continue
            obj = json.loads(line)
            if i == 0 and isinstance(obj, dict) and "_meta" in obj:
                continue
            yield obj


def write_jsonl(path: Path, rows: list[dict]):
    with open(path, "w", encoding="utf-8") as fo:
        for r in rows:
            fo.write(json.dumps(r, ensure_ascii=False) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--queue", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--batch-size", type=int, default=200)
    ap.add_argument("--start-batch", type=int, default=1)
    ap.add_argument("--max-batches", type=int, default=None)
    ap.add_argument("--model", default="openai-codex/gpt-5.2")
    ap.add_argument("--extraction-version", default="prompt_v1_20260208")
    ap.add_argument("--rules", default="")
    args = ap.parse_args()

    rules = _load_rules(args.rules)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    batch: list[dict] = []
    batch_idx = 0
    processed = 0

    def flush():
        nonlocal batch, batch_idx, processed
        if not batch:
            return
        batch_idx += 1
        batch_no = args.start_batch + batch_idx - 1
        bpath = outdir / f"llm_filename_extract_input_{batch_no:04d}_{len(batch):04d}.jsonl"
        epath = outdir / f"llm_filename_extract_output_{batch_no:04d}_{len(batch):04d}.jsonl"
        write_jsonl(bpath, batch)

        rows = []
        for rec in batch:
            name = rec["name"]
            base = strip_suffix(name)
            base = re.sub(r"^【(?:最新作|新作|話題作|メガヒット\d+)】", "", base).strip(" _")
            air = extract_air_date(name)
            ex = _apply_exception_rules(base, rules)
            if ex:
                prog, sub = ex
            else:
                prog, sub = parse_program_and_subtitle(base)
            ep = parse_episode(base)

            needs = False
            reasons: list[str] = []
            conf = 0.78
            if air is None:
                mtime = rec.get("mtime_utc") or rec.get("mtimeUtc")
                if isinstance(mtime, str) and len(mtime) >= 10 and mtime[4] == "-" and mtime[7] == "-":
                    air = mtime[:10]
                    reasons.append("air_date_from_mtime")
                    conf = min(conf, 0.72)
                else:
                    if prog not in ("塚原卜伝",):
                        needs = True
                        reasons.append("missing_air_date")
                        conf = min(conf, 0.65)

            if prog in ("UNKNOWN", ""):
                needs = True
                reasons.append("unknown_program_title")
                conf = 0.4
            if prog == "虹クロ" and not sub:
                needs = True
                reasons.append("niji_kuro_missing_subtitle")
                conf = min(conf, 0.65)
            if len(prog) > 80:
                needs = True
                reasons.append("program_title_too_long")
                conf = min(conf, 0.7)

            broadcaster = parse_broadcaster(base)
            genre = infer_genre(prog, sub)
            rows.append(
                {
                    "path_id": rec["path_id"],
                    "path": rec["path"],
                    "program_title": prog,
                    "episode_no": ep,
                    "subtitle": sub,
                    "air_date": air,
                    "genre": genre,
                    "broadcaster": broadcaster,
                    "channel": broadcaster,
                    "confidence": round(float(conf), 2),
                    "needs_review": bool(needs),
                    "needs_review_reason": ",".join(reasons) if reasons else None,
                    "model": args.model,
                    "extraction_version": args.extraction_version,
                    "normalized_program_key": norm_key(prog),
                    "evidence": {"source_name": "filename", "raw": name},
                }
            )

        write_jsonl(epath, rows)
        errs = validate_rows(rows)
        if errs:
            for e in errs[:20]:
                print(f"E {e}")
            raise SystemExit(f"validation failed: {epath} errors={len(errs)}")

        from upsert_path_metadata_jsonl import main as _upsert_main  # type: ignore
        import sys

        sys.argv = ["upsert", "--db", args.db, "--in", str(epath), "--source", "llm"]
        if _upsert_main() != 0:
            raise SystemExit(f"upsert failed: {epath}")

        processed += len(batch)
        batch = []

    for rec in load_queue(args.queue):
        batch.append(rec)
        if len(batch) >= args.batch_size:
            flush()
            if args.max_batches is not None and batch_idx >= args.max_batches:
                break
    flush()
    print(f"OK processed={processed} batches={batch_idx}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
