"""Re-extract metadata using the current prompt_v1 policy (LLM-style parsing rules)."""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import unicodedata
from pathlib import Path
from typing import Any

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
DB_CONTRACT_REQUIRED = {"program_title", "air_date", "needs_review"}
ACTIVE_TITLE_ARCHITECTURE = "A_AI_PRIMARY_WITH_GUARDRAILS"
DEFERRED_TITLE_ARCHITECTURES = (
    "B_FULL_AI_MIN_RULES",
    "C_LEGACY_PARSER_WITH_AI_REVIEW",
    "D_RULE_ENGINE_PRIMARY_WITH_AI_FALLBACK",
    "E_TWO_STAGE_LIGHT_PARSE_THEN_AI",
)

try:
    import yaml  # type: ignore
except Exception:
    yaml = None


def validate_rows(rows: list[dict]) -> list[str]:
    errs: list[str] = []
    for i, obj in enumerate(rows, start=1):
        if not (obj.get("path_id") or obj.get("path")):
            errs.append(f"row {i}: missing path_id/path")
        missing = sorted([k for k in REQUIRED if k not in obj])
        if missing:
            errs.append(f"row {i}: missing keys: {missing}")
        missing_contract = sorted([k for k in DB_CONTRACT_REQUIRED if k not in obj])
        if missing_contract:
            errs.append(f"row {i}: missing DB contract keys: {missing_contract}")
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


def _normalize_alias_key(s: str) -> str:
    return norm_ws(s).lower()


def _iter_hint_items(obj: Any) -> list[dict[str, Any]]:
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        out: list[dict[str, Any]] = []
        for key in ("hints", "user_learned"):
            seq = obj.get(key)
            if isinstance(seq, list):
                out.extend([x for x in seq if isinstance(x, dict)])
        return out
    return []


def _load_hint_items_from_file(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        if path.suffix.lower() == ".json":
            with path.open("r", encoding="utf-8-sig") as f:
                obj = json.load(f)
            return _iter_hint_items(obj)
        if path.suffix.lower() in {".yaml", ".yml"}:
            if yaml is None:
                print(f"W hints yaml parser unavailable, skip={path}")
                return []
            with path.open("r", encoding="utf-8-sig") as f:
                obj = yaml.safe_load(f)
            return _iter_hint_items(obj)
    except Exception as e:
        print(f"W failed to load hints: path={path} error={e}")
    return []


def _load_hints(path: str | None) -> tuple[dict[str, str], bool]:
    if not path:
        return {}, False
    p = Path(path)
    if not p.exists():
        print(f"W hints file missing: {p}")
        return {}, False

    files: list[Path]
    if p.is_dir():
        files = sorted(
            [x for x in p.glob("*") if x.suffix.lower() in {".yaml", ".yml", ".json"}]
        )
    else:
        files = [p]

    alias_map: dict[str, str] = {}
    for fp in files:
        for item in _load_hint_items_from_file(fp):
            canonical = item.get("canonical_title")
            if not isinstance(canonical, str):
                continue
            canonical = canonical.strip()
            if not canonical:
                continue
            aliases = item.get("aliases")
            raw_aliases: list[str] = []
            if isinstance(aliases, list):
                raw_aliases.extend([a for a in aliases if isinstance(a, str)])
            raw_aliases.append(canonical)
            for alias in raw_aliases:
                key = _normalize_alias_key(alias)
                if key:
                    alias_map[key] = canonical
    return alias_map, len(alias_map) > 0


def _canonicalize_title_from_hints(base: str, parsed_program_title: str, alias_map: dict[str, str]) -> str:
    key = _normalize_alias_key(parsed_program_title)
    if key in alias_map:
        return alias_map[key]
    base_norm = _normalize_alias_key(base)
    for alias_key, canonical in alias_map.items():
        if alias_key and alias_key in base_norm:
            return canonical
    return parsed_program_title


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


def extract_title_ai_primary(name: str, base: str, alias_hints: dict[str, str]) -> dict:
    # Architecture A: AI-primary path with optional alias guardrails.
    prog, sub = parse_program_and_subtitle(base)
    canonical = _canonicalize_title_from_hints(base=base, parsed_program_title=prog, alias_map=alias_hints)
    source = "ai_primary_policy"
    if canonical != prog:
        prog = canonical
        source = "ai_primary_policy+hints"
    return {
        "program_title": prog,
        "subtitle": sub,
        "episode_no": parse_episode(base),
        "air_date": extract_air_date(name),
        "title_extraction_path": source,
    }


def enforce_db_contract(row: dict) -> None:
    missing = [k for k in DB_CONTRACT_REQUIRED if k not in row]
    if missing:
        raise ValueError(f"missing DB contract keys: {missing}")
    if not isinstance(row.get("needs_review"), bool):
        raise ValueError("needs_review must be bool")


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


def _get_latest_llm_metadata(con: sqlite3.Connection, path_id: str) -> dict | None:
    cur = con.cursor()
    try:
        row = cur.execute(
            """
            SELECT data_json
            FROM path_metadata
            WHERE path_id=? AND source='llm'
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (path_id,),
        ).fetchone()
    except sqlite3.Error:
        return None
    if not row:
        return None
    try:
        d = json.loads(row[0])
        return d if isinstance(d, dict) else None
    except Exception:
        return None


def _build_locked_row(existing: dict, rec: dict, model: str, extraction_version: str) -> dict:
    row = dict(existing)
    program_title = str(row.get("program_title") or "")
    row["path_id"] = rec.get("path_id") or row.get("path_id")
    row["path"] = rec.get("path") or row.get("path")
    row["program_title"] = program_title
    row["episode_no"] = row.get("episode_no")
    row["subtitle"] = row.get("subtitle")
    row["air_date"] = row.get("air_date")
    c = row.get("confidence")
    row["confidence"] = float(c) if isinstance(c, (int, float)) else 0.9
    row["needs_review"] = bool(row.get("needs_review"))
    row["model"] = row.get("model") or model
    row["extraction_version"] = row.get("extraction_version") or extraction_version
    row["normalized_program_key"] = row.get("normalized_program_key") or norm_key(program_title)
    row["evidence"] = row.get("evidence") or {"source_name": "manual_review_lock", "raw": rec.get("name")}
    row["human_reviewed"] = True
    row.setdefault("title_architecture", ACTIVE_TITLE_ARCHITECTURE)
    row.setdefault("title_extraction_path", "human_review_lock")
    return row


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
    ap.add_argument("--hints", default="")
    ap.add_argument("--ignore-human-reviewed", action="store_true")
    args = ap.parse_args()

    alias_hints, hints_loaded = _load_hints(args.hints)
    print(
        f"INFO title_architecture={ACTIVE_TITLE_ARCHITECTURE} "
        f"deferred={','.join(DEFERRED_TITLE_ARCHITECTURES)} "
        f"hints_loaded={str(hints_loaded).lower()} hints_count={len(alias_hints)}"
    )
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    batch: list[dict] = []
    batch_idx = 0
    processed = 0
    preserved_human_reviewed = 0
    db_con = sqlite3.connect(args.db)

    def flush():
        nonlocal batch, batch_idx, processed, preserved_human_reviewed
        if not batch:
            return
        batch_idx += 1
        batch_no = args.start_batch + batch_idx - 1
        bpath = outdir / f"llm_filename_extract_input_{batch_no:04d}_{len(batch):04d}.jsonl"
        epath = outdir / f"llm_filename_extract_output_{batch_no:04d}_{len(batch):04d}.jsonl"
        write_jsonl(bpath, batch)

        rows = []
        for rec in batch:
            pid = rec.get("path_id")
            if pid and not args.ignore_human_reviewed:
                existing = _get_latest_llm_metadata(db_con, str(pid))
                if isinstance(existing, dict) and existing.get("human_reviewed") is True:
                    locked_row = _build_locked_row(existing, rec, args.model, args.extraction_version)
                    enforce_db_contract(locked_row)
                    rows.append(locked_row)
                    preserved_human_reviewed += 1
                    continue

            name = rec["name"]
            base = strip_suffix(name)
            title_res = extract_title_ai_primary(name=name, base=base, alias_hints=alias_hints)
            air = title_res.get("air_date")
            prog = str(title_res.get("program_title") or "")
            sub = title_res.get("subtitle")
            ep = title_res.get("episode_no")

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

            row = {
                "path_id": rec["path_id"],
                "path": rec["path"],
                "program_title": prog,
                "episode_no": ep,
                "subtitle": sub,
                "air_date": air,
                "genre": None,
                "broadcaster": None,
                "channel": None,
                "confidence": round(float(conf), 2),
                "needs_review": bool(needs),
                "needs_review_reason": ",".join(reasons) if reasons else None,
                "model": args.model,
                "extraction_version": args.extraction_version,
                "normalized_program_key": norm_key(prog),
                "title_architecture": ACTIVE_TITLE_ARCHITECTURE,
                "title_extraction_path": title_res.get("title_extraction_path"),
                "evidence": {"source_name": "filename", "raw": name},
            }
            enforce_db_contract(row)
            rows.append(row)

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

    try:
        for rec in load_queue(args.queue):
            batch.append(rec)
            if len(batch) >= args.batch_size:
                flush()
                if args.max_batches is not None and batch_idx >= args.max_batches:
                    break
        flush()
    finally:
        db_con.close()
    print(f"OK preserved_human_reviewed={preserved_human_reviewed}")
    print(f"OK processed={processed} batches={batch_idx}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
