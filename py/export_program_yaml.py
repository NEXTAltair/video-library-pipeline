"""Export reviewed candidate program info YAML from extracted metadata JSONL."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


MAX_REVIEW_CANDIDATES = 50
MAX_PREVIEW_CHARS = 220


def json_scalar(value: Any) -> str:
    return json.dumps("" if value is None else value, ensure_ascii=False)


def as_str(value: Any) -> str:
    return value if isinstance(value, str) else ""


def preview_text(value: Any, max_chars: int = MAX_PREVIEW_CHARS) -> str | None:
    text = as_str(value).strip()
    if not text:
        return None
    if len(text) <= max_chars:
        return text
    return f"{text[: max_chars - 1]}…"


def is_iso_date(text: str) -> bool:
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", text))


def push_count(counts: dict[str, int], key: str) -> None:
    counts[key] = counts.get(key, 0) + 1


def append_unique(items: list[str], value: str) -> None:
    if value and value not in items:
        items.append(value)


def lower_compact(text: str) -> str:
    return re.sub(r'[<>:"/\\|?*\s]+', "", unicodedata.normalize("NFKC", str(text or "")).lower())


def by_program_group_from_path(win_path: str | None) -> str | None:
    parts = [part for part in re.split(r"[\\/]+", str(win_path or "")) if part]
    for idx, part in enumerate(parts):
        if part.lower() == "by_program" and idx + 1 < len(parts):
            return parts[idx + 1]
    for idx, part in enumerate(parts):
        if part.lower() == "videolibrary" and idx + 1 < len(parts):
            return parts[idx + 1]
    return None


def looks_swallowed_program_title(program_title: str, folder_title: str) -> bool:
    program = program_title.strip()
    folder = folder_title.strip()
    if not program or not folder or program == folder:
        return False
    program_norm = lower_compact(program)
    folder_norm = lower_compact(folder)
    if not program_norm or not folder_norm or not program_norm.startswith(folder_norm):
        return False
    return len(program_norm) >= len(folder_norm) + 8


def sha256_short(content: str, length: int = 16) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:length]


def read_jsonl_rows(source_jsonl_path: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    raw = Path(source_jsonl_path).read_text(encoding="utf-8")
    text = raw[1:] if raw.startswith("\ufeff") else raw
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            obj = json.loads(stripped)
        except Exception:
            continue
        if isinstance(obj, dict) and "_meta" not in obj:
            rows.append(obj)
    return rows


def build_review_diagnostics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    field_counts: dict[str, int] = {}
    reason_counts: dict[str, int] = {}
    candidates: list[dict[str, Any]] = []
    rows_needing_review = 0
    required_field_missing_rows = 0
    invalid_air_date_rows = 0
    needs_review_flag_rows = 0
    suspicious_program_title_rows = 0

    for row in rows:
        columns: list[str] = []
        reasons: list[str] = []
        severity = "review"
        program_title = as_str(row.get("program_title")).strip()
        air_date = as_str(row.get("air_date")).strip()
        needs_review = row.get("needs_review") is True
        needs_review_reason = as_str(row.get("needs_review_reason")).strip()
        path_value = as_str(row.get("path")).strip()

        if not program_title:
            append_unique(columns, "program_title")
            append_unique(reasons, "missing_program_title")
            severity = "required"
        if not air_date:
            append_unique(columns, "air_date")
            append_unique(reasons, "missing_air_date")
            severity = "required"
        elif not is_iso_date(air_date):
            append_unique(columns, "air_date")
            append_unique(reasons, "invalid_air_date")
            severity = "required"
        if not isinstance(row.get("needs_review"), bool):
            append_unique(columns, "needs_review")
            append_unique(reasons, "missing_or_invalid_needs_review")
            severity = "required"
        if needs_review:
            append_unique(columns, "needs_review")
            if needs_review_reason:
                append_unique(columns, "needs_review_reason")
            append_unique(reasons, needs_review_reason or "needs_review_flagged")

        folder_title = by_program_group_from_path(path_value)
        if folder_title and program_title and looks_swallowed_program_title(program_title, folder_title):
            append_unique(columns, "program_title")
            append_unique(reasons, "program_title_may_include_description")

        if not reasons:
            continue

        rows_needing_review += 1
        if any(reason.startswith("missing_") or reason in {"invalid_air_date", "missing_or_invalid_needs_review"} for reason in reasons):
            required_field_missing_rows += 1
        if "invalid_air_date" in reasons:
            invalid_air_date_rows += 1
        if needs_review:
            needs_review_flag_rows += 1
        if "program_title_may_include_description" in reasons:
            suspicious_program_title_rows += 1
        for column in columns:
            push_count(field_counts, column)
        for reason in reasons:
            push_count(reason_counts, reason)

        if len(candidates) < MAX_REVIEW_CANDIDATES:
            evidence = row.get("evidence")
            evidence_raw = evidence.get("raw") if isinstance(evidence, dict) else None
            candidates.append(
                {
                    "pathId": row.get("path_id") if isinstance(row.get("path_id"), str) else None,
                    "path": path_value,
                    "columns": columns,
                    "reasons": reasons,
                    "severity": severity,
                    "folderTitle": folder_title,
                    "current": {
                        "program_title": preview_text(row.get("program_title")),
                        "air_date": preview_text(row.get("air_date")),
                        "subtitle": preview_text(row.get("subtitle")),
                        "needs_review": row.get("needs_review") if isinstance(row.get("needs_review"), bool) else None,
                        "needs_review_reason": preview_text(row.get("needs_review_reason")),
                    },
                    "evidence": {
                        "raw": preview_text(evidence_raw),
                    },
                }
            )

    return {
        "summary": {
            "rowsNeedingReview": rows_needing_review,
            "requiredFieldMissingRows": required_field_missing_rows,
            "invalidAirDateRows": invalid_air_date_rows,
            "needsReviewFlagRows": needs_review_flag_rows,
            "suspiciousProgramTitleRows": suspicious_program_title_rows,
            "fieldCounts": field_counts,
            "reasonCounts": reason_counts,
        },
        "candidates": candidates,
        "truncated": rows_needing_review > len(candidates),
    }


def build_yaml(
    source_jsonl_path: str,
    rows_total: int,
    rows_used: int,
    include_needs_review: bool,
    include_unknown: bool,
    stats: list[dict[str, Any]],
    cohort: dict[str, Any] | None = None,
) -> str:
    lines: list[str] = []
    lines.append("# Auto-generated candidate YAML from extraction output.")
    lines.append("# Review manually before using as production hints.")
    lines.append(f"generated_at: {json_scalar(datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'))}")
    lines.append(f"source_jsonl: {json_scalar(source_jsonl_path)}")
    lines.append(f"rows_total: {rows_total}")
    lines.append(f"rows_used: {rows_used}")
    if cohort:
        lines.append("cohort:")
        lines.append(f"  source_jsonl_sha256: {json_scalar(cohort['sourceJsonlSha256'])}")
        lines.append(f"  path_id_count: {cohort['pathIdCount']}")
        lines.append(f"  path_id_set_hash: {json_scalar(cohort['pathIdSetHash'])}")
    lines.append("filters:")
    lines.append(f"  include_needs_review: {'true' if include_needs_review else 'false'}")
    lines.append(f"  include_unknown: {'true' if include_unknown else 'false'}")
    lines.append("hints:")
    for stat in stats:
        lines.append(f"  - canonical_title: {json_scalar(stat['canonicalTitle'])}")
        lines.append("    aliases:")
        lines.append(f"      - {json_scalar(stat['canonicalTitle'])}")
        lines.append("    stats:")
        lines.append(f"      count: {stat['count']}")
        lines.append(f"      needs_review_count: {stat['needsReviewCount']}")
        lines.append("    samples:")
        sample_paths = stat.get("samplePaths", [])
        sample_raw_names = stat.get("sampleRawNames", [])
        if not sample_paths and not sample_raw_names:
            lines.append("      - {}")
        else:
            sample_count = max(len(sample_paths), len(sample_raw_names))
            for index in range(sample_count):
                lines.append(f"      - path: {json_scalar(sample_paths[index] if index < len(sample_paths) else '')}")
                lines.append(f"        raw: {json_scalar(sample_raw_names[index] if index < len(sample_raw_names) else '')}")
    return "\n".join(lines) + "\n"


def default_output_path_for_source(source_jsonl_path: str, output_path: str | None = None) -> str:
    if output_path:
        return output_path
    source_path = Path(source_jsonl_path)
    source_name = source_path.name
    if source_name.startswith("llm_filename_extract_output_") and source_name.endswith(".jsonl"):
        suffix = source_name[len("llm_filename_extract_output_") : -len(".jsonl")]
        return str(source_path.with_name(f"program_aliases_review_{suffix}.yaml"))
    return str(source_path.with_name(f"{source_path.stem}_review.yaml"))


def generate_review_yaml(
    source_jsonl_path: str,
    output_path: str | None = None,
    *,
    include_needs_review: bool = True,
    include_unknown: bool = False,
    max_samples_per_program: int = 3,
    only_if_reviewable: bool = False,
) -> dict[str, Any]:
    if not os.path.exists(source_jsonl_path):
        return {
            "ok": False,
            "tool": "export_program_yaml",
            "error": f"sourceJsonlPath does not exist: {source_jsonl_path}",
        }

    rows = read_jsonl_rows(source_jsonl_path)
    review = build_review_diagnostics(rows)
    if only_if_reviewable and review["summary"]["rowsNeedingReview"] <= 0:
        return {
            "ok": True,
            "tool": "export_program_yaml",
            "sourceJsonlPath": source_jsonl_path,
            "outputPath": None,
            "rowsTotal": len(rows),
            "rowsUsed": 0,
            "programs": 0,
            "includeNeedsReview": include_needs_review,
            "includeUnknown": include_unknown,
            "maxSamplesPerProgram": max_samples_per_program,
            "reviewSummary": review["summary"],
            "reviewCandidates": review["candidates"],
            "reviewCandidatesTruncated": review["truncated"],
            "skippedReason": "no_reviewable_rows",
        }

    source_content = Path(source_jsonl_path).read_text(encoding="utf-8")
    path_ids = [row.get("path_id") for row in rows if isinstance(row.get("path_id"), str) and row.get("path_id")]
    cohort = {
        "sourceJsonlSha256": sha256_short(source_content),
        "pathIdCount": len(path_ids),
        "pathIdSetHash": sha256_short("\n".join(sorted(path_ids))),
    }

    stats_map: dict[str, dict[str, Any]] = {}
    rows_used = 0
    max_samples = max(1, int(max_samples_per_program))

    for row in rows:
        title = as_str(row.get("program_title")).strip()
        if not title:
            continue
        if not include_unknown and title == "UNKNOWN":
            continue
        needs_review = row.get("needs_review") is True
        if not include_needs_review and needs_review:
            continue
        normalized_program_key = lower_compact(title)
        key = f"{title}::{normalized_program_key}"
        current = stats_map.get(key) or {
            "canonicalTitle": title,
            "normalizedProgramKey": normalized_program_key,
            "count": 0,
            "needsReviewCount": 0,
            "samplePaths": [],
            "sampleRawNames": [],
        }
        current["count"] += 1
        if needs_review:
            current["needsReviewCount"] += 1
        if len(current["samplePaths"]) < max_samples and isinstance(row.get("path"), str):
            current["samplePaths"].append(str(row["path"]))
        evidence = row.get("evidence")
        raw_name = evidence.get("raw") if isinstance(evidence, dict) else None
        if len(current["sampleRawNames"]) < max_samples and isinstance(raw_name, str):
            current["sampleRawNames"].append(raw_name)
        stats_map[key] = current
        rows_used += 1

    stats = sorted(stats_map.values(), key=lambda item: item["canonicalTitle"])
    resolved_output_path = default_output_path_for_source(source_jsonl_path, output_path)
    yaml_text = build_yaml(
        source_jsonl_path,
        len(rows),
        rows_used,
        include_needs_review,
        include_unknown,
        stats,
        cohort,
    )

    Path(resolved_output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(resolved_output_path).write_text(yaml_text, encoding="utf-8")

    return {
        "ok": True,
        "tool": "export_program_yaml",
        "sourceJsonlPath": source_jsonl_path,
        "outputPath": resolved_output_path,
        "rowsTotal": len(rows),
        "rowsUsed": rows_used,
        "programs": len(stats),
        "includeNeedsReview": include_needs_review,
        "includeUnknown": include_unknown,
        "maxSamplesPerProgram": max_samples,
        "cohort": cohort,
        "reviewSummary": review["summary"],
        "reviewCandidates": review["candidates"],
        "reviewCandidatesTruncated": review["truncated"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-jsonl", required=True)
    parser.add_argument("--output", default="")
    parser.add_argument("--exclude-needs-review", action="store_true")
    parser.add_argument("--include-unknown", action="store_true")
    parser.add_argument("--max-samples-per-program", type=int, default=3)
    parser.add_argument("--only-if-reviewable", action="store_true")
    args = parser.parse_args()

    result = generate_review_yaml(
        args.source_jsonl,
        args.output or None,
        include_needs_review=not bool(args.exclude_needs_review),
        include_unknown=bool(args.include_unknown),
        max_samples_per_program=args.max_samples_per_program,
        only_if_reviewable=bool(args.only_if_reviewable),
    )
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
