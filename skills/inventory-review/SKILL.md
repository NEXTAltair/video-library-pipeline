---
name: video-library-pipeline-inventory-review
description: Stage 1 of interactive operation. Run inventory scan and queue generation, then stop for human review before extraction.
metadata: {"openclaw":{"emoji":"📋","requires":{"plugins":["video-library-pipeline"]}}}
---

# Stage 1: Inventory + Human Review

## Rule

- Use plugin tools only. Do not call Python/PowerShell scripts directly.
- Keep execution in the main agent turn. Do not use subagents.
- This stage ends with human review. Do not continue to extraction automatically.

## Tool sequence

1. Call `video_pipeline_validate` with `{"checkWindowsInterop": true, "intent": "normalize"}`. Follow the `nextStep` field in the result.
2. Call `video_pipeline_analyze_and_move_videos` with:
   - `apply=false`
   - `allowNeedsReview=false`
   - optional `maxFilesPerRun` (default plugin value is acceptable)
3. Parse summary JSON in tool result `stdout` and extract:
   - `inventory` — path to the inventory file for this run
   - `queue` — path to the metadata extraction queue; pass as `queuePath` to Stage 2 (`/extract-review`)
   - `plan` — move plan for the current dry-run
   - `plan_stats` — per-category skip/move counters:
     - `skipped_needs_review`: files flagged for human review (not moved)
     - `skipped_missing_fields`: files without required metadata (not moved)
     - `skipped_outside`: files outside configured destination roots (investigate if > 0)
     - `genre_route_counts`: per-genre destination drive counts (multi-route mode)
4. Call `video_pipeline_logs` with `{"kind":"all","tail":50}`.

## Human review checklist

- Inventory path exists and points to the expected run.
- Inventory file list is acceptable.
- `plan_stats.skipped_outside == 0` (or user explicitly accepts reason).
- No non-zero exit code / no runtime error.

## Handoff

- Report `inventory` and `queue` pointers to the user.
- Pass **`queue`** path (NOT `inventory`) as `queuePath` to Stage 2. The `inventory` file is a raw file listing without `path_id`; passing it to reextract will fail.
- Ask user whether to proceed to Stage 2: `/extract-review` (Extraction + YAML review).
