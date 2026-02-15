---
name: video-library-pipeline-extract-review
description: Stage 2 of interactive operation. Run extraction, export program YAML, and stop for human review.
metadata: {"openclaw":{"emoji":"🧠","requires":{"plugins":["video-library-pipeline"]}}}
---

# Stage 2: Extract + YAML + Human Review

## Rule

- Use plugin tools only. Do not call scripts directly.
- Keep execution in the main agent turn. Do not use subagents.
- This stage must save program info to YAML after extraction.
- This stage ends with human review. Do not continue to move/apply automatically.

## Tool sequence

1. Call `video_pipeline_validate` with `{"checkWindowsInterop": true}`.
2. Call `video_pipeline_reextract` with:
   - `queuePath` from Stage 1 summary (`queue`)
   - optional `batchSize` / `maxBatches`
   - keep default `preserveHumanReviewed=true` (reviewed rows are protected from overwrite)
3. Call `video_pipeline_export_program_yaml` with:
   - `sourceJsonlPath` omitted (latest extraction output auto-detected), or explicit path
   - optional `outputPath` if user wants a fixed location
   - default is `${windowsOpsRoot}/llm/program_aliases_review_YYYYMMDD_HHMMSS.yaml`
4. If user manually edits extracted JSONL, call `video_pipeline_apply_reviewed_metadata`:
   - `sourceJsonlPath`: edited extraction JSONL path
   - default `markHumanReviewed=true`
   - this step writes reviewed metadata into DB (`path_metadata`)

## Human review checklist

- YAML file was generated successfully.
- Program titles/aliases in YAML are acceptable.
- Rows with `needs_review` are either fixed or intentionally kept for later.
- Manual fixes are applied to DB via `video_pipeline_apply_reviewed_metadata`.
- User confirms YAML should be used as the next review baseline.

## Handoff

- Return:
  - extraction result summary
  - YAML output path
  - count of programs exported
- Ask user whether to proceed to Stage 3 (Move + Human review).
