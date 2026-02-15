---
name: video-library-pipeline-move-review
description: Stage 3 of interactive operation. Execute move/apply and stop for final human review.
metadata: {"openclaw":{"emoji":"📦","requires":{"plugins":["video-library-pipeline"]}}}
---

# Stage 3: Move + Human Review

## Rule

- Use plugin tools only. Do not call scripts directly.
- Keep execution in the main agent turn. Do not use subagents.
- Require explicit user confirmation before `apply=true`.

## Tool sequence

1. Call `video_pipeline_validate` with `{"checkWindowsInterop": true}`.
2. Ask for final user confirmation to apply move.
3. Call `video_pipeline_analyze_and_move_videos` with:
   - `apply=true`
   - `allowNeedsReview=false` (default safety)
   - optional `maxFilesPerRun`
4. Parse summary JSON and collect:
   - `applied`
   - `remaining_files`
   - `plan_stats`
5. Call `video_pipeline_logs` with `{"kind":"all","tail":50}`.

## Human review checklist

- `exitCode == 0`
- `remaining_files == 0` (or user accepts residual files)
- no abnormal skip counts (`skipped_needs_review`, `skipped_missing_fields`, `skipped_outside`)
- paths (`inventory`, `queue`, `plan`, `applied`) are present

## Completion criteria

- Move stage is complete only after user checks post-run summary/logs.
