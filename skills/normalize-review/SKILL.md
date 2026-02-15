---
name: video-library-pipeline-normalize-review
description: Stage 1 of interactive operation. Run normalization/inventory and stop for human review before extraction.
metadata: {"openclaw":{"emoji":"🧹","requires":{"plugins":["video-library-pipeline"]}}}
---

# Stage 1: Normalize + Human Review

## Rule

- Use plugin tools only. Do not call Python/PowerShell scripts directly.
- Keep execution in the main agent turn. Do not use subagents.
- This stage ends with human review. Do not continue to extraction automatically.

## Tool sequence

1. Call `video_pipeline_validate` with `{"checkWindowsInterop": true}`.
2. Call `video_pipeline_analyze_and_move_videos` with:
   - `apply=false`
   - `allowNeedsReview=false`
   - optional `maxFilesPerRun` (default plugin value is acceptable)
3. Parse summary JSON in tool result `stdout` and extract:
   - `inventory`
   - `queue`
   - `plan`
   - `plan_stats`
4. Call `video_pipeline_logs` with `{"kind":"all","tail":50}`.

## Human review checklist

- Inventory path exists and points to the expected run.
- Filename normalization result is acceptable.
- `plan_stats.skipped_outside == 0` (or user explicitly accepts reason).
- No non-zero exit code / no runtime error.

## Handoff

- Report `inventory` and `queue` pointers to the user.
- Ask user whether to proceed to Stage 2 (Extraction + YAML review).
