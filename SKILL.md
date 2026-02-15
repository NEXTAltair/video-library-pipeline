---
name: video-library-pipeline
description: Run and inspect the video library pipeline via OpenClaw plugin tools. Prefer this over direct Python/PowerShell script execution, especially in cron/heartbeat automation.
metadata: {"openclaw":{"emoji":"🎬","requires":{"plugins":["video-library-pipeline"]},"localReads":["~/.openclaw/openclaw.json"]}}
---

# video-library-pipeline

This skill defines the standard way to operate the `video-library-pipeline` plugin.

## Rule

- Use plugin tools, not direct script calls.
- Primary execute tool: `video_pipeline_analyze_and_move_videos`
- Health and diagnostics tools: `video_pipeline_validate`, `video_pipeline_logs`, `video_pipeline_status`

## Standard run flow

1. Validate environment:
   - Call `video_pipeline_validate` with `{"checkWindowsInterop": true}`
   - Stop if `ok` is false.
2. Run pipeline:
   - Call `video_pipeline_analyze_and_move_videos` with:
     - `apply`: `true` for real move, `false` for dry-run
     - `maxFilesPerRun`: e.g. `500` in cron
     - `allowNeedsReview`: default `false`
3. Parse run summary:
   - Parse JSON from tool result `stdout`.
   - Expected keys:
     - `inventory`
     - `queue`
     - `plan`
     - `applied`
     - `remaining_files`
     - `plan_stats`
4. Collect latest pointers:
   - Call `video_pipeline_logs` with `{"kind":"all","tail":50}`.

## Alert guidance (cron/automation)

Alert if any of:

- validate `ok` is false
- runner/tool exit code is non-zero
- summary JSON parse fails
- `remaining_files > 0`
- `plan_stats.skipped_needs_review > 0`
- `plan_stats.skipped_missing_fields > 0`
- `plan_stats.skipped_outside > 0`
- summary/log pointers missing (`inventory`, `queue`, `plan`, `applied`)

On healthy run, avoid noisy reports.

