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
- Execute in the main agent turn; do not delegate `video-library-pipeline` execution to subagents.
- Before running, always check path config mismatch risk (`sourceRoot`, `destRoot`, `windowsOpsRoot`) and ask the user to confirm if there is any possibility of wrong path settings.
- This plugin does not use `pnpm test` / `scripts/*.sh` style E2E. E2E is performed by tool-call sequence.
- If user asks "E2E test", do not ask for shell test command. Run the standard flow immediately.

## Command naming guardrail (required)

- Do not invent CLI commands such as `openclaw video_pipeline_run` or `openclaw tool ...`.
- In this plugin, execution is done by **tool calls**:
  - `video_pipeline_validate`
  - `video_pipeline_analyze_and_move_videos`
  - `video_pipeline_logs`
  - `video_pipeline_status`
  - `video_pipeline_reextract`
  - `video_pipeline_repair_db`
- The only plugin CLI helper command is:
  - `openclaw video-pipeline-status`

## Path sanity prompt (required)

Before `video_pipeline_validate` or pipeline execution, ask a short confirmation when path mismatch is possible:

- "パス設定ミスの可能性があります。`sourceRoot` / `destRoot` / `windowsOpsRoot` は実在パスですか？"
- If an error says `... does not exist`, first treat it as config/path mismatch and ask for the intended real path.

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

## E2E execution contract (required)

- For E2E requests, execute this exact order without asking for extra command details:
  1) `video_pipeline_validate` with `{"checkWindowsInterop": true}`
  2) `video_pipeline_analyze_and_move_videos` with `{"apply": false, "maxFilesPerRun": 1, "allowNeedsReview": false}` for first dry-run check
  3) `video_pipeline_logs` with `{"kind":"all","tail":50}`
- Do not use `sessions_spawn`/subagent for this flow. Keep tool calls in the same main-agent turn.
- Success can be declared only if all 3 tool calls above produced concrete `toolResult` outputs in the same session.
- A completion notice with `Findings: (no output)` is not a successful E2E result.
- If step 1 fails, report the failing checks and stop.
- If step 2 fails, report `exitCode`, `stderr` summary, and log pointers.

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
