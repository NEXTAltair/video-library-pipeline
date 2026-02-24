---
name: video-library-pipeline
description: Run and inspect the video library pipeline via OpenClaw plugin tools. Interactive runs must use the 3-stage flow with human review at every gate.
metadata: {"openclaw":{"emoji":"🎬","requires":{"plugins":["video-library-pipeline"]},"localReads":["~/.openclaw/openclaw.json"]}}
---

# video-library-pipeline

This skill is the orchestrator for `video-library-pipeline`.

Use one of the stage skills below for actual execution:

- `extensions/video-library-pipeline/skills/normalize-review/SKILL.md`
- `extensions/video-library-pipeline/skills/extract-review/SKILL.md`
- `extensions/video-library-pipeline/skills/move-review/SKILL.md`

## Rule

- Use plugin tools, not direct script calls.
- Execute in the main agent turn; do not delegate pipeline execution to subagents.
- Never substitute plugin tools with `exec` shell commands.
  - `video_pipeline_*` names are tool names, not shell commands.
  - If a tool call cannot be issued in the current environment, stop and report it as "tool registry / permissions issue".
- Health and diagnostics tools:
  - `video_pipeline_validate`
  - `video_pipeline_logs`
  - `video_pipeline_status`
  - `video_pipeline_backfill_moved_files`
  - `video_pipeline_dedup_recordings`
- Before running, always check path config mismatch risk (`sourceRoot`, `destRoot`, `windowsOpsRoot`) and ask the user to confirm if there is any possibility of wrong path settings.
- Long path prerequisite for apply operations: require `pwsh7` and Windows `LongPathsEnabled=1` (check via `video_pipeline_validate`).
- This plugin does not use `pnpm test` / `scripts/*.sh` style E2E. E2E is performed by tool-call sequence.

## Tool scope / result semantics (required)

- `video_pipeline_analyze_and_move_videos` targets the configured `sourceRoot` (normally `B:\\未視聴`) only.
  - `remaining_files` means "files still remaining under `sourceRoot` after this run".
  - `remaining_files == 0` does **not** mean all files under `destRoot` / `by_program` were reviewed, moved, or fixed.
- `video_pipeline_backfill_moved_files` is a **DB synchronization** tool.
  - It updates DB tracking (`paths`, `observations`, events, optional metadata queue).
  - It does **not** physically move files.
  - A successful backfill does **not** mean long-path files under `B:\\VideoLibrary\\by_program\\...` were physically relocated.
- When reporting completion, always state the scope explicitly:
  - "DB sync complete" vs "physical move complete"
  - which root was scanned (`sourceRoot`, `destRoot`, `by_program` etc.)
- If the user asks about residual files under `destRoot/by_program`, do not use `remaining_files` as evidence. Use an explicit scan of that root (or state that this run did not inspect it).

## Mandatory interactive flow (human review required)

For user-driven runs, follow this order and stop at each review gate:

1. Normalization + human review
2. Extraction + human review
3. Move/apply + human review

Critical requirement:

- After extraction, save program info to YAML and review it with the user before move/apply.
- Use `video_pipeline_export_program_yaml` to generate this YAML.

## Command naming guardrail (required)

- Do not invent CLI commands such as `openclaw video_pipeline_run` or `openclaw tool ...`.
- In this plugin, execution is done by **tool calls**:
  - `video_pipeline_validate`
  - `video_pipeline_backfill_moved_files`
  - `video_pipeline_dedup_recordings`
  - `video_pipeline_analyze_and_move_videos`
  - `video_pipeline_logs`
  - `video_pipeline_status`
  - `video_pipeline_reextract`
  - `video_pipeline_apply_reviewed_metadata`
  - `video_pipeline_export_program_yaml`
  - `video_pipeline_repair_db`
- The only plugin CLI helper command is:
  - `openclaw video-pipeline-status`

## Exec fallback guardrail (required)

- Do not run DB checks with `python -c` via `exec` for this plugin flow.
- Do not retry the same failing `exec` command repeatedly.
- If command construction fails once due to quoting/syntax (`unexpected EOF`, `unrecognized token`, `SyntaxError`), stop immediately and report:
  - failing command (brief)
  - root cause (quote/syntax break)
  - required fix (use plugin tool call directly)

## Path sanity prompt (required)

Before `video_pipeline_validate` or pipeline execution, ask a short confirmation when path mismatch is possible:

- "パス設定ミスの可能性があります。`sourceRoot` / `destRoot` / `windowsOpsRoot` は実在パスですか？"
- If an error says `... does not exist`, first treat it as config/path mismatch and ask for the intended real path.

## Non-interactive automation flow (cron)

1. Validate environment:
   - Call `video_pipeline_validate` with `{"checkWindowsInterop": true}`
   - This also auto-syncs plugin-managed PowerShell scripts under `<windowsOpsRoot>/scripts`.
   - Stop if `ok` is false.
2. Optional pre-run backfill:
   - Call `video_pipeline_backfill_moved_files` with `{"apply": false}`
   - Use apply only after reviewing dry-run result.
3. Optional pre-run dedup:
   - Call `video_pipeline_dedup_recordings` with `{"apply": false}`
   - Use apply only after reviewing dry-run result.
4. Run pipeline:
   - Call `video_pipeline_analyze_and_move_videos` with:
     - `apply`: `true` for real move, `false` for dry-run
     - `maxFilesPerRun`: e.g. `500` in cron
     - `allowNeedsReview`: default `false`
5. Parse run summary:
   - Parse JSON from tool result `stdout`.
   - Expected keys:
     - `inventory`
     - `queue`
     - `plan`
     - `applied`
     - `remaining_files`
     - `plan_stats`
   - Interpret `remaining_files` as `sourceRoot`-scoped only (not `destRoot` / `by_program`).
6. Collect latest pointers:
   - Call `video_pipeline_logs` with `{"kind":"all","tail":50}`.

## E2E execution contract (required)

- For E2E requests, execute this exact order without asking for extra command details:
  1) `video_pipeline_validate` with `{"checkWindowsInterop": true}`
  2) `video_pipeline_analyze_and_move_videos` with `{"apply": false, "maxFilesPerRun": 1, "allowNeedsReview": false}` for first dry-run check
  3) `video_pipeline_logs` with `{"kind":"all","tail":50}`
- If step 1 cannot be called as a tool (tool not visible/available), do not replace it with shell probing. Stop and report the environment mismatch.
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

Do not alert/claim success about `destRoot/by_program` cleanup based only on `remaining_files`.

On healthy run, avoid noisy reports.
