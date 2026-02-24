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
  - `video_pipeline_relocate_existing_files`
  - `video_pipeline_dedup_recordings`
- Before running, always check path config mismatch risk (`sourceRoot`, `destRoot`, `windowsOpsRoot`) and ask the user to confirm if there is any possibility of wrong path settings.
- Long path prerequisite for apply operations: require `pwsh7` and Windows `LongPathsEnabled=1` (check via `video_pipeline_validate`).
- This plugin does not use `pnpm test` / `scripts/*.sh` style E2E. E2E is performed by tool-call sequence.

## Start by classifying the user request (required)

Before running tools, classify the request into one of these goals and follow the matching flow:

1. **`sourceRoot` pipeline run (通常の未視聴処理)**
   - Use `video_pipeline_analyze_and_move_videos`
   - `remaining_files` is meaningful here
2. **DB sync / inventory recovery only**
   - Use `video_pipeline_backfill_moved_files`
   - Do not claim physical move completion
3. **任意ディレクトリ（既存ファイル）の再配置 / cleanup**
   - Use `video_pipeline_relocate_existing_files`
   - Do not use `remaining_files` from analyze-and-move as evidence

If the user asks about cleanup/reorganization for an already-existing directory tree, treat that as **(3) relocate flow**, not the `sourceRoot` pipeline flow.

## Tool scope / result semantics (required)

- `video_pipeline_analyze_and_move_videos` targets the configured `sourceRoot` (normally `B:\\未視聴`) only.
  - `remaining_files` means "files still remaining under `sourceRoot` after this run".
  - `remaining_files == 0` does **not** mean all files under `destRoot` or any custom cleanup target root were reviewed, moved, or fixed.
- `video_pipeline_backfill_moved_files` is a **DB synchronization** tool.
  - It updates DB tracking (`paths`, `observations`, events, optional metadata queue).
  - It does **not** physically move files.
  - A successful backfill does **not** mean files under any custom cleanup target root were physically relocated.
  - `queueMissingMetadata` is scoped to rows touched by that backfill run (newly upserted/remapped/rename-detected or observation backfill rows).
  - Files skipped as existing (`skippedExisting`) do **not** automatically become backfill metadata queue entries.
- `video_pipeline_relocate_existing_files` is a **physical relocation** tool for existing files.
  - It scans explicitly provided `roots` (or `rootsFilePath`) and recomputes destinations from DB metadata.
  - It physically moves files (via plugin-managed PowerShell move engine) and updates DB paths on apply.
  - On `apply=true`, files found as `unregistered_path` are auto-registered into DB tracking (`paths`/`observations` + register event), but they still cannot be moved until metadata exists.
  - `backfill` is still the primary bulk DB sync tool; use it when the main goal is registration/sync rather than relocation.
  - `queueMissingMetadata` can collect registered files skipped by `missing_metadata` / invalid contract / `needs_review`.
  - To write the queue file during dry-run, set `writeMetadataQueueOnDryRun=true` (otherwise dry-run reports count only).
- When reporting completion, always state the scope explicitly:
  - "DB sync complete" vs "physical move complete"
  - which root was scanned (`sourceRoot`, `roots[]`, `destRoot` subtree, etc.)
- If the user asks about residual files under a custom cleanup target root, do not use `remaining_files` as evidence. Use an explicit scan of that root (or state that this run did not inspect it).

## Winning Flow (arbitrary existing-root relocation / cleanup)

Use this when the user wants to relocate existing files from explicitly chosen roots using current DB metadata.
Example targets:
- `B:\\VideoLibrary\\legacy_import`
- `B:\\VideoLibrary\\staging`
- any explicitly provided `roots[]`

1. Validate:
   - `video_pipeline_validate {"checkWindowsInterop": true}`
2. Relocate dry-run with queue planning:
   - `video_pipeline_relocate_existing_files {"apply": false, "roots":["B:\\\\VideoLibrary\\\\legacy_import"], "queueMissingMetadata": true, "writeMetadataQueueOnDryRun": true, "scanErrorPolicy":"warn", "scanRetryCount": 2}`
3. Read relocate result and branch:
   - If `plannedMoves > 0`: review plan and prepare for apply
   - If `metadataQueuePlannedCount > 0`: run reextract/review flow first
   - If `unregisteredSkipped > 0`: `relocate apply` can auto-register them (DB tracking only), then rerun relocate after metadata is filled
4. Reextract/review (when metadata is missing):
   - `video_pipeline_reextract` (use relocate metadata queue)
   - `video_pipeline_export_program_yaml`
   - human review
   - `video_pipeline_apply_reviewed_metadata`
5. Relocate dry-run again (rebuild plan with fresh metadata)
6. Relocate apply (only after dry-run review)
7. Optional dedup dry-run

Do not run `backfill` expecting it to create metadata queue entries for `skippedExisting` rows. That is not its queue scope.

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
  - `video_pipeline_relocate_existing_files`
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

This flow is for routine `sourceRoot` pipeline automation. It is **not** the primary flow for arbitrary existing-root cleanup.

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
4. Optional relocate stage (existing files under arbitrary roots):
   - Call `video_pipeline_relocate_existing_files` with `{"apply": false, "roots":[\"B:\\\\VideoLibrary\\\\legacy_import\"]}`
   - `roots`/`rootsFilePath` is required (no implicit default target scan for safety).
   - If you need a reextract queue for `missing_metadata` rows without moving files yet, use:
     - `{"apply": false, "roots":[...], "queueMissingMetadata": true, "writeMetadataQueueOnDryRun": true}`
   - Use apply only after reviewing dry-run result.
5. Run pipeline:
   - Call `video_pipeline_analyze_and_move_videos` with:
     - `apply`: `true` for real move, `false` for dry-run
     - `maxFilesPerRun`: e.g. `500` in cron
     - `allowNeedsReview`: default `false`
6. Parse run summary:
   - Parse JSON from tool result `stdout`.
   - Expected keys:
     - `inventory`
     - `queue`
     - `plan`
     - `applied`
     - `remaining_files`
     - `plan_stats`
   - Interpret `remaining_files` as `sourceRoot`-scoped only (not any custom cleanup target root).
7. Collect latest pointers:
   - Call `video_pipeline_logs` with `{"kind":"all","tail":50}`.

## E2E execution contract (required)

- This E2E contract validates the **`sourceRoot` pipeline tool path**, not arbitrary existing-root cleanup completeness.

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

Do not alert/claim success about arbitrary existing-root cleanup based only on `remaining_files`.

On healthy run, avoid noisy reports.
