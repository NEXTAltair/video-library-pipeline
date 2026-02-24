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
- Do not treat plugin tool names as workspace skill names.
  - Example: `video_pipeline_prepare_relocate_metadata` and `video_pipeline_relocate_existing_files` are **tools**, not `skills/<name>/SKILL.md`.
  - If a `read` attempt fails with `.../workspace/.../skills/<tool-like-name>/SKILL.md`, recover by calling the plugin tool directly instead of searching for a symlink or alternate skill path.
- Never substitute plugin tools with `exec` shell commands.
  - `video_pipeline_*` names are tool names, not shell commands.
  - If a tool call cannot be issued in the current environment, stop and report it as "tool registry / permissions issue".
- Health and diagnostics tools:
  - `video_pipeline_validate`
  - `video_pipeline_logs`
  - `video_pipeline_status`
  - `video_pipeline_backfill_moved_files`
  - `video_pipeline_relocate_existing_files`
  - `video_pipeline_prepare_relocate_metadata`
  - `video_pipeline_dedup_recordings`
- Before running, always check path config mismatch risk (`sourceRoot`, `destRoot`, `windowsOpsRoot`) and ask the user to confirm if there is any possibility of wrong path settings.
- Long path prerequisite for apply operations: require `pwsh7` and Windows `LongPathsEnabled=1` (check via `video_pipeline_validate`).
- This plugin does not use `pnpm test` / `scripts/*.sh` style E2E. E2E is performed by tool-call sequence.

## Intent Mapping (required)

Interpret natural-language requests by intent first, then choose the matching flow/tool sequence.
Do not jump directly to shell file moves when the user is asking for rule-based cleanup/reorganization.

Before running tools, classify the request into one of these goals:

1. **`sourceRoot` pipeline run (normal incoming/unwatched processing)**
   - Use `video_pipeline_analyze_and_move_videos`
   - `remaining_files` is meaningful here
2. **DB sync / inventory recovery only**
   - Use `video_pipeline_backfill_moved_files`
   - Do not claim physical move completion
3. **Arbitrary existing-directory relocation / cleanup**
   - Use `video_pipeline_relocate_existing_files`
   - Do not use `remaining_files` from analyze-and-move as evidence

If the user asks about cleanup/reorganization for an already-existing directory tree, treat that as **(3) relocate flow**, not the `sourceRoot` pipeline flow.

## Tool scope / result semantics (required)

- `video_pipeline_analyze_and_move_videos` targets the configured `sourceRoot` (normally the incoming/unwatched directory) only.
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
- `video_pipeline_prepare_relocate_metadata` is an **orchestration tool** for relocate prerequisites.
  - It runs `relocate` dry-run with queue generation, then runs `reextract` on that queue.
  - It does **not** apply reviewed metadata and does **not** perform physical relocation.
  - Prefer `followUpToolCalls` from this tool result to continue the flow (YAML export -> human review -> metadata apply -> relocate rerun).
- Metadata state labels used in this skill:
  - `machine_extracted_unreviewed_metadata`: machine-generated candidates from `reextract` (even if already written to DB, still not human-verified)
  - `human_reviewed_metadata`: visually reviewed/edited metadata reflected by `video_pipeline_apply_reviewed_metadata`
  - `auto_registered_file_facts`: `paths` / `observations` / register-type events (file existence/size/mtime facts, not program interpretation)
- When reporting completion, always state the scope explicitly:
  - "DB sync complete" vs "physical move complete"
  - which root was scanned (`sourceRoot`, `roots[]`, `destRoot` subtree, etc.)
  - if `autoRegisteredPaths > 0`, include the `autoRegisteredFiles` list (or state it was truncated) so the user can see which files were newly registered into DB tracking
- If a tool result includes `followUpToolCalls`, use those exact tool names/params instead of inventing a new flow description.
- If the user asks about residual files under a custom cleanup target root, do not use `remaining_files` as evidence. Use an explicit scan of that root (or state that this run did not inspect it).
- When asking for human review after `video_pipeline_export_program_yaml`, do not provide only generic review themes.
  - Prefer `reviewSummary` and `reviewCandidates` from the tool result.
  - Treat YAML output as a human-facing artifact only; the agent should not infer corrections from YAML text.
  - Report exact `path`, `columns[]`, and `reasons[]` for the rows that need review.
  - If the list is truncated, say so explicitly (`reviewCandidatesTruncated=true`).
  - Treat this as a metadata review stage only (`program_title`, `air_date`, `needs_review`, YAML aliases).
  - Do not ask the user to decide destination folders, move plans, or genre/category strategy at this stage.
  - If the user corrects a title/subtitle interpretation, restate it as a column-level correction (for example, "`program_title` should remain X; subtitle/description is not part of `program_title`").
  - Do not claim YAML review was applied to DB unless `video_pipeline_apply_reviewed_metadata` succeeded on the corresponding JSONL input.
- After `video_pipeline_apply_reviewed_metadata`, do not claim review was applied unless the tool result confirms it was not blocked by safety checks (for example, raw extraction input / no-content-change guard).

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
   - Shortcut option (same purpose): `video_pipeline_prepare_relocate_metadata {...}`
3. Read relocate result and branch:
   - If `plannedMoves > 0`: review plan and prepare for apply
   - If `metadataQueuePlannedCount > 0`: this is a prerequisite/metadata-preparation state (not a physical-move failure); run reextract/review flow first
   - If `unregisteredSkipped > 0`: `relocate apply` can auto-register them (DB tracking only), then rerun relocate after metadata is filled
   - Prefer `outcomeType`, `requiresMetadataPreparation`, and `nextActions` from the relocate tool result when present
   - Do **not** describe files as "already in the correct place" unless `alreadyCorrect > 0` is explicitly reported
   - `plannedMoves == 0` alone is insufficient to claim `alreadyCorrect`
4. Reextract/review (when metadata is missing):
   - `video_pipeline_reextract` (use relocate metadata queue)
   - `video_pipeline_export_program_yaml`
   - human review
   - `video_pipeline_apply_reviewed_metadata`
5. Relocate dry-run again (rebuild plan with fresh metadata)
6. Relocate apply (only after dry-run review)
7. Optional dedup dry-run

Do not run `backfill` expecting it to create metadata queue entries for `skippedExisting` rows. That is not its queue scope.
When `relocate` reports metadata gaps, do not summarize it as "failed" unless `ok=false`; summarize it as "metadata preparation required" and follow the suggested next actions.

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
  - `video_pipeline_prepare_relocate_metadata`
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

- "Possible path configuration mismatch. Do `sourceRoot` / `destRoot` / `windowsOpsRoot` point to real existing paths?"
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
