---
name: video-library-pipeline
description: Run and inspect the video library pipeline via OpenClaw plugin tools. Interactive runs must use the 3-stage flow with human review at every gate.
metadata: {"openclaw":{"emoji":"🎬","requires":{"plugins":["video-library-pipeline"]},"localReads":["~/.openclaw/openclaw.json"]}}
---

# video-library-pipeline

This skill is the orchestrator for `video-library-pipeline`.

## Rules

- Use plugin tools, not direct script calls. Never substitute with `exec` shell commands.
  - `video_pipeline_*` names are tool names, not shell commands.
  - Wrong: `exec find "B:\VideoLibrary" -name "*foo*"`
  - Correct: call `video_pipeline_relocate_existing_files` directly as a tool.
- Execute in the main agent turn; do not delegate to subagents.
- Do not output explanation text and stop mid-flow. If you say "I will check X", immediately call the tool for X in the same turn.
- Do not treat plugin tool names as workspace skill names. If a `read` attempt fails looking for a `skills/<tool-like-name>/SKILL.md`, recover by calling the plugin tool directly.
- If a tool call cannot be issued in the current environment, stop and report "tool registry / permissions issue".

## Intent Mapping (required)

Classify the user request first, then **immediately read the sub-skill SKILL.md and follow its tool sequence**. Do not wait for further user input after routing.

| User intent | → Action |
|-------------|----------|
| Reorganize/relocate existing files ("整理したい", "フォルダを移動したい", subtitle in folder name, etc.) | Read `skills/relocate-review/SKILL.md`, then follow its sequence |
| Process new recordings from `sourceRoot` (B:\未視聴) | Read `skills/inventory-review/SKILL.md`, then follow its sequence |
| DB sync only ("DB化", "DBに登録して", "既存ファイルをDBに入れて") | Call `video_pipeline_backfill_moved_files` directly (no `roots` param needed) |
| Ingest EPG (program.txt capture) | Call `video_pipeline_ingest_epg` (run before deleting program.txt) |
| Re-run metadata extraction only | Read `skills/extract-review/SKILL.md`, then follow its sequence |
| Rebroadcast detection | Call `video_pipeline_detect_rebroadcasts` directly (EPG `[再]` flag based: `rebroadcast` / `original` / `unknown`) |

If the user asks about cleanup/reorganization for an already-existing directory tree, treat that as **relocate flow** (read `skills/relocate-review/SKILL.md`), not the `sourceRoot` pipeline flow.

## Metadata state labels (single source)

- `machine_extracted_unreviewed_metadata`: output of `video_pipeline_reextract`; LLM/rule-generated candidates, not yet human-verified
- `human_reviewed_metadata`: visually reviewed metadata reflected by `video_pipeline_apply_reviewed_metadata`
- `auto_registered_file_facts`: `paths` / `observations` / register-type events (file existence facts, not program interpretation)

## Folder naming rule (required)

- Folder name = `program_title` only. Never include `subtitle`, episode description, or guest names in folder names.
- The pipeline builds destination paths as `<dest_root>/<program_title>/<year>/<month>/<filename>`.
- If `program_title` looks like it contains a subtitle (e.g. "おぎやはぎの愛車遍歴▽黒木瞳超難関!"), the extraction is wrong — fix `program_title` to "おぎやはぎの愛車遍歴" and move the rest to `subtitle`.

## Tool scope (required)

- `video_pipeline_analyze_and_move_videos` — targets `sourceRoot` only. `remaining_files` is `sourceRoot`-scoped; it does not reflect status of any other root.
- `video_pipeline_backfill_moved_files` — DB sync only, no physical moves. `backfill_roots.yaml` pre-configures all drives; call without `roots` unless scanning a new location. `queueMissingMetadata` scope is limited to rows touched by that run.
- `video_pipeline_relocate_existing_files` — physical relocation for existing files via explicitly provided `roots`. On `apply=true`, `unregistered_path` files are auto-registered into DB but still cannot move until metadata exists.
- `video_pipeline_prepare_relocate_metadata` — orchestrates relocate dry-run + reextract. Does not apply metadata or physically move files. Prefer `followUpToolCalls` from its result.

## Reporting (required)

- Always state scope explicitly: "DB sync complete" vs "physical move complete"; which root was scanned.
- If `autoRegisteredPaths > 0`, show the `autoRegisteredFiles` list (or note it was truncated).
- If a tool result includes `followUpToolCalls`, use those exact tool names/params.
- Do not use `remaining_files` as evidence about any root other than `sourceRoot`.

## Command naming guardrail (required)

Valid tool names (tool calls only — not CLI commands):
`video_pipeline_validate`, `video_pipeline_backfill_moved_files`, `video_pipeline_relocate_existing_files`, `video_pipeline_prepare_relocate_metadata`, `video_pipeline_dedup_recordings`, `video_pipeline_analyze_and_move_videos`, `video_pipeline_logs`, `video_pipeline_status`, `video_pipeline_reextract`, `video_pipeline_apply_reviewed_metadata`, `video_pipeline_export_program_yaml`, `video_pipeline_repair_db`, `video_pipeline_ingest_epg`, `video_pipeline_detect_rebroadcasts`

Only plugin CLI helper: `openclaw video-pipeline-status`

## Path sanity (required)

Before pipeline execution, ask a short confirmation when path mismatch is possible:
> "Possible path configuration mismatch. Do `sourceRoot` / `destRoot` / `windowsOpsRoot` point to real existing paths?"

If an error says `... does not exist`, treat it as config/path mismatch first.

## Alert guidance (cron/automation)

Alert if any of:
- `validate ok=false` / non-zero exit code / summary JSON parse fails
- `remaining_files > 0`
- `plan_stats.skipped_needs_review > 0`
- `plan_stats.skipped_missing_fields > 0`
- `plan_stats.skipped_outside > 0`
- summary/log pointers missing (`inventory`, `queue`, `plan`, `applied`)

Do not alert about arbitrary existing-root cleanup based only on `remaining_files`. On healthy run, avoid noisy reports.
