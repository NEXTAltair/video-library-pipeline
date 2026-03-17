---
name: video-library-pipeline-relocate-review
description: Relocate existing video files to correct folders based on DB metadata. Use when the user says "フォルダを移動したい", "整理したい", "サブタイトルがフォルダ名になってる", or wants to reorganize files already under B:\VideoLibrary or any library drive.
metadata: {"openclaw":{"emoji":"📁","requires":{"plugins":["video-library-pipeline"]}}}
---

# Relocate existing files

## !! Critical rules !!

- **No exec / shell commands.** B:\VideoLibrary is not directly accessible from Linux.
  - Wrong: `exec find "B:\VideoLibrary" -name "foo"`
  - Correct: `video_pipeline_relocate_existing_files {"apply": false, "roots": ["B:\\VideoLibrary\\foo"]}`
- **Only tool: `video_pipeline_relocate_existing_files`**
- Folder structure rules (`<program_title>/<year>/<month>/`) are defined in `drive_routes.yaml`. Do not ask the user how to organize files.
- Destination drives are determined automatically by genre. Do not ask the user which drive to use.

## When to use this skill

- "フォルダを移動したい" / "整理したい" / "正しい場所に移動したい"
- Folder names contain subtitle / extra phrases beyond the program title
- User wants to relocate existing files under VideoLibrary or any library drive
- Any manual operation using `video_pipeline_relocate_existing_files`

## roots指定ルール (required)

- 番組名で整理を依頼された場合、roots は **親ディレクトリ** を指定する。
  - 「おぎやはぎの愛車遍歴を整理」→ `roots=["B:\\VideoLibrary"]` (親ディレクトリ)
  - 個別フォルダ `B:\\VideoLibrary\\おぎやはぎの愛車遍歴` を指定すると、`おぎやはぎの愛車遍歴▽...` のような兄弟フォルダが範囲外になる。
- サブタイトル付き兄弟フォルダ（▽/▼含み）を漏らさないため、常に親を指定すること。

## Tool sequence

> **Execution rule:** Steps 1→2→3 run consecutively without waiting for user confirmation.
> Only pause for user confirmation at step 4 (before apply).
> Do not output text and stop mid-flow — always continue to the next tool call.

1. **Validate**
   ```
   video_pipeline_validate {"checkWindowsInterop": true, "intent": "relocate"}
   ```
   Stop and report if `ok=false`. If `ok=true`, follow the `nextStep` field in the result and proceed to step 2.

2. **Dry-run (required)**
   ```
   video_pipeline_relocate_existing_files {
     "apply": false,
     "roots": ["B:\\VideoLibrary"],
     "queueMissingMetadata": true,
     "writeMetadataQueueOnDryRun": true,
     "scanErrorPolicy": "warn",
     "scanRetryCount": 2
   }
   ```

3. **Branch on dry-run result**

   | State | Next action |
   |-------|-------------|
   | `plannedMoves > 0` and `requiresMetadataPreparation=false` | → Step 4 (user confirmation) |
   | `requiresMetadataPreparation=true` or `metadataQueuePlannedCount > 0` | → Run `/extract-review` to fill metadata, then restart from step 2 |
   | `suspiciousProgramTitleSkipped > 0` | → Follow `followUpToolCalls` (通常は reextract) でメタデータ修正、その後 step 2 再実行 |
   | `unregisteredSkipped > 0` | → Apply to auto-register, then fill metadata and re-run |
   | `plannedMoves == 0` and `alreadyCorrect > 0` and `suspiciousProgramTitleSkipped == 0` | → Report all files already in correct location |
   | `plannedMoves == 0` and `alreadyCorrect` not reported | → State explicitly: cannot confirm correct placement; report reason |

   - Prefer `outcomeType` / `nextActions` / `diagnostics` from the tool result when present.
   - `suspiciousProgramTitleSkipped > 0` はサブタイトル汚染 (▽/▼がprogram_titleに混入)。`already_correct` ではない。
   | User says title is wrong / folder name ≠ program name | → Use `video_pipeline_update_program_titles` to fix title first (dryRun=true then apply), then restart from step 2 |
   - Do not describe metadata gaps as "failed" — call them "metadata preparation required".

4. **Show dry-run plan to user and get confirmation**
   - Summarize: number of files to move, source paths, destination paths.
   - Do not apply until the user explicitly approves.

5. **Apply**
   ```
   video_pipeline_relocate_existing_files {
     "apply": true,
     "planPath": "<plan_path from dry-run result>"
   }
   ```
   - **`planPath` is required.** Obtain from dry-run result. Never apply without `planPath`.

6. **Completion report**
   - Distinguish "physical move complete" from "DB sync" explicitly.
   - If `autoRegisteredPaths > 0`, show the `autoRegisteredFiles` list to the user.

## Human review checklist

- [ ] User reviewed `plannedMoves` count and destination paths from dry-run
- [ ] If metadata gaps exist, `/extract-review` was completed first
- [ ] User gave explicit approval before apply
- [ ] `planPath` was taken from dry-run result and passed to apply
- [ ] Post-apply: `exitCode == 0` / `movedCount` verified

## Handoff to extract-review

If `requiresMetadataPreparation=true` is returned:

1. Pass the metadata queue path (`metadataQueuePath` from dry-run result) as `queuePath` to the `/extract-review` skill.
2. After extract-review completes (`human_reviewed_metadata` written), restart this skill from step 2.
