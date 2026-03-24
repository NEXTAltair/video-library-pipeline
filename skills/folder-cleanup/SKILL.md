---
name: video-library-pipeline-folder-cleanup
description: Fix contaminated folder/program titles with a user-specified primary flow. Use when the user points to a wrong folder/title (e.g. "フォルダ名がおかしい", "サブタイトルがフォルダに入ってる").
metadata: {"openclaw":{"emoji":"🧹","requires":{"plugins":["video-library-pipeline"]}}}
---

# Folder contamination cleanup (user-specified first)

## Critical rules

- **No exec/shell commands.** Use plugin tools only (+ file write for review YAML).
- Primary entry is **operator-specified wrong target** (path/title), not auto-detect full scan.
- Auto-detect full scan is optional audit mode only.
- Folder name = `program_title` only. Subtitle/episode info in folder names is contamination.

## Shared review YAML contract (same mental model as extract-review)

Use one human-facing review shape across review workflows:

```yaml
# review contract v1
review_type: "program_title_correction"
candidates:
  - current_value: "ヒューマニエンス 選『自律神経』あなたを操るもう一人のあなた"
    suggested_value: "ヒューマニエンス"
    approved_value:
    decision: "accept" # accept | edit | skip
```

Rules:
- `decision=accept` + `approved_value` empty → adopt `suggested_value`
- `decision=edit` + `approved_value` set → adopt `approved_value`
- `decision=skip` (or candidate deleted) → skip

Keep machine-only fields (`pathIds`, confidence, match source) **out of the editable YAML**.

## Primary flow (recommended)

### 1) Validate

`video_pipeline_validate {"checkWindowsInterop": true, "intent": "relocate"}`

Stop if `ok=false`.

### 2) Resolve from explicit operator input

When user gives a representative wrong path and/or wrong current title:

`video_pipeline_detect_folder_contamination {"representativePath":"<user path>", "targetProgramTitle":"<optional current wrong title>"}`

- This scoped mode must be preferred for operator-directed correction.
- If result has no candidates and user still wants broad audit, run full scan fallback:
  - `video_pipeline_detect_folder_contamination {}`

### 3) Write review YAML under `{windowsOpsRoot}/llm/`

Output path: `{wsl_windowsOpsRoot}/llm/folder_contamination_review_{YYYYMMDD_HHMMSS}.yaml`

Map each `contaminatedTitles` item to one `candidates[]` row:
- `current_value` ← `programTitle`
- `suggested_value` ← `suggestedTitle`
- `approved_value` ← empty
- `decision` ← `accept`

Wait for human edits.

### 4) Build updates and apply title correction

From edited YAML:
1. resolve `new_title` per candidate decision
2. map `current_value` back to step-2 detection result to get `pathIds`
3. build `[{"path_id":"...","new_title":"..."}]`

Run dry-run first:

`video_pipeline_update_program_titles {"updates": <array>, "dryRun": true}`

Then apply:

`video_pipeline_update_program_titles {"updates": <same array>, "dryRun": false}`

### 5) Relocate (dry-run → apply)

Dry-run:

`video_pipeline_relocate_existing_files {"apply": false, "roots": [<affected parent root>], "queueMissingMetadata": true, "writeMetadataQueueOnDryRun": true}`

After confirmation, apply:

`video_pipeline_relocate_existing_files {"apply": true, "planPath": "<planPath from dry-run>"}`

## Notes

- Use parent directory in `roots` (not a single contaminated child folder).
- If `requiresMetadataPreparation=true`, hand off to `skills/extract-review/SKILL.md`, then resume relocate.
