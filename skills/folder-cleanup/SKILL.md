---
name: video-library-pipeline-folder-cleanup
description: Detect and fix contaminated folder names under by_program/. Use when the user says "フォルダ名がおかしい", "サブタイトルがフォルダに入ってる", "folder names look wrong", or "フォルダ分けが変".
metadata: {"openclaw":{"emoji":"🧹","requires":{"plugins":["video-library-pipeline"]}}}
---

# Folder contamination cleanup

## !! Critical rules !!

- **No exec / shell commands.** Use plugin tools only (+ file write for review YAML).
- **suggestedTitle is a proposal, not a decision.** Always present to user for review. Some folder names that contain separators ARE the complete canonical title.
- Folder name = `program_title` only. Subtitle, episode info, or guest names in folder names = contamination.
- **Selective approval is the default.** User may approve some, reject others, or override suggestedTitle.

## When to use this skill

- "フォルダ名がおかしい" / "フォルダ分けが変" / "サブタイトルがフォルダに入ってる"
- "folder names look wrong" / "folder cleanup"
- Folder names contain `▽▼◇「` or episode descriptions beyond the program title

## Tool sequence

> **Execution rule:** Steps 1→2 run consecutively without waiting for user confirmation.
> Pause at step 3 for user to edit the review YAML.

### Step 1: Validate and get config

```
video_pipeline_validate {"checkWindowsInterop": true, "intent": "relocate"}
```

Stop and report if `ok=false`.

From the result, extract **`windowsOpsRoot`** (e.g. `B:\_AI_WORK`). The WSL-equivalent path is needed for file writes — convert by replacing the drive letter: `B:\...` → `/mnt/b/...`. The `llm/` subdirectory under this path is where all review YAML files go (same location as `program_aliases_review_*.yaml` from extract-review).

### Step 2: Detect contamination

```
video_pipeline_detect_folder_contamination {}
```

Branch on result:
- `totalContaminatedTitles == 0` → Report clean, stop.
- `totalContaminatedTitles > 0` → Proceed to step 3. **Keep the detection result in context** — `pathIds` and other details are needed in step 4.

### Step 3: Write review YAML to `{windowsOpsRoot}/llm/`

**Output path** (required): `{wsl_windowsOpsRoot}/llm/folder_contamination_review_{YYYYMMDD_HHMMSS}.yaml`

Example: if `windowsOpsRoot` = `B:\_AI_WORK`, write to `/mnt/b/_AI_WORK/llm/folder_contamination_review_20260323_211200.yaml`.

The YAML is **for human editing only** — keep it minimal:

```yaml
# Folder contamination review
# - approved_title 空欄 → suggested_title を採用
# - approved_title 記入 → そちらを採用
# - 行ごと削除 → スキップ
candidates:
  - program_title: "ヒューマニエンス 選「自律神経」あなたを操るもう一人のあなた"
    suggested_title: "ヒューマニエンス"
    approved_title:
```

Map from the detection result's `contaminatedTitles` array:
- `program_title` ← `programTitle`
- `suggested_title` ← `suggestedTitle`
- `approved_title` ← always empty

Do NOT include `pathIds`, `confidence`, `matchSource`, `affectedFiles`, or other machine data in the YAML. The agent already has this from the step 2 result.

Present the file path to the user and wait for them to finish editing.

**[User review gate]** — User edits the YAML:
- Entry kept, `approved_title` empty → use `suggested_title`
- Entry kept, `approved_title` filled → use that title
- Entry deleted → skip

### Step 4: Read YAML and build title updates

After user signals completion, read the edited YAML. For each entry with `decision: accept` or `decision: edit`:

1. Determine `new_title`: use `approved_title` if `decision: edit`, otherwise `suggested_title`
2. Look up `pathIds` from the **step 2 detection result** by matching `programTitle`
3. Build one `{ "path_id": "<id>", "new_title": "<new_title>" }` per path_id

Then dry-run:

```
video_pipeline_update_program_titles {
  "updates": <constructed updates array>,
  "dryRun": true
}
```

Show preview. If the user says "OK":

```
video_pipeline_update_program_titles {
  "updates": <same array>,
  "dryRun": false
}
```

### Step 5: Relocate dry-run

After title correction, run relocate to move files to correct folders:

```
video_pipeline_relocate_existing_files {
  "apply": false,
  "roots": [<parent directory of affected folders>],
  "queueMissingMetadata": true,
  "writeMetadataQueueOnDryRun": true
}
```

Present relocate plan to user: number of files, source → destination paths.

**[User confirmation gate]** — User approves relocation plan.

### Step 6: Relocate apply

```
video_pipeline_relocate_existing_files {
  "apply": true,
  "planPath": "<planPath from step 5 dry-run result>"
}
```

### Step 7: Completion report

- Report: contaminated folders fixed, files relocated.
- Distinguish "title correction" from "physical move".
- If any files had `missingMetadata`, note they need extraction before relocation.

## Handoff

- If `requiresMetadataPreparation=true` after relocate dry-run, hand off to `skills/extract-review/SKILL.md` for metadata extraction, then restart from step 5.
- For the relocate portion (steps 5-6), follow the same rules as `skills/relocate-review/SKILL.md`.

## roots specification rule

- Always specify the **parent directory** as roots, not individual contaminated folders.
  - Correct: `roots=["B:\\VideoLibrary"]`
  - Wrong: `roots=["B:\\VideoLibrary\\ヒューマニエンス 選「自律神経」..."]`
- This ensures sibling contaminated folders are all included in the scan.
