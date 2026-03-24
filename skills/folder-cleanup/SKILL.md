---
name: video-library-pipeline-folder-cleanup
description: Fix contaminated folder names under by_program/ with operator-directed input as the primary path. Use when the user says "このフォルダが間違い", "フォルダ名がおかしい", "サブタイトルがフォルダに入ってる", "folder names look wrong", or "フォルダ分けが変".
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

### Step 2: Detect contamination (operator-directed primary path)

```
video_pipeline_detect_folder_contamination {
  "programTitle": "<optional: current wrong title>",
  "representativePathLike": "<optional: %...wrong folder/path...%>",
  "preferredTitle": "<optional: operator-intended canonical title>"
}
```

Rules:
- Prefer explicit user-provided target (`programTitle` and/or `representativePathLike`) when available.
- Use full scan (`{}`) only as a secondary audit mode when the user asks for broad cleanup.

Branch on result:
- `totalContaminatedTitles == 0` → Report clean, stop.
- `totalContaminatedTitles > 0` → Proceed to step 3. **Keep the detection result in context** — `pathIds` and other details are needed in step 4.

### Step 3: Write review YAML to `{windowsOpsRoot}/llm/`

**Output path** (required): `{wsl_windowsOpsRoot}/llm/folder_contamination_review_{YYYYMMDD_HHMMSS}.yaml`

Example: if `windowsOpsRoot` = `B:\_AI_WORK`, write to `/mnt/b/_AI_WORK/llm/folder_contamination_review_20260323_211200.yaml`.

The YAML is **for human editing only** and should use the **same review contract as extract-review** (`program_aliases_v1`):

```yaml
# Folder contamination review (shared contract: program_aliases_v1)
# - canonical_title をそのまま → 提案採用
# - canonical_title を編集 → 上書き
# - エントリ削除 → スキップ
hints:
  - canonical_title: "ヒューマニエンス"
    aliases:
      - "ヒューマニエンス 選「自律神経」あなたを操るもう一人のあなた"
```

Map from the detection result's `reviewYamlTemplate.hints` (preferred) or `contaminatedTitles`:
- `canonical_title` ← `suggestedTitle` (or edited by human)
- `aliases[]` ← include current contaminated `programTitle`

Do NOT include `pathIds`, `confidence`, `matchSource`, `affectedFiles`, or other machine data in the YAML.

Present the file path to the user and wait for them to finish editing.

**[User review gate]** — User edits the YAML:
- Entry kept, `canonical_title` unchanged → accept suggestion
- Entry kept, `canonical_title` edited → use edited canonical title
- Entry deleted → skip

### Step 4: Read YAML and build title updates

After user signals completion, read the edited YAML. For each remaining entry:

1. Read `hints[].canonical_title` and `hints[].aliases[]`
2. For each alias, look up `pathIds` from the **step 2 detection result** by matching `programTitle`
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

After title correction, run relocate to move files to correct folders.
Use `affectedRoots` returned by `video_pipeline_update_program_titles` as the default `roots` value (fallback: parent directory of affected folders):

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
