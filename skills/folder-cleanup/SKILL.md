---
name: video-library-pipeline-folder-cleanup
description: V2 migration note for folder contamination cleanup; direct legacy cleanup tools are not public in the V2 surface.
metadata: {"openclaw":{"emoji":"🧹","requires":{"plugins":["video-library-pipeline"]}}}
---

# Folder Cleanup In V2

Folder contamination cleanup is not exposed as a standalone V2 public workflow in #109.

## Current Rule

- Do not call hidden legacy cleanup or title-repair tools directly.
- Do not use shell commands to inspect Windows library paths.
- If a relocate run returns a review gate or next action related to metadata/title cleanup, follow that run-scoped action.
- Otherwise, report that folder cleanup needs a future V2 workflow or an explicit maintenance/admin path.

## What To Preserve For Future V2 Work

- Folder name should equal `program_title` only.
- Subtitle, episode descriptions, guest names, and separator tails do not belong in the folder title.
- Human review should use run-scoped artifacts and review gates.
- Any future cleanup flow must resume by `runId` and artifact IDs, not by latest YAML or ad-hoc path scans.

## Handoff

If the user's actual goal is to relocate existing files and no direct title cleanup is required, use `skills/relocate-review/SKILL.md`.
