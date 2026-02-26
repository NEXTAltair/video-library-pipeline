# video-library-pipeline current spec index

This file is the **documentation entrypoint** for the current plugin behavior.

Use this document first when you need to answer:
- "What is the plugin supposed to do right now?"
- "Which file is the source of truth for this question?"
- "Is this requirement doc historical or current?"

## 1) read order (recommended)

1. `skills/SKILL.md`
   - AI agent execution rules, intent mapping, tool usage guardrails
2. `FLOW_AND_OWNERSHIP.md`
   - current execution order, layer boundaries, ownership
3. `DEPENDENCIES.md`
   - runtime prerequisites, binaries, scripts, preflight
4. `src/tool-definitions.ts`
   - current tool schemas (params/result contract at plugin layer)
5. Python/TS implementation files (`src/*.ts`, `py/*.py`)
   - actual behavior details and edge-case handling

## 2) source of truth by question

### "Which tool should be used for this request?"
- Primary: `skills/SKILL.md`
- Secondary: `src/tool-definitions.ts`

### "What does this tool mean / what scope does its result cover?"
- Primary: `skills/SKILL.md` (tool scope / result semantics)
- Secondary: `src/tool-definitions.ts`
- Implementation details: `src/tool-*.ts`, `py/*.py`

### "What is the current execution flow?"
- Primary: `FLOW_AND_OWNERSHIP.md`
- Secondary: `skills/SKILL.md` (agent-facing winning flows)

### "What binaries/config/scripts are required?"
- Primary: `DEPENDENCIES.md`
- Secondary: `src/tool-validate.ts`, `src/windows-scripts-bootstrap.ts`

### "What exactly happens in DB update / move reconciliation?"
- Primary implementation:
  - `py/update_db_paths_from_move_apply.py`
  - `py/relocate_existing_files.py`
  - `py/backfill_moved_files.py`
- Supporting docs:
  - `FLOW_AND_OWNERSHIP.md`
  - `DEPENDENCIES.md`

### "What should the AI agent ask the human to review?"
- Primary: `skills/extract-review/SKILL.md`
- Supporting tool result shape: `src/tool-export-program-yaml.ts`

## 3) current operational docs (live behavior)

- `skills/SKILL.md`
  - top-level orchestration skill
  - intent mapping (natural language -> tool flow)
  - agent guardrails (tool-vs-skill confusion, shell fallback prohibition)
- `skills/normalize-review/SKILL.md`
  - sourceRoot pipeline stage 1 (normalization + review gate)
- `skills/extract-review/SKILL.md`
  - metadata review stage
  - YAML is human-only artifact; agent should use structured fields
- `skills/move-review/SKILL.md`
  - move/apply stage review gate
- `FLOW_AND_OWNERSHIP.md`
  - runtime boundaries and ordered processing flow
- `DEPENDENCIES.md`
  - current runtime prerequisites, long-path assumptions, preflight

## 4) historical / planning docs (not current behavior by default)

These are valuable for intent and design history, but may not match current implementation.

- `BACKFILL_MOVED_FILES_REQUIREMENTS.md`
  - historical requirements and design notes for backfill feature
- `DUPLICATE_DEDUP_REQUIREMENTS.md`
  - historical requirements and design notes for dedup feature

Rule:
- Do **not** treat these files as the current behavior spec unless verified against:
  - `skills/SKILL.md`
  - `FLOW_AND_OWNERSHIP.md`
  - `DEPENDENCIES.md`
  - `src/tool-definitions.ts`

## 5) code-level source of truth (when docs disagree)

If there is a mismatch between docs:

1. `src/tool-definitions.ts` (tool params/schema)
2. `src/tool-*.ts` (tool wrapper behavior and returned fields)
3. `py/*.py` (core execution logic)
4. `assets/windows-scripts/*.ps1` (Windows FS behavior)

For runtime incident triage, prefer:
- tool JSON results
- audit JSONL artifacts under `<windowsOpsRoot>/move` and `<windowsOpsRoot>/llm`
- DB state in `<windowsOpsRoot>/db/mediaops.sqlite`

## 6) maintenance rule for future edits

When changing plugin behavior:

- Update code first
- Then update:
  - `src/tool-definitions.ts` (if tool schema changed)
  - `skills/SKILL.md` (if agent behavior or interpretation changes)
  - `FLOW_AND_OWNERSHIP.md` / `DEPENDENCIES.md` (if flow/dependency changed)
- If a requirement doc becomes stale, either:
  - add/update a `Status / Source of Truth` note, or
  - explicitly mark sections as historical

## 7) quick anti-confusion checklist

Before assuming a doc is current, check:

- Does it describe a tool that exists in `src/tool-definitions.ts`?
- Does it match current tool names (`video_pipeline_*`)?
- Does it mention `prepare_relocate_metadata` / `relocate` if discussing existing-root cleanup?
- Does it distinguish:
  - DB sync (`backfill`)
  - metadata prep (`prepare_relocate_metadata` / `reextract`)
  - physical relocation (`relocate`)
- Does it match current long-path assumptions (`pwsh7`, `LongPathsEnabled=1`)?

If any answer is "no", treat the doc as historical until verified in code.
