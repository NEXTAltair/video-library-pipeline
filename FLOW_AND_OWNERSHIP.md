# video-library-pipeline flow and ownership

This document describes plugin-only execution order, ownership boundaries, and AI-primary extraction policy.

## 1) layer responsibilities

- TypeScript tool layer (`src/*.ts`)
  - validates plugin config (fail-fast)
  - resolves one canonical runtime config
  - launches Python runner with explicit args
- Python orchestration layer (`py/*.py`)
  - runs end-to-end pipeline stages
  - owns DB upsert/reconcile and artifact generation
  - writes run summary
- Windows PowerShell layer (`<windowsOpsRoot>/scripts/*.ps1`)
  - owns Windows filesystem mutation/enumeration
  - emits raw move/inventory evidence
  - performs internal long-path (`\\?\`) handling and emits normal Windows paths externally

## 2) runtime contract

Required plugin config:

- `windowsOpsRoot`
- `sourceRoot`
- `destRoot`

Optional plugin config:

- `db` (defaults to `<windowsOpsRoot>/db/mediaops.sqlite`)
- `defaultMaxFilesPerRun`

Runtime contract paths under `windowsOpsRoot`:

- `db`, `move`, `llm`, `scripts`
- `scripts` is auto-provisioned on validate/run if required PS1 files are missing.
- Plugin-managed scripts are auto-synced on validate/run/backfill/relocate/dedup (missing or outdated files are updated).
- User custom sub-scripts are outside plugin management and are not overwritten.
- Long path operation requires Windows `LongPathsEnabled=1` and PowerShell 7.

Plugin-local backfill config:

- `<plugin-root>/rules/backfill_roots.yaml`
  - optional roots/extensions source for `video_pipeline_backfill_moved_files`
  - overridden by tool params when provided

Plugin-local relocate config:

- `<plugin-root>/rules/relocate_roots.yaml`
  - optional roots/extensions source for `video_pipeline_relocate_existing_files`
  - safer than implicit `destRoot` scan; overridden by tool params when provided

Plugin-local dedup config:

- `<plugin-root>/rules/broadcast_buckets.yaml`
  - keyword rules for broadcast bucket classification (`terrestrial` / `bs_cs` / `unknown`)
  - used by `video_pipeline_dedup_recordings`

## 3) extraction policy (AI primary)

Active architecture: `A_AI_PRIMARY_WITH_GUARDRAILS`

- Primary path: AI-oriented parsing flow in `run_metadata_batches_promptv1.py`
- Optional guardrail input: `<plugin-root>/rules/program_aliases.yaml`
  - format: human-readable YAML hints (`canonical_title`, `aliases`)
  - no `id`, no regex-based rule engine
- Missing YAML hints is not fatal:
  - extraction continues in AI-only mode
  - unknown/ambiguous rows are surfaced with `needs_review=true`

User correction loop:

- human review updates hint dictionary (`hints` / `user_learned`)
- next runs load updated hints and improve canonical title normalization

## 4) ordered processing flow

1. Configure plugin values.
2. Optional backfill stage:
   - Trigger `video_pipeline_backfill_moved_files` (dry-run/apply)
   - scan roots and reconcile `paths/observations/events`
   - optional metadata queue generation for reextract flow
3. Optional dedup stage:
   - Trigger `video_pipeline_dedup_recordings` (dry-run/apply)
   - classify duplicates by metadata keys and optional broadcast bucket split
4. Optional relocate stage:
   - Trigger `video_pipeline_relocate_existing_files` (dry-run/apply)
   - physically move existing files using DB metadata + current placement rules
5. Trigger `video_pipeline_analyze_and_move_videos`.
6. `src/tool-run.ts` runs:
   - `uv run python py/unwatched_pipeline_runner.py --db ... --source-root ... --dest-root ... --windows-ops-root ... --max-files-per-run ... [--apply] [--allow-needs-review]`
7. Runner prepares `db/move/llm`.
8. Runner normalizes filenames and snapshots inventory via PowerShell.
9. Runner ingests inventory and builds metadata queue.
10. Runner executes extraction with optional YAML hints.
11. Runner builds move plan and applies (or dry-runs) move actions.
12. Runner reconciles DB paths, writes remaining report, rotates old artifacts.
13. Runner prints final JSON summary.

## 5) ownership map

- Config source of truth: plugin config (`plugins.entries.video-library-pipeline.config`)
- Hints source of truth: `<plugin-root>/rules/program_aliases.yaml`
- Backfill roots source of truth: `<plugin-root>/rules/backfill_roots.yaml`
- Relocate roots source of truth: `<plugin-root>/rules/relocate_roots.yaml`
- Dedup broadcast rules source of truth: `<plugin-root>/rules/broadcast_buckets.yaml`
- Script template source of truth:
  - Preferred: `<windowsOpsRoot>/templates/windows-scripts/*.ps1`
  - Fallback: `<plugin-root>/assets/windows-scripts/*.ps1`
- DB state: `<windowsOpsRoot>/db/mediaops.sqlite`
- Raw evidence: `<windowsOpsRoot>/move/*.jsonl`, `<windowsOpsRoot>/llm/*.jsonl`

## 6) boundary rules

- Do not implement Windows filesystem mutation directly in TypeScript or Python.
- For long-path-sensitive filesystem access, prefer pwsh7 helpers over WSL file I/O.
- Keep TS->Python argument names aligned exactly with runner CLI args.
- Treat hints as optional assistive input, not as the primary extraction engine.
