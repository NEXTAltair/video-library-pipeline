# video-library-pipeline dependencies

This document defines plugin-only runtime dependencies and the minimum preflight for safe execution.
For stage order and ownership boundaries, see `FLOW_AND_OWNERSHIP.md`.

## Scope

- Plugin package: `extensions/video-library-pipeline`
- Tool entrypoints: `src/tool-run.ts`, `src/tool-backfill.ts`, `src/tool-dedup.ts`, `src/tool-status.ts`, `src/tool-validate.ts`, `src/tool-reextract.ts`
- Python orchestration: `py/unwatched_pipeline_runner.py`, `py/backfill_moved_files.py`, `py/dedup_recordings.py`, `py/run_metadata_batches_promptv1.py`
- Windows operations: scripts under `<windowsOpsRoot>/scripts`

## Plugin config contract

Required config keys:

- `windowsOpsRoot`
- `sourceRoot`
- `destRoot`

Optional config keys:

- `db` (default: `<windowsOpsRoot>/db/mediaops.sqlite`)
- `defaultMaxFilesPerRun` (default: `200`)

Expected directories under `windowsOpsRoot`:

- `db`
- `move`
- `llm`
- `scripts`

Notes:

- Plugin hints file `rules/program_aliases.yaml` is optional. If missing, extraction continues in AI-only mode.
- Backfill roots file `rules/backfill_roots.yaml` is optional input for backfill tool; if unresolved, `destRoot` is used.
- Broadcast bucket rules `rules/broadcast_buckets.yaml` are used by dedup tool (terrestrial / bs_cs / unknown).
- `db/move/llm` are created by the runner when missing.
- `scripts` is required for runtime, but missing directory/files are auto-provisioned from plugin templates on `video_pipeline_validate` and `video_pipeline_analyze_and_move_videos`.
- Existing scripts under `<windowsOpsRoot>/scripts` are never overwritten by auto-provision.

## Required binaries

- `uv`
- `python` (invoked as `uv run python ...`)
- `pwsh` or `pwsh.exe` (PowerShell 7)

## Python runtime dependencies

- `pyyaml` is optional and only needed when loading YAML hints
- Python standard library modules (`sqlite3`, `argparse`, `json`, etc.)
- DB access is implemented with standard `sqlite3` only (no external ORM dependency)

## Required Windows-side scripts

Under `<windowsOpsRoot>/scripts`:

- `normalize_filenames.ps1`
- `unwatched_inventory.ps1`
- `apply_move_plan.ps1`
- `list_remaining_unwatched.ps1`

Template source of truth:

- Preferred (user override): `<windowsOpsRoot>/templates/windows-scripts/*.ps1`
- Fallback (plugin bundled): `<plugin-root>/assets/windows-scripts/*.ps1`

## Minimum preflight

1) config sanity

- `openclaw gateway call video-library-pipeline.status --json`
- `openclaw video-pipeline-status`

2) binary sanity

- `uv --version`
- `pwsh -NoProfile -Command "$PSVersionTable.PSVersion.ToString()"`

3) dry-run (1 file)

- `uv run python "<plugin-dir>/py/unwatched_pipeline_runner.py" --windows-ops-root "<windows-ops-root>" --source-root "<source-root>" --dest-root "<dest-root>" --max-files-per-run 1`

4) apply (1 file)

- `uv run python "<plugin-dir>/py/unwatched_pipeline_runner.py" --windows-ops-root "<windows-ops-root>" --source-root "<source-root>" --dest-root "<dest-root>" --max-files-per-run 1 --apply`

## Operational outputs

- DB state: `<windowsOpsRoot>/db/mediaops.sqlite`
- Move/audit artifacts: `<windowsOpsRoot>/move`
- Extraction artifacts: `<windowsOpsRoot>/llm`
- Hints dictionary: `<plugin-root>/rules/program_aliases.yaml`
- Backfill roots config: `<plugin-root>/rules/backfill_roots.yaml`
- Broadcast bucket rules: `<plugin-root>/rules/broadcast_buckets.yaml`
- Backfill artifacts:
  - `<windowsOpsRoot>/move/backfill_plan_*.jsonl`
  - `<windowsOpsRoot>/move/backfill_apply_*.jsonl`
  - `<windowsOpsRoot>/llm/backfill_metadata_queue_*.jsonl`
- Dedup artifacts:
  - `<windowsOpsRoot>/move/dedup_plan_*.jsonl`
  - `<windowsOpsRoot>/move/dedup_apply_*.jsonl`
  - `<windowsOpsRoot>/duplicates/quarantine`
