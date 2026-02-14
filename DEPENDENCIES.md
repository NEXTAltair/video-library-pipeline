# video-library-pipeline dependencies

This document is the fixed reference for runtime dependencies and preflight checks.
For ordered execution and file ownership, see `FLOW_AND_OWNERSHIP.md`.

## Scope

- Targets: `src/tool-run.ts`, `src/tool-status.ts`, `src/tool-validate.ts`
- Python entrypoints are resolved from `extensions/video-library-pipeline/py/*`
- Windows file operations are delegated to `pwsh.exe -File` scripts under `<windowsOpsRoot>/scripts`

## External dependencies

### Required binaries

- `uv`
- `python` runtime (recommended execution: `uv run --python <venv-python>`)
- `pwsh` available on PATH (PowerShell 7)

### Required Python modules

- `sqlalchemy` (used by ingest/update/upsert scripts)
- standard library modules (`sqlite3`, `argparse`, `json`, etc.) are assumed available

### Required Windows-side scripts

Under `<windowsOpsRoot>/scripts`:

- `normalize_filenames.ps1`
- `fix_prefix_timestamp_names.ps1`
- `normalize_unwatched_names.ps1`
- `unwatched_inventory.ps1`
- `apply_move_plan.ps1`
- `list_remaining_unwatched.ps1`

### Maintenance-only Windows-side scripts (not in active run flow)

- `repair_collisions_nested_drive.ps1`
- `rollback_rename_jsonl.ps1`

### Required paths and data

- SQLite DB: `<windowsOpsRoot>/db/mediaops.sqlite`
- Move logs dir: `<windowsOpsRoot>/move`
- LLM work dir: `<windowsOpsRoot>/llm`
- Rules file (default): `<windowsOpsRoot>/rules/program_aliases.json`
- Windows ops root (`windowsOpsRoot`): configured per environment

### Active output roots (authoritative)

- move/audit outputs: `<windowsOpsRoot>/move`
- extraction intermediate outputs: `<windowsOpsRoot>/llm`
- do not use ad-hoc output roots for active flow (for example legacy `inventory/`, `logs/`)

### History model (operational contract)

- DB is the normalized history/state layer (`runs/events/paths/...`).
- JSONL is the raw runtime evidence layer (stage audit, input/output artifacts).
- `move/LATEST_SUMMARY.md` is the human-first layer.

### Runtime roots supplied by plugin config

- source root: `sourceRoot`
- destination root: `destRoot`

## Preflight checklist

Run these before production apply:

1) plugin config sanity

- `openclaw gateway call video-library-pipeline.status --json`
- `openclaw video-pipeline-status`

2) binary sanity

- `uv --version`
- `pwsh -NoProfile -Command "$PSVersionTable.PSVersion.ToString()"`

3) dry-run

- `uv run --python "<venv-python-path>" python "<video-library-pipeline-dir>/py/unwatched_pipeline_runner.py" --max-files-per-run 1`

4) limited apply

- `uv run --python "<venv-python-path>" python "<video-library-pipeline-dir>/py/unwatched_pipeline_runner.py" --max-files-per-run 1 --apply`

5) post-check

- check latest `LATEST_SUMMARY.md` in `<windowsOpsRoot>/move`
- check latest `move_apply_*.jsonl` in `<windowsOpsRoot>/move`
- verify `runs/events` rows in `<windowsOpsRoot>/db/mediaops.sqlite`

## Notes

- `video_pipeline_status` / `video_pipeline_logs` are plugin tools, not Gateway RPC methods.
- Keep all Windows filesystem actions in `ps1` scripts. Avoid inline `-Command` for file operations.
- `video_pipeline_analyze_and_move_videos` is the primary entrypoint for analyze+move flow.
