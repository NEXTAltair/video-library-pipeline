# rules governance

This document defines how `program_aliases` hints are maintained for AI-primary extraction.

## Current contract

- Primary hints file:
  - `rules/program_aliases.yaml`
- Hints are optional:
  - missing hints file must not stop extraction
  - runtime falls back to AI-only extraction

## YAML format contract

- No `id`
- No regex fields
- Human-readable title dictionary only

Recommended structure:

```yaml
version: "YYYY-MM-DD"
description: "Program title hint dictionary"
hints:
  - canonical_title: "ドキュメント72時間"
    aliases:
      - "ドキュメント72時"
      - "手話で楽しむみんなのテレビ×ドキュメント72時間"
user_learned:
  - canonical_title: "午後LIVE_ニュースーン_4時台"
    aliases:
      - "午後LIVEニュースーン4時台"
```

## Quality checks

- Role boundary (must keep):
  - hints are optional guardrails for known title normalization errors.
  - hints must not become the primary extraction engine.
  - if a correction can be solved by improving AI extraction, prefer that path.

- `canonical_title` must be non-empty.
- `aliases` should be non-empty strings.
- Keep entries concise and deduplicated.
- `notes` can be tracked in commit messages or surrounding docs when needed.

## Validation procedure

Before deploy:

1. Parse YAML syntax.
2. Check duplicate alias entries mapping to different canonical titles.
3. Run one dry extraction with hints present.
4. Run one dry extraction with hints temporarily absent.
5. Compare summary counts (`needs_review`, extracted rows) with baseline.

## Tracking policy

- Track hint files in git.
- Ignore temporary backups (`*.bak`, `*.tmp`) in `.gitignore`.
