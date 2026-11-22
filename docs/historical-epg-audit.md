# Historical EPG audit

`./scripts/uv-project run python py/audit_historical_epg_genres.py --db /private/copy.sqlite`

This command opens an existing SQLite database using `mode=ro` and query-only
mode. It does not initialize schemas, update metadata, accept reviews or move
recordings. Keep its output private: titles and path IDs are operational data.
Prefer a consistent readonly production database copy for operational checks.

The audit reports exact EPG arrays shared across distinct normalized titles,
and a deliberately broad fallback of documentary/information rows with EPG
arrays. The fallback retains isolated historical contamination (including
accepted rows) without guessing the real genre from a title. Legitimate cooking
and documentary programs will also appear. Reasons are triage signals, not
proof of cross-program copying or attribution to any historical bug. No proposed
classification or automatic correction is generated (`row_overrides` is empty).

For each candidate, independently check recording/official program evidence.
Create a normal run-bound metadata review for the relevant paths. For already
placed files use `prepare_placed_metadata` then `apply_placed_metadata`; this
reviews metadata without moving recordings. Edit the run-owned YAML with an
explicit per-path override, for example (synthetic IDs only):

```yaml
row_overrides:
  synthetic-path-id:
    genre: ドラマ
    epg_genres: null
    review_note: Independent program evidence checked; unrelated EPG discarded.
```

Use a corrected EPG array only when supported by independent evidence; `null`
or `[]` explicitly clears inherited evidence. A genre-only program decision
does not clear EPG. Preserve the source/cohort and all unrelated rows. Follow
the normal review digest, acceptance and risk checks; title and air date remain
required. Merely generating an audit or editing YAML never writes the DB.
Accepted human/rule/agent rows can be re-reviewed through this existing path;
new agent acceptance records supersession rather than claiming old human
confirmation. Do not use unreviewed ingestion to bypass accepted-row protection.

Known limitation: identical genre arrays are common, so false positives are
intentional. Row-level historical attribution and actual production correction
remain operational review, not an automatic migration.
