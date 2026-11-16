#!/usr/bin/env python3
"""Prepare note-backed overrides for the normal run-bound metadata review.

Reads JSONL metadata or a classification report containing rows. Does not open
SQLite, mutate extraction artifacts, route paths, or accept/apply a review.
"""
import argparse
import json
from pathlib import Path
import yaml
from video_pipeline.domain.program_genre_policy import prepare_program_genre_review


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True)
    parser.add_argument('--policy', help='JSON policy with evidence-backed aliases/rules/classifications')
    parser.add_argument('--review-template', help='Exact current run-owned YAML; preserves source/cohort/hints in draft copy')
    parser.add_argument('--report', required=True)
    parser.add_argument('--review-yaml', required=True)
    args = parser.parse_args()
    text = Path(args.input).read_text(encoding='utf-8-sig')
    try:
        data = json.loads(text)
        rows = data['rows'] if isinstance(data, dict) and 'rows' in data else ([data] if isinstance(data, dict) else data)
    except json.JSONDecodeError:
        rows = [json.loads(line) for line in text.splitlines() if line.strip()]
    policy = json.loads(Path(args.policy).read_text()) if args.policy else {}
    report = prepare_program_genre_review(rows, policy)
    # Never overwrite an immutable extraction artifact or input/policy file.
    body = {'hints': [], 'row_overrides': report['row_overrides']}
    if args.review_template:
        body = yaml.safe_load(Path(args.review_template).read_text(encoding='utf-8-sig'))
        if not isinstance(body, dict) or not isinstance(body.get('hints'), list) or not body.get('source_jsonl'):
            parser.error('template requires hints and source_jsonl from the current run')
        from video_pipeline.platform.pathscan_common import windows_to_wsl_path
        source_path = Path(windows_to_wsl_path(str(body['source_jsonl'])))
        if not source_path.is_absolute():
            parser.error('template source_jsonl must be absolute')
        cohort = [json.loads(line) for line in source_path.read_text(encoding='utf-8-sig').splitlines() if line.strip()]
        ids = {str(r['path_id']) for r in cohort if '_meta' not in r}
        edits = dict(body.get('row_overrides') or {})
        for pid, fields in report['row_overrides'].items():
            if pid not in ids:
                continue
            if pid in edits and any(k in edits[pid] and edits[pid][k] != v for k,v in fields.items() if k != 'review_note'):
                parser.error(f'existing row override conflicts with policy: {pid}')
            previous_note = str(edits.get(pid, {}).get('review_note') or '')
            edits[pid] = {**edits.get(pid, {}), **fields}
            if previous_note:
                edits[pid]['review_note'] = previous_note + '; ' + fields['review_note']
        body['row_overrides'] = edits
    inputs = {Path(p).resolve() for p in (args.input, args.policy, args.review_template) if p}
    outputs = [Path(args.report), Path(args.review_yaml)]
    if any(p.resolve() in inputs for p in outputs) or outputs[0].resolve() == outputs[1].resolve():
        parser.error('outputs must be distinct from inputs and each other')
    for path in outputs:
        if path.exists():
            parser.error(f'output already exists: {path}')
    outputs[0].write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    outputs[1].write_text(yaml.safe_dump(body, allow_unicode=True, sort_keys=False))
    print(json.dumps({'rows': report['rows'], 'programs': len(report['programs']),
                      'overrides': len(report['row_overrides']), 'held': len(report['held_rows'])}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
