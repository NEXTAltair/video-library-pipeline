#!/usr/bin/env python3
"""Read-only historical EPG triage. Output is private review material, not corrections."""
import argparse
import json
from video_pipeline.application.historical_epg_audit import audit_database


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', required=True, help='Existing SQLite database (prefer a readonly production copy)')
    args = parser.parse_args()
    print(json.dumps(audit_database(args.db), ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
