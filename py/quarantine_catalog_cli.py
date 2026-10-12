#!/usr/bin/env python3
"""Build one searchable HTML view; never move or delete quarantined recordings."""
import argparse
import json
from pathlib import Path

from video_pipeline.platform.pathscan_common import windows_to_wsl_path
from video_pipeline.platform.quarantine_catalog import create_catalog


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--handoff", required=True, help="Existing verified deletion-handoff.json")
    parser.add_argument("--ops-root", required=True, help="Root containing original duplicates/runs records")
    parser.add_argument("--out", required=True, help="New HTML file outside quarantine and exact run directories")
    args = parser.parse_args(argv)
    try:
        result = create_catalog(*(Path(windows_to_wsl_path(value)) for value in (args.handoff, args.ops_root, args.out)))
    except Exception as error:
        result = {"ok": False, "error": str(error)}
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
