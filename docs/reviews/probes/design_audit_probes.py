"""Hermetic probes for the 2026-10-10 design audit, not production operations.

Exit 1: issue observed; 0: no issue observed; 2: probe failed/inconclusive.
The original audit used an internal read barrier to expose lost updates.
After M0, coordinate callers before entering the writer scope (never inside it).
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPOSITORY_ROOT / "py"))

from backup_mediaops_db import backup_db  # noqa: E402
from video_pipeline.workflows.store import WorkflowStore  # noqa: E402


def probe_wal_snapshot(root: Path) -> dict:
    db_path = root / "wal-probe.sqlite"
    connection = sqlite3.connect(db_path)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA wal_autocheckpoint=0")
        connection.execute("CREATE TABLE audit_probe(value TEXT)")
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        connection.execute("INSERT INTO audit_probe VALUES ('committed')")
        connection.commit()
        success, backup_path, error = backup_db(str(db_path), "design_probe")
        if not success:
            raise RuntimeError(f"backup helper failed: {error}")
        copied = sqlite3.connect(backup_path)
        try:
            backup_rows = copied.execute("SELECT COUNT(*) FROM audit_probe").fetchone()[0]
        finally:
            copied.close()
        source_rows = connection.execute("SELECT COUNT(*) FROM audit_probe").fetchone()[0]
        return {
            "helperReportedSuccess": success,
            "sourceCommittedRows": source_rows,
            "backupRows": backup_rows,
            "issueDetected": source_rows != backup_rows,
        }
    finally:
        connection.close()


def probe_concurrent_registration(root: Path) -> dict:
    store = WorkflowStore(root / "ops")
    run = store.init_run("source_root", run_id="run_concurrent_probe")
    paths = []
    for name in ("a", "b"):
        artifact_path = root / f"{name}.json"
        artifact_path.write_text(json.dumps({"probe": name}), encoding="utf-8")
        paths.append(artifact_path)

    barrier = threading.Barrier(2)

    def register(pair):
        name, artifact_path = pair
        barrier.wait(timeout=5)
        return store.register_artifact(
            run.run_id,
            artifact_type="audit_probe",
            path=artifact_path,
            producer="design_audit_probe",
            artifact_id=name,
        ).id

    with ThreadPoolExecutor(max_workers=2) as executor:
        successful = list(executor.map(register, zip(("a", "b"), paths)))
    persisted = store.read_run(run.run_id)
    return {
        "successfulCalls": successful,
        "persistedArtifactIds": persisted.artifact_ids,
        "issueDetected": set(successful) != set(persisted.artifact_ids),
    }


def main() -> int:
    if len(sys.argv) != 1:
        print("This probe takes no arguments and only uses temporary fixtures.", file=sys.stderr)
        return 2
    try:
        with tempfile.TemporaryDirectory(prefix="video-pipeline-design-audit-") as temporary:
            root = Path(temporary)
            report = {
                "probeVersion": 2,
                "walBackup": probe_wal_snapshot(root),
                "concurrentArtifactRegistration": probe_concurrent_registration(root),
            }
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return int(any(result["issueDetected"] for result in report.values() if isinstance(result, dict)))
    except Exception as exc:
        print(json.dumps({"probeFailed": True, "error": str(exc)}, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
