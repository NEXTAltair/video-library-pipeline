"""Run-bound metadata preflight and one atomic, replay-safe batch apply.

Generated review YAML is an editable draft; extraction inputs are immutable.
Approval applies the exact bytes captured by this command, never an arbitrary
caller-supplied file. No archive rename invalidates legacy artifact references.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import yaml

from backup_mediaops_db import backup_db
from export_program_yaml import by_program_group_from_path, lower_compact, looks_swallowed_program_title
from mediaops_schema import begin_immediate, connect_db, create_schema_if_needed
from path_placement_rules import TITLE_RELATED_REASONS, detect_subtitle_in_program_title
from upsert_path_metadata_jsonl import upsert_records, validate_db_contract
from video_pipeline.platform.pathscan_common import windows_to_wsl_path, now_iso


def local_path(value):
    return Path(windows_to_wsl_path(str(value))).resolve()


def suspicious(row):
    title = str(row.get("program_title") or "").strip()
    folder = by_program_group_from_path(row.get("path"))
    if detect_subtitle_in_program_title(title):
        return True
    if not folder:
        return False
    tn, fn = lower_compact(title), lower_compact(folder)
    return looks_swallowed_program_title(title, folder) or bool(
        tn and fn and fn.startswith(tn) and len(fn) >= len(tn) + 3)


def review_rows(rows, hints):
    aliases = {}
    for hint in hints:
        if not isinstance(hint, dict) or not isinstance(hint.get("canonical_title"), str):
            raise ValueError("review hint requires canonical_title")
        canonical = hint["canonical_title"].strip()
        if not canonical or not isinstance(hint.get("aliases", []), list):
            raise ValueError("invalid review aliases")
        for alias in [canonical, *hint.get("aliases", [])]:
            if not isinstance(alias, str) or not alias.strip():
                raise ValueError("invalid review alias")
            key = lower_compact(alias)
            if key in aliases and aliases[key] != canonical:
                raise ValueError("ambiguous review alias")
            aliases[key] = canonical
    canonicals = sorted(set(aliases.values()), key=lambda s: len(lower_compact(s)), reverse=True)
    result = []
    for row in rows:
        edited = dict(row)
        title = str(row.get("program_title") or "").strip()
        canonical = aliases.get(lower_compact(title))
        if not canonical:
            canonical = next((c for c in canonicals if lower_compact(title).startswith(lower_compact(c))
                              and len(lower_compact(title)) >= len(lower_compact(c)) + 8), None)
        if canonical:
            edited["program_title"] = canonical
            reasons = {part.strip() for part in str(edited.get("needs_review_reason") or "").split(",") if part.strip()}
            if edited.get("needs_review") is True and reasons <= TITLE_RELATED_REASONS and not suspicious(edited):
                edited["needs_review"] = False
                edited["needs_review_reason"] = ""
        ok, reason = validate_db_contract(edited)
        if not ok:
            raise ValueError(f"invalid reviewed metadata: {reason}")
        if not edited.get("program_title") or not edited.get("air_date"):
            raise ValueError("reviewed metadata requires title and air_date")
        if edited.get("needs_review") or suspicious(edited):
            raise ValueError("review-risk rows remain; edit the review before applying")
        if not edited.get("path_id"):
            raise ValueError("reviewed metadata requires registered path_id")
        edited["human_reviewed"] = True
        edited["human_reviewed_at"] = now_iso()
        result.append(edited)
    return result


def _freeze(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as stream:
            stream.write(content)
            stream.flush()
            import os
            os.fsync(stream.fileno())
    except FileExistsError:
        if path.read_bytes() != content:
            raise ValueError("accepted review snapshot was modified")


def preflight(store, config):
    run = store.read_run(config.run_id)
    if run.flow not in {"source_root", "relocate"}:
        raise ValueError(f"unsupported metadata review flow: {run.flow}")
    if run.phase != "review_required" or run.status != "active":
        raise ValueError(f"metadata review requires active review_required run, got {run.phase}/{run.status}")
    if config.run_revision is not None and config.run_revision != run.revision:
        raise ValueError("metadata review run revision conflict")
    gates = [run.review_gates[gid] for gid in run.review_gate_ids
             if run.review_gates[gid].type == "metadata_review" and run.review_gates[gid].status == "open"]
    if not gates:
        raise ValueError("open metadata_review gate is missing")
    if config.gate_id and config.gate_id not in {gate.id for gate in gates}:
        raise ValueError("metadata review gate does not belong to this open run")
    ids = list(dict.fromkeys(aid for gate in gates for aid in gate.artifact_ids))
    if not ids:
        raise ValueError("metadata review gate has no artifacts")
    if config.artifact_ids and (len(config.artifact_ids) != len(ids) or set(config.artifact_ids) != set(ids)):
        raise ValueError("metadata review artifact scope mismatch")
    stored_db = run.config_snapshot.get("db")
    if not stored_db:
        raise ValueError("run has no bound metadata database")
    db = local_path(stored_db)
    if config.db and local_path(config.db) != db:
        raise ValueError("metadata review database differs from run database")
    if not db.is_file():
        raise FileNotFoundError(f"metadata database missing: {db}")
    artifacts = [run.artifacts.get(aid) for aid in ids]
    if any(artifact is None or artifact.type != "metadata_review_yaml" or artifact.status != "available" for artifact in artifacts):
        raise ValueError("metadata review artifact missing, unavailable or wrong type")
    owned_paths = [local_path(artifact.path) for artifact in artifacts]
    if config.review_yaml_paths and (len(config.review_yaml_paths) != len(owned_paths)
                                    or {local_path(p) for p in config.review_yaml_paths} != set(owned_paths)):
        raise ValueError("review YAML does not belong to this run/gate")
    if len(set(owned_paths)) != len(owned_paths):
        raise ValueError("metadata review paths overlap")
    prepared, path_ids = [], set()
    for artifact, draft in zip(artifacts, owned_paths):
        if not draft.is_relative_to(store.run_dir(run.run_id).resolve()):
            raise ValueError("review draft escaped its run directory")
        draft_bytes = draft.read_bytes()
        digest = hashlib.sha256(draft_bytes).hexdigest()
        if config.review_digests and config.review_digests.get(artifact.id) != digest:
            raise ValueError("review draft digest differs from requested acceptance")
        body = yaml.safe_load(draft_bytes.decode("utf-8-sig"))
        if not isinstance(body, dict) or not isinstance(body.get("hints"), list):
            raise ValueError("invalid review YAML")
        sources = [run.artifacts.get(aid) for aid in artifact.input_artifact_ids]
        sources = [source for source in sources if source and source.type == "metadata_extract_output" and source.status == "available"]
        if len(sources) != 1:
            raise ValueError("review must have one run-owned extraction input")
        source = sources[0]
        source_path = local_path(source.path)
        if not source_path.is_relative_to(store.run_dir(run.run_id).resolve()):
            raise ValueError("extraction input escaped its run directory")
        if not body.get("source_jsonl") or local_path(body["source_jsonl"]) != source_path:
            raise ValueError("review source_jsonl differs from run-owned extraction input")
        source_bytes = source_path.read_bytes()
        source_digest = hashlib.sha256(source_bytes).hexdigest()
        if source_digest != source.sha256:
            raise ValueError("extraction input checksum changed")
        rows = []
        for line in source_bytes.decode("utf-8-sig").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if not isinstance(row, dict):
                raise ValueError("extraction JSONL contains non-object row")
            if "_meta" not in row:
                rows.append(row)
        if not rows:
            raise ValueError("review extraction contains no rows")
        cohort = body.get("cohort") or {}
        current_ids = sorted(str(row.get("path_id") or "") for row in rows)
        cohort_hash = hashlib.sha256("\n".join(current_ids).encode()).hexdigest()[:16]
        if cohort and (cohort.get("source_jsonl_sha256") not in {source_digest, source_digest[:16]}
                       or cohort.get("path_id_count") != len(current_ids)
                       or cohort.get("path_id_set_hash") != cohort_hash):
            raise ValueError("review cohort differs from extraction input")
        edited = review_rows(rows, body["hints"])
        for row in edited:
            if row["path_id"] in path_ids:
                raise ValueError("metadata review batches overlap")
            path_ids.add(row["path_id"])
        prepared.append({"artifact": artifact, "draft": draft_bytes, "digest": digest,
                         "source": source_bytes, "sourceDigest": source_digest, "rows": edited})
    return run, gates, db, prepared


def apply_reviewed_metadata(store, config):
    with store.writer_scope():
        run, gates, db, prepared = preflight(store, config)
        fingerprint = hashlib.sha256(json.dumps([
            [p["artifact"].id, p["digest"], p["sourceDigest"]] for p in prepared], sort_keys=True).encode()).hexdigest()
        operation = hashlib.sha256(json.dumps([str(store.run_dir(run.run_id).resolve()),
                                               sorted(gate.id for gate in gates)]).encode()).hexdigest()
        accepted_dir = store.run_dir(run.run_id) / "review" / "accepted" / fingerprint
        for index, item in enumerate(prepared):
            _freeze(accepted_dir / f"{index:04d}.yaml", item["draft"])
            _freeze(accepted_dir / f"{index:04d}.jsonl", item["source"])
        con = connect_db(str(db))
        try:
            begin_immediate(con)
            exists = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='workflow_metadata_commits'").fetchone()
            previous = con.execute("SELECT digest, summary_json FROM workflow_metadata_commits WHERE operation_id=?", (operation,)).fetchone() if exists else None
            if previous:
                if previous["digest"] != fingerprint:
                    raise ValueError("metadata already committed with a different accepted review; reconcile before changing it")
                con.rollback()
                return json.loads(previous["summary_json"])
            # Snapshot while SQLite holds the writer reservation. The source
            # connection is read-only, so no uncommitted work enters the backup.
            ok, backup_path, error = backup_db(str(db), "pre_apply_reviewed_metadata")
            if not ok:
                raise ValueError(f"metadata backup failed: {error}")
            create_schema_if_needed(con)
            rows = [row for p in prepared for row in p["rows"]]
            summary = upsert_records(con, rows, source="human_reviewed", strict=True)
            summary.update(ok=True, backupPath=backup_path, acceptedDigest=fingerprint,
                           acceptedDirectory=str(accepted_dir), artifactIds=[p["artifact"].id for p in prepared])
            con.execute("CREATE TABLE IF NOT EXISTS workflow_metadata_commits (operation_id TEXT PRIMARY KEY, digest TEXT NOT NULL, summary_json TEXT NOT NULL)")
            con.execute("INSERT INTO workflow_metadata_commits VALUES (?,?,?)", (operation, fingerprint, json.dumps(summary)))
            con.commit()
            return summary
        except BaseException:
            con.rollback()
            raise
        finally:
            con.close()
