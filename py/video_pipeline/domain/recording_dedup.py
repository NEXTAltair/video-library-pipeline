"""Episode dedup policy: original and repeat broadcasts are never interchangeable.

Planning is read-only. Unknown/conflicting broadcast evidence is NOT an original
broadcast, even if another recording in the group has a repeat flag.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import subprocess
import stat
import time
import unicodedata

from video_pipeline.db.db_helpers import reconstruct_path_metadata
from video_pipeline.platform.pathscan_common import windows_to_wsl_path, wsl_to_windows_path


def text_key(value):
    # Preserve season/series suffixes and punctuation. No franchise/prefix aliases.
    return re.sub(r"\s+", "", unicodedata.normalize("NFKC", str(value or "")).casefold())


def episode_key(md):
    title = text_key(md.get("program_title"))
    if not title or title in {"unknown", "不明"}:
        return None
    ep = text_key(md.get("episode_no"))
    if re.fullmatch(r"\d+", ep):
        ep = str(int(ep))
    sub = text_key(md.get("subtitle"))
    if not ep and (len(sub) < 2 or sub in {"未定", "不明", "unknown"}):
        return None
    return (title, text_key(md.get("season_no")), "ep" if ep else "sub", ep or sub)


def explicit_flag(value):
    # Never use truthiness: "false" and missing/null are not repeat evidence.
    if value is True or (type(value) is int and value == 1):
        return True
    if value is False or (type(value) is int and value == 0):
        return False
    return None


def broadcast_kind(md, broadcasts):
    flags = [explicit_flag(b["is_rebroadcast_flag"]) for b in broadcasts]
    explicit = explicit_flag(md.get("is_rebroadcast_flag"))
    if explicit is not None:
        flags.append(explicit)
    known = {x for x in flags if x is not None}
    raw = str(md.get("path") or "")
    repeat_marker = bool(re.search(r"\[再\]|［再］|【再】|🈞", raw))
    if repeat_marker:
        known.add(True)
    if len(known) > 1:
        return "unknown", "conflicting_broadcast_flags"
    if flags and None not in flags and len(known) == 1:
        return ("repeat" if True in known else "original"), "explicit_epg_flag"
    # A positive [再] marker is evidence; absence of a marker is not evidence.
    if repeat_marker:
        if False in known:
            return "unknown", "conflicting_broadcast_flags"
        return "repeat", "filename_repeat_marker"
    return "unknown", "broadcast_kind_unknown"


def file_signature(path, *, native_cache=None):
    p = Path(path)
    try:
        st = p.stat()
        if p.is_symlink() or not stat.S_ISREG(st.st_mode):
            raise ValueError("missing_or_symlink")
    except OSError as exc:
        if not str(p).startswith('/mnt/'):
            raise ValueError("missing_or_symlink") from exc
        from video_pipeline.platform.native_file_signatures import native_path_key, read_native_signatures
        key = native_path_key(p)
        item = native_cache.get(key) if native_cache is not None else read_native_signatures([p]).get(key)
        if not item or not item.get('exists') or item.get('error') or item['signature']['size_bytes'] <= 0:
            raise ValueError("native_missing_empty_or_reparse")
        return dict(item['signature'])
    if st.st_size <= 0:
        raise ValueError("empty_file")
    return {"size_bytes": st.st_size, "mtime_ns": st.st_mtime_ns,
            "mtime_utc": datetime.fromtimestamp(st.st_mtime, timezone.utc).isoformat()}


def probe_video(path):
    from video_pipeline.platform.native_media_input import readable_media_input
    with readable_media_input(path) as input_path:
        cp = subprocess.run([
            "ffprobe", "-v", "error", "-show_entries",
            "format=duration:stream=codec_type,width,height,duration:stream_disposition=attached_pic",
            "-of", "json", str(input_path),
        ], capture_output=True, text=True, encoding="utf-8", timeout=30)
    if cp.returncode or cp.stderr.strip():
        raise ValueError("video_probe_failed")
    data = json.loads(cp.stdout)
    streams = [s for s in data.get("streams", []) if s.get("codec_type") == "video"
               and not s.get("disposition", {}).get("attached_pic")]
    if not streams:
        raise ValueError("video_stream_missing")
    stream = max(streams, key=lambda s: int(s.get("width", 0)) * int(s.get("height", 0)))
    duration = float(stream.get("duration") or data.get("format", {}).get("duration") or 0)
    pixels = int(stream.get("width", 0)) * int(stream.get("height", 0))
    if not math.isfinite(duration) or duration <= 0 or pixels <= 0:
        raise ValueError("video_quality_unknown")
    return {"duration": duration, "pixels": pixels}


def within_roots(path, roots, *, native_cache=None):
    p = Path(path)
    if not any(p.absolute().is_relative_to(root) for root in roots):
        return False
    # Also reject directory symlink escapes; exact filesystem spelling is preserved.
    try:
        resolved = p.resolve()
    except OSError:
        if not str(p).startswith('/mnt/'):
            return False
        from video_pipeline.platform.native_file_signatures import native_path_key, read_native_signatures
        key=native_path_key(p)
        item=native_cache.get(key) if native_cache is not None else read_native_signatures([p]).get(key)
        # Native signatures reject reparse ancestors. Do not treat lexical
        # containment alone as permission to follow a redirected native path.
        if not item or not item.get('exists') or item.get('error'):
            return False
        resolved=p.absolute()
    return any(resolved.is_relative_to(root) for root in roots)


def plan_recordings(con, roots, *, probe=probe_video, min_age_seconds=3600,
                    max_groups=50, group_offset=0, now=None):
    if not 1 <= max_groups <= 5000 or min_age_seconds < 0:
        raise ValueError("invalid dedup limits")
    now = time.time() if now is None else now
    root_paths = [Path(windows_to_wsl_path(str(p))).resolve() for p in roots]
    links = defaultdict(list)
    for row in con.execute("""SELECT pp.path_id,b.is_rebroadcast_flag,p.canonical_title
        FROM path_programs pp JOIN broadcasts b ON b.broadcast_id=pp.broadcast_id
        JOIN programs p ON p.program_id=b.program_id"""):
        links[row["path_id"]].append(dict(row))
    tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    holds = {r[0] for r in con.execute("SELECT path_id FROM recording_holds WHERE resolved_at IS NULL")} if "recording_holds" in tables else set()
    states = {r["path_id"]: dict(r) for r in con.execute("SELECT * FROM path_file_state")} if "path_file_state" in tables else {}
    historical = {r[0] for r in con.execute("SELECT DISTINCT path_id FROM file_paths")} if "file_paths" in tables else set()
    groups = defaultdict(list)
    excluded = Counter()
    for row in con.execute("SELECT pm.*,p.path FROM path_metadata pm JOIN paths p USING(path_id) ORDER BY p.path"):
        path = Path(windows_to_wsl_path(row["path"]))
        if path.suffix.lower() not in {".mp4", ".mkv", ".m4v"}:
            continue
        md = reconstruct_path_metadata(row)
        key = episode_key(md)
        if not key:
            excluded["episode_identity_unknown"] += 1
            continue
        if not within_roots(path, root_paths) or "duplicates" in {p.casefold() for p in path.parts}:
            excluded["outside_roots_or_quarantine"] += 1
            continue
        try:
            sig = file_signature(path)
        except (OSError, ValueError):
            excluded["missing_empty_or_symlink"] += 1
            continue
        kind, evidence = broadcast_kind({**md, "path": row["path"]}, links[row["path_id"]])
        candidate = {"path_id": row["path_id"], "path": row["path"], "local_path": str(path),
                     "signature": sig, "broadcast_kind": kind, "broadcast_evidence": evidence,
                     "subtitle_key": text_key(md.get("subtitle")),
                     "metadata_checksum": hashlib.sha256(json.dumps(dict(row), sort_keys=True, ensure_ascii=False).encode()).hexdigest()}
        reason = None
        try:
            confidence = float(md.get("confidence", 0))
        except (ValueError, TypeError):
            confidence = 0
        if row["path_id"] in holds:
            reason = "recording_held"
        elif row["path_id"] in states and not observed_signature_matches(states[row["path_id"]], sig):
            reason = "recording_identity_changed"
        elif row["path_id"] in historical and row["path_id"] not in states:
            reason = "historical_location_reoccupied"
        elif md.get("needs_review") or not math.isfinite(confidence) or confidence < 0.85:
            reason = "metadata_review_required"
        elif now - sig["mtime_ns"] / 1e9 < min_age_seconds:
            reason = "recording_too_recent"
        elif kind == "unknown":
            reason = evidence
        elif any(text_key(b["canonical_title"]) != key[0] for b in links[row["path_id"]]):
            reason = "epg_program_identity_mismatch"
        candidate["defer_reason"] = reason
        groups[key].append(candidate)

    results, moves = [], []
    total = sum(len(v) > 1 for v in groups.values())
    duplicate_groups = [(key, arr) for key, arr in sorted(groups.items()) if len(arr) > 1]
    offset = group_offset % total if total else 0
    duplicate_groups = (duplicate_groups[offset:] + duplicate_groups[:offset])[:max_groups]
    for key, candidates in duplicate_groups:
        group_id = hashlib.sha256(json.dumps(key, ensure_ascii=False).encode()).hexdigest()[:20]
        result = {"group_id": group_id, "episode_key": list(key), "candidates": candidates}
        # Episode number alone must not override conflicting subtitles/editions.
        subtitles = {c["subtitle_key"] for c in candidates if c["subtitle_key"]}
        if len(subtitles) > 1:
            for c in candidates:
                c["defer_reason"] = "conflicting_episode_subtitles"
        for kind in ("original", "repeat", "unknown"):
            cohort = [c for c in candidates if c["broadcast_kind"] == kind]
            if not cohort:
                continue
            # Do not discard any member of an uncertain cohort.
            if any(c["defer_reason"] for c in cohort):
                for c in cohort:
                    c.update(decision="defer", reason=c["defer_reason"] or "cohort_has_uncertain_recording")
                continue
            if len(cohort) == 1:
                cohort[0].update(decision="keep", reason="only_recording_in_broadcast_kind")
                continue
            try:
                for c in cohort:
                    c["quality"] = probe(c["local_path"])
                    if file_signature(c["local_path"]) != c["signature"]:
                        raise ValueError("recording_changed_during_probe")
                    q = c["quality"]
                    if not math.isfinite(q["duration"]) or q["duration"] <= 0 or q["pixels"] <= 0:
                        raise ValueError("video_quality_unknown")
                durations = [c["quality"]["duration"] for c in cohort]
                # A material length difference could be a cut/extended edition or
                # incomplete recording. ffprobe is not a full corruption test.
                if max(durations) - min(durations) > max(30, max(durations) * 0.02):
                    raise ValueError("duration_or_edition_mismatch")
            except (OSError, ValueError, KeyError, subprocess.TimeoutExpired) as exc:
                for c in cohort:
                    c.update(decision="defer", reason=str(exc))
                continue
            keep = min(cohort, key=lambda c: (-c["quality"]["pixels"], -c["signature"]["size_bytes"], c["path"]))
            keep.update(decision="keep", reason="best_quality_in_broadcast_kind")
            for c in cohort:
                if c is keep:
                    continue
                c.update(decision="quarantine", reason="duplicate_in_same_broadcast_kind", keep_path=keep["path"])
                # Per-volume quarantine avoids copying multi-GB videos across disks.
                local_dst = quarantine_root(Path(c["local_path"]), root_paths) / group_id / kind / c["path_id"] / Path(c["local_path"]).name
                moves.append({"op": "move", "path_id": c["path_id"], "src": c["path"],
                              "dst": wsl_to_windows_path(str(local_dst)), "precondition": c["signature"],
                              "keep": {"path": keep["path"], **keep["signature"]},
                              "keep_metadata_checksum": keep["metadata_checksum"],
                              "group_id": group_id, "broadcast_kind": kind,
                              "metadata_checksum": c["metadata_checksum"]})
        results.append(result)
    decisions = Counter(c["decision"] for g in results for c in g["candidates"])
    reasons = Counter(c["reason"] for g in results for c in g["candidates"] if c["decision"] == "defer")
    return {"policy": "preserve_original_and_repeat", "groups_total": total, "groups_checked": len(results),
            "next_group_offset": (offset + len(results)) % total if total else 0,
            "groups_remaining": max(0, total-len(results)), "decisions": dict(decisions),
            "defer_reasons": dict(reasons), "excluded": dict(excluded), "groups": results, "moves": moves}


def quarantine_root(path, roots):
    # Outside EVERY media root, otherwise the next source/relocate scan could
    # ingest quarantined recordings back into the library. Same-volume move.
    # Native apply checks every ancestor for reparse points. Lexical matching is
    # required for NTFS names inaccessible to DrvFS; Python planning validates the
    # observed source separately rather than pretending DrvFS absence is deletion.
    resolved_source = path.absolute() if str(path).startswith('/mnt/') else path.resolve()
    matching = [root for root in roots if resolved_source.is_relative_to(root)]
    if not matching:
        raise ValueError("quarantine source outside roots")
    if len(path.parts) >= 3 and path.parts[1] == "mnt" and re.fullmatch(r"[a-z]", path.parts[2]):
        base = Path("/mnt") / path.parts[2] / "_AI_WORK"
    else:
        base = min(matching, key=lambda root: len(root.parts)).parent
    quarantine = base / "duplicates" / "quarantine_v3"
    # Whole-volume scans explicitly exclude _AI_WORK in every supported scanner.
    # A drive-root search is not an ingest root for that reserved subtree.
    volume_root = Path("/mnt") / path.parts[2] if len(path.parts) >= 3 and path.parts[1] == "mnt" and re.fullmatch(r"[a-z]", path.parts[2]) else None
    resolved_quarantine = quarantine.resolve()
    if any(resolved_quarantine.is_relative_to(root) and root != volume_root for root in roots):
        raise ValueError("quarantine would be inside a configured media root")
    return quarantine


def observed_signature_matches(state, sig):
    try:
        stamp = re.sub(r"(\.\d{6})\d+(?=[+-]|$)", r"\1", state["mtime_utc"].replace("Z", "+00:00"))
        timestamp = datetime.fromisoformat(stamp).timestamp()
        return state["size_bytes"] == sig["size_bytes"] and abs(timestamp - sig["mtime_ns"] / 1e9) <= 0.005
    except (ValueError, TypeError, KeyError, AttributeError):
        return False
