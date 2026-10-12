"""Explicit date-first cleanup of a reviewed physical discovery snapshot.

Never used implicitly by scheduled dedup. Original-period and unknown recordings
remain protected; cached media measurements are usable only with fresh signatures.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta
import csv
import hashlib
import json
import math
import ntpath
from pathlib import Path
import re
import time
import unicodedata
import uuid

from video_pipeline.domain.recording_dedup import file_signature, observed_signature_matches, quarantine_root, within_roots
from video_pipeline.domain.reviewed_content_coverage import load_coverage_reviews, match_coverage_review
from video_pipeline.platform.pathscan_common import path_id_for, windows_to_wsl_path, wsl_to_windows_path

RECORDING_STAMP_PATTERN = r"((?:19|20)\d{2})[-_ ]?(0[1-9]|1[0-2])[-_ ]?(0[1-9]|[12]\d|3[01])[-_ ]?([01]\d|2\d)[：:_ ]?([0-5]\d)(?:[-_ ]?([0-5]\d))?(?:(?:[-_ ]?\(\d+\))|(?:[-_]\d+))*\.[^.]+$"


def title_key(value):
    return re.sub(r"[\s_]+", "", unicodedata.normalize("NFKC", str(value or ""))).casefold().strip("「」『』")


def metadata_title_key(value):
    # Broadcast-slot decorations are not seasons or editions. Remove only the
    # explicitly known leading slot labels, never arbitrary parenthetical text.
    text = unicodedata.normalize('NFKC',str(value or ''))
    text = re.sub(r'^(?:<(?:深夜一挙放送|新春一挙放送|一挙放送|集中放送)>)[\s_]*','',text)
    return title_key(text)


def episode_subtitle(value, episodes):
    """A bare, matching episode number is not an independently named subtitle."""
    text = unicodedata.normalize('NFKC', str(value or '')).strip()
    marker = re.fullmatch(r'(?:#\s*(\d+)|第\s*(\d+)\s*話)', text)
    if marker and len(episodes) == 1 and int(next(v for v in marker.groups() if v is not None)) == episodes[0]:
        return ''
    return value or ''


def filename_episode_scope_conflict(name, episodes):
    """Do not treat an omnibus label as proof of one declared episode.

    Sanitized names can lose the separator in e.g. #25-30, leaving #2530#25.
    Detect the disagreement, but never guess the lost range or rewrite metadata.
    """
    if len(episodes) != 1:
        return False
    text = unicodedata.normalize('NFKC', str(name))
    if '一挙' not in text:
        return False
    marker = re.search(r'#\s*(\d+)(?:\s*([-~〜～])\s*(\d+))?', text)
    return bool(marker and (marker.group(2) or int(marker.group(1)) != episodes[0]))


def filename_single_episode_subtitle(name, episodes):
    """Read a literal label after this exact episode marker, not a synopsis.

    This repairs absent discovery labels, not named contradictions. Dates, extra
    episode markers and ambiguous free-form broadcast descriptions are excluded.
    """
    if len(episodes) != 1 or not recording_stamp(name):
        return ''
    text = unicodedata.normalize('NFKC', str(name))
    date = re.search(RECORDING_STAMP_PATTERN, text)
    prefix = text[:date.start()].rstrip(' _-')
    pattern = r'(?:#\s*(\d+)(?!\d)|第\s*(\d+)\s*話)'
    marker = re.search(pattern, prefix)
    def same_episode(match):
        return int(next(g for g in match.groups() if g is not None)) == episodes[0]
    if not marker or not same_episode(marker):
        return ''
    tail = prefix[marker.end():].strip()
    repeated = re.match(pattern, tail)
    if repeated:
        if not same_episode(repeated):
            return ''
        tail = tail[repeated.end():].strip()
    if re.search(pattern, tail):
        return ''
    quote = re.match(r'^「([^「」]{1,120})」', tail)
    if quote:
        if any(c in tail[quote.end():] for c in '「」'):
            return ''
        return quote.group(1).strip()
    if (not tail or len(tail) > 80 or any(c in tail for c in '「」▽()【】\r\n')
            or re.search(r'出演|放送|全\d+話|ほか|(?:19|20)\d{2}年', tail)):
        return ''
    return tail


def path_key(value):
    return ntpath.normpath(str(value).replace("/", "\\")).casefold()


def recording_stamp(name):
    m = re.search(RECORDING_STAMP_PATTERN, unicodedata.normalize("NFKC", name))
    if not m:
        return None
    try:
        y, month, d, h, minute = map(int, m.groups()[:5])
        return datetime(y, month, d) + timedelta(hours=h, minutes=minute)
    except ValueError:
        return None


def snapshot_cache(con):
    locations = {r["path_id"]:dict(r) for r in con.execute("SELECT * FROM paths")}
    metadata = {r["path_id"]:dict(r) for r in con.execute("SELECT * FROM path_metadata")}
    states = {r["path_id"]:dict(r) for r in con.execute("SELECT * FROM path_file_state")}
    historical = {r[0] for r in con.execute("SELECT DISTINCT path_id FROM file_paths")}
    return {r["path"].casefold():{"location":r,"metadata":metadata.get(pid),"state":states.get(pid),"historical":pid in historical} for pid,r in locations.items()}


def location_snapshot(con, path, cache=None):
    if cache is not None:
        return cache.get(path.casefold(),{"location":None,"metadata":None,"state":None,"historical":False})
    row = con.execute("SELECT * FROM paths WHERE path=? COLLATE NOCASE", (path,)).fetchone()
    if not row:
        return {"location": None, "metadata": None, "state": None, "historical": False}
    pid = row["path_id"]
    md = con.execute("SELECT * FROM path_metadata WHERE path_id=?", (pid,)).fetchone()
    state = con.execute("SELECT * FROM path_file_state WHERE path_id=?", (pid,)).fetchone()
    historical = bool(con.execute("SELECT 1 FROM file_paths WHERE path_id=?", (pid,)).fetchone())
    return {"location": dict(row), "metadata": dict(md) if md else None,
            "state": dict(state) if state else None, "historical": historical}


def predicted_location_id(con, path):
    row = con.execute("SELECT path_id FROM paths WHERE path=? COLLATE NOCASE", (path,)).fetchone()
    if row:
        return row[0]
    pid = path_id_for(path)
    if con.execute("SELECT 1 FROM paths WHERE path_id=?", (pid,)).fetchone():
        pid = str(uuid.uuid5(uuid.NAMESPACE_URL, "video-pipeline:location:v2:" + path.replace("/", "\\").lower()))
    return pid


def validate_candidate(con, item, roots, cache=None, native_cache=None):
    local = Path(windows_to_wsl_path(item["path"]))
    try:
        in_roots = within_roots(local, roots,native_cache=native_cache) and local.resolve() == local.absolute()
    except OSError:
        in_roots = any(local.absolute().is_relative_to(root) for root in roots)
    if not in_roots:
        raise ValueError("recording escaped roots or reparse/symlink path")
    if file_signature(local,native_cache=native_cache) != item["signature"]:
        raise ValueError("recording changed: " + item["path"])
    current = location_snapshot(con, item["path"], cache)
    if current != item["db_snapshot"]:
        raise ValueError("recording DB association changed: " + item["path"])
    if current["location"] and con.execute("SELECT 1 FROM recording_holds WHERE path_id=? AND resolved_at IS NULL", (current["location"]["path_id"],)).fetchone():
        raise ValueError("recording held")
    if current["metadata"] and current["metadata"]["needs_review"]:
        raise ValueError("metadata review required")
    if current["state"] and not observed_signature_matches(current["state"], item["signature"]):
        raise ValueError("recording instance changed")
    if current["historical"] and not current["state"]:
        raise ValueError("historical location reoccupied")


def plan_by_date(con, roots, evidence_dir, *, now=None):
    evidence = Path(evidence_dir)
    names = ("candidate-index.json", "merged-video-probes.json", "broadcast-date-sources.json", "dated-recordings.csv", "batch-duration-anomalies.json")
    checksums = {n: hashlib.sha256((evidence/n).read_bytes()).hexdigest() for n in names}
    coverage_reviews = load_coverage_reviews(evidence, checksums)
    index = json.loads((evidence/names[0]).read_text())
    probes = {q["id"]: q for q in json.loads((evidence/names[1]).read_text())}
    sources = json.loads((evidence/names[2]).read_text())
    periods = {}
    for p in sources["periods"]:
        if not p.get("url", "").startswith("https://"):
            raise ValueError("unsourced broadcast period")
        start, end = datetime.fromisoformat(p["start"]), datetime.fromisoformat(p["end"])
        if start > end:
            raise ValueError("invalid broadcast period")
        periods[title_key(p["title"])] = p
    dated = {path_key(r["実パス"]): r for r in csv.DictReader((evidence/names[3]).open(encoding="utf-8-sig"))}
    anomalies = {q["id"] for q in json.loads((evidence/names[4]).read_text())}
    ids = {i for g in index["same_episode_set_groups"] for i in g["file_ids"]} | {i for g in index["overlap_title_groups"] for i in g["file_ids"]}
    files = {f["id"]: f for f in index["files"]}
    root_paths = [Path(windows_to_wsl_path(str(r))).resolve() for r in roots]
    # The saved physical population includes independent TV/university consumers.
    for r in index["scope"]["roots"]:
        root = Path(windows_to_wsl_path(r["root"])).resolve()
        if not r["available"] or not root.is_dir():
            raise ValueError("discovery root unavailable: " + str(root))
        if root not in root_paths:
            root_paths.append(root)
    cache = snapshot_cache(con)
    registered = {path_key(s["location"]["path"]):s["location"]["path"] for s in cache.values()}
    # Batch inaccessible native paths once, not one PowerShell process per file.
    native_paths = []
    for i in sorted(ids):
        local = Path(windows_to_wsl_path(files[i]["path"]))
        if str(local).startswith('/mnt/'):
            try:
                local.stat()
            except OSError:
                native_paths.append(local)
    from video_pipeline.platform.native_file_signatures import read_native_signatures
    native_cache = read_native_signatures(native_paths) if native_paths else {}
    now = time.time() if now is None else now
    held, unavailable, good, originals = [], [], [], []
    for i in sorted(ids):
        f = files[i]
        local = Path(windows_to_wsl_path(f["path"]))
        path = registered.get(path_key(f["path"]), wsl_to_windows_path(str(local)))
        item = {"discovery_id": i, "path": path, "title": f["title"], "episodes": f["episodes"],
                "subtitle": episode_subtitle(f.get("subtitle", ""), f["episodes"]),
                "discovery_subtitle": f.get("subtitle", ""), "quality": probes.get(i, {}), "signature": None,
                "db_snapshot": location_snapshot(con, path, cache)}
        literal = filename_single_episode_subtitle(f['name'], f['episodes'])
        if literal:
            item['filename_subtitle_literal'] = literal
        if not item['subtitle']:
            if literal:
                item['subtitle'] = literal
                item['filename_subtitle_evidence'] = {
                    'filename': f['name'], 'episodes': list(f['episodes']),
                    'literal_subtitle': literal, 'discovery_subtitle': f.get('subtitle', ''),
                    'method': 'exact_episode_marker_and_terminal_recording_date',
                }
        stamp = recording_stamp(f["name"])
        period = periods.get(title_key(f["title"]))
        row = dated.get(path_key(f["path"]))
        try:
            item["signature"] = file_signature(local,native_cache=native_cache)
        except (OSError, ValueError):
            item["reason"] = "snapshot_location_no_longer_present"
            unavailable.append(item)
            continue
        reason = None
        # Windows observations emit .NET's seven fractional digits; Python 3.10
        # accepts six. Normalize presentation only, leaving the freshness guard intact.
        old_stamp = re.sub(r"(\.\d{6})\d+(?=[+-]|$)", r"\1", f["mtimeUtc"].replace("Z", "+00:00"))
        old = datetime.fromisoformat(old_stamp).timestamp()
        q = item["quality"]
        if ntpath.basename(f['path']) != f['name']:
            reason = 'filename_snapshot_conflict'
        elif item["signature"]["size_bytes"] != f["size"] or abs(item["signature"]["mtime_ns"]/1e9-old) > .005:
            reason = "discovery_or_cached_probe_stale"
        elif now-item["signature"]["mtime_ns"]/1e9 < 3600:
            reason = "recording_too_recent"
        elif not stamp or not period or not row:
            reason = "original_broadcast_period_or_recording_date_unknown"
        elif stamp.strftime("%Y-%m-%d %H:%M") != row["録画日時_JST(ファイル名末尾)"]:
            reason = "date_evidence_conflict"
        elif stamp.date().isoformat() <= period["end"]:
            item["reason"] = "protected_original_period_or_preperiod"
            originals.append(item)
            continue
        elif row["放送日による区分"] != "初回放送終了後":
            reason = "date_classification_conflict"
        elif not f["episodes"] or any(type(e) is not int or e < 1 for e in f["episodes"]):
            reason = "episode_identity_unknown"
        elif filename_episode_scope_conflict(f['name'], f['episodes']):
            reason = "filename_omnibus_episode_scope_conflict"
        elif i in anomalies:
            reason = "omnibus_duration_anomaly"
        elif not q.get("ok") or any(not math.isfinite(float(q.get(k, 0))) or float(q.get(k, 0)) <= 0 for k in ("duration", "width", "height")):
            reason = "cached_video_information_unreadable"
        elif q["duration"]/len(f["episodes"]) < 600:
            reason = "short_fragment_or_episode_count_conflict"
        elif literal and episode_subtitle(f.get('subtitle', ''), f['episodes']) and title_key(literal) != title_key(item['subtitle']):
            reason = 'filename_named_subtitle_conflict'
        if not reason:
            try:
                validate_candidate(con, item, root_paths, cache, native_cache)
                md = item["db_snapshot"]["metadata"]
                if md and md["program_title"] and metadata_title_key(md["program_title"]) != title_key(f["title"]):
                    aliases={metadata_title_key(t) for t in period.get('metadata_title_aliases',[])}
                    # An explicitly reviewed base-title alias can be narrower in
                    # the filename (e.g. S1/S2, 4K edition). Require that exact
                    # season/edition label in the filename and group by the full
                    # filename title; never collapse it to the coarse DB title.
                    if metadata_title_key(md['program_title']) not in aliases or title_key(f['title']) not in title_key(f['name']):
                        raise ValueError("metadata_program_identity_conflict")
                if md and md["episode_no"] and str(md["episode_no"]).isdigit() and f["episodes"] != [int(md["episode_no"])]:
                    raise ValueError("metadata_episode_identity_conflict")
            except (OSError, ValueError) as exc:
                reason = str(exc)
        if reason:
            item["reason"] = reason
            held.append(item)
        else:
            item["broadcast_period"] = period
            # Do not merge remastered, dubbed, theatrical or recut editions.
            item["edition"] = sorted(set(re.findall(r"4K|リマスター|ノーカット|吹替|字幕版|再編集|劇場版|特別編", unicodedata.normalize("NFKC", f["name"]), flags=re.I)))
            good.append(item)
    # Missing labels are repaired only when another fully validated same-work,
    # same-episode recording's own filename independently contains that literal.
    # Never infer a missing label merely from a peer's metadata or a substring.
    literal_peers = defaultdict(list)
    for item in good:
        literal = item.get('filename_subtitle_literal')
        if literal:
            key = (title_key(item['title']), tuple(item['edition']), tuple(item['episodes']), title_key(literal))
            literal_peers[key].append(item['path'])
    for item in good:
        proof = item.get('filename_subtitle_evidence')
        if not proof:
            continue
        key = (title_key(item['title']), tuple(item['edition']), tuple(item['episodes']), title_key(item['subtitle']))
        peers = [p for p in literal_peers[key] if path_key(p) != path_key(item['path'])]
        if peers:
            proof['independent_filename_peer_paths'] = peers
        else:
            item['subtitle'] = episode_subtitle(item['discovery_subtitle'], item['episodes'])
            item['unconfirmed_filename_subtitle_candidate'] = item.pop('filename_subtitle_evidence')
    singles = defaultdict(list)
    batches = defaultdict(list)
    for item in good:
        group = (title_key(item["title"]), tuple(item["edition"]), tuple(item["episodes"]))
        (singles if len(item["episodes"]) == 1 else batches)[group].append(item)
    keepers, drops, unresolved_groups = [], [], []
    consumed_reviews = set()
    single_keepers = defaultdict(list)
    def rank(item):
        return (-item["quality"]["width"]*item["quality"]["height"], -item["signature"]["size_bytes"], item["path"])
    def equal_sets(cohort, group):
        # Conflicting named subtitles/lengths form separate preservation cohorts.
        partitions = defaultdict(list)
        for item in cohort:
            partitions[title_key(item["subtitle"])].append(item)
        retained = []
        for partition in partitions.values():
            clusters = []
            for item in sorted(partition, key=lambda c: c["quality"]["duration"]):
                cluster = next((c for c in clusters if item["quality"]["duration"]-c[0]["quality"]["duration"] <= max(30,item["quality"]["duration"]*.02)), None)
                if cluster is None:
                    clusters.append([item])
                else:
                    cluster.append(item)
            for cluster in clusters:
                keeper = sorted(cluster, key=rank)[0]
                retained.append(keeper)
                keepers.append(keeper)
                for drop in cluster:
                    if drop is not keeper:
                        drops.append((drop, [keeper], "same_episode_set_subtitle_edition_and_later_airing"))
        # Duration discrepancies stay protected unless a concrete full-content
        # maintenance review names both unchanged recordings. This is one-way
        # containment: an incomplete source may be covered by a complete keeper.
        retained_by_path = {item['path'].casefold(): item for item in retained}
        reviewed_drops = {}
        for number, review in enumerate(coverage_reviews):
            source = retained_by_path.get(review['source']['path'].casefold())
            keeper = retained_by_path.get(review['keeper']['path'].casefold())
            if source is None or keeper is None:
                continue
            match_coverage_review(review, source, keeper, title_key)
            reviewed_drops[source['path']] = (source, keeper, review)
            consumed_reviews.add(number)
        if reviewed_drops:
            if {k['path'] for _, k, _ in reviewed_drops.values()} & reviewed_drops.keys():
                raise ValueError('content coverage keeper is also a drop')
            # Existing equal-duration drops must follow the ultimate keeper too.
            drops[:] = [(d, [reviewed_drops[k['path']][1] if k['path'] in reviewed_drops else k for k in ks], reason)
                        for d, ks, reason in drops]
            for source, keeper, review in reviewed_drops.values():
                source['content_coverage_review'] = review
                drops.append((source, [keeper], 'explicitly_reviewed_program_content_covered_by_complete_keeper'))
            retained = [item for item in retained if item['path'] not in reviewed_drops]
            keepers[:] = [item for item in keepers if item['path'] not in reviewed_drops]
        if len(retained) > 1:
            reasons = []
            if len(partitions) > 1:
                reasons.append("subtitle_conflict_or_missing")
            for partition in partitions.values():
                durations = [item["quality"]["duration"] for item in partition]
                if max(durations) - min(durations) > max(30, max(durations) * .02):
                    reasons.append("duration_or_edition_mismatch")
                    break
            unresolved_groups.append({
                "title": retained[0]["title"], "episodes": list(group[2]),
                "edition": list(group[1]), "broadcast_kind": "repeat_by_sourced_date",
                "reasons": reasons, "content_equivalence": "not_verified",
                "candidates": [{"path": item["path"], "subtitle": item["subtitle"],
                                "duration_seconds": item["quality"]["duration"],
                                "signature": item["signature"]} for item in retained],
            })
        return retained
    for group, cohort in singles.items():
        single_keepers[group].extend(equal_sets(cohort, group))
    for group, cohort in batches.items():
        uncovered = []
        for batch in cohort:
            coverage = [single_keepers.get((group[0],group[1],(ep,)), []) for ep in batch["episodes"]]
            if any(len(arr) != 1 for arr in coverage):
                uncovered.append(batch)
                continue
            ks = [arr[0] for arr in coverage]
            seconds = sum(k["quality"]["duration"] for k in ks)
            pixels = batch["quality"]["width"]*batch["quality"]["height"]
            if abs(batch["quality"]["duration"]-seconds) > max(90,seconds*.03) or any(k["quality"]["width"]*k["quality"]["height"] < pixels for k in ks):
                uncovered.append(batch)
                continue
            drops.append((batch,ks,"omnibus_all_named_episodes_retained_in_same_later_airing_class"))
        if uncovered:
            equal_sets(uncovered,group)
    if len(consumed_reviews) != len(coverage_reviews):
        raise ValueError('content coverage review not applicable to current protected/held candidates')
    moves = []
    for drop, ks, reason in drops:
        group_id = hashlib.sha256(json.dumps([title_key(drop["title"]),drop["episodes"],drop["edition"]],ensure_ascii=False).encode()).hexdigest()[:20]
        pid = predicted_location_id(con,drop["path"])
        local = Path(windows_to_wsl_path(drop["path"]))
        dest = quarantine_root(local,root_paths)/group_id/"repeat_by_sourced_date"/pid/local.name
        if dest.exists() or dest.resolve() != dest.absolute():
            raise ValueError("occupied or redirected quarantine destination")
        moves.append({"op":"move","src":drop["path"],"dst":wsl_to_windows_path(str(dest)),"path_id":pid,
                      "precondition":drop["signature"],"keep":{"path":ks[0]["path"],**ks[0]["signature"]},
                      "keeps":[{"path":k["path"],**k["signature"]} for k in ks],"group_id":group_id,
                      "broadcast_kind":"repeat_by_sourced_date","reason":reason,"discovery_id":drop["discovery_id"]})
    drop_paths = {m["src"] for m in moves}
    if len(drop_paths) != len(moves) or drop_paths & {k["path"] for k in keepers}:
        raise ValueError("global keep/drop overlap")
    review_paths = {c["path"] for g in unresolved_groups for c in g["candidates"]}
    return {"policy":"explicit_date_first_preserve_original_and_later_airings","roots":list(map(str,root_paths)),
            "evidence_directory":str(evidence.resolve()),"evidence_checksums":checksums,"candidates_total":len(ids),
            "keepers":keepers,"drops":[d for d,_,_ in drops],"holds":held,"protected_originals":originals,
            "unavailable_snapshot_locations":unavailable,"moves":moves,
            "unresolved_same_episode_groups":unresolved_groups,
            "unresolved_same_episode_files":len(review_paths),
            "candidate_review_complete":not (unresolved_groups or held or unavailable),
            "cleanup_complete":not (moves or unresolved_groups or held or unavailable),
            "decisions":{"keep":len(keepers)-len(review_paths),"review":len(review_paths),"quarantine":len(moves),"hold":len(held),"protect_original":len(originals),"unavailable":len(unavailable)},
            "defer_reasons":dict(Counter(i["reason"] for i in held)),"video_content_reads":0}
