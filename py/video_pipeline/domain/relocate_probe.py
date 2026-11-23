"""Bounded physical observations for a Windows relocate preview; no apply cache."""
from datetime import datetime, timezone
from pathlib import PureWindowsPath
import re

from video_pipeline.domain.companion_bundle import BUNDLE_VERSION, VIDEO_EXTENSIONS, _signature_ticks, companion_paths
from video_pipeline.domain.move_preflight import _mtime_timestamp
from video_pipeline.platform.windows_probe_batch import windows_entry_reader

BATCH_SIZE = 256


def _requirements(video):
    path = PureWindowsPath(video)
    siblings = [str(path.with_suffix(ext)) for ext in sorted(VIDEO_EXTENSIONS)
                if path.suffix.lower() != ext]
    return [str(parent) for parent in path.parents], siblings, companion_paths(video)


def _state(entries, path):
    kind = entries[path]['kind']
    if kind == 'reparse':
        raise ValueError('reparse_point')
    if kind not in ('file', 'directory', 'missing'):
        raise ValueError('unverified_windows_entry')
    return kind


def _signature(entry):
    size, ticks = entry.get('size'), entry.get('ticks')
    if type(size) is not int or size < 0 or type(ticks) is not int or ticks < 0:
        raise ValueError('invalid_native_companion_signature')
    # Windows FILETIME is a count of 100ns ticks since 1601; never round-trip
    # through floating point when writing the eventual apply precondition.
    seconds, fraction = divmod(ticks - 116444736000000000, 10_000_000)
    time = datetime.fromtimestamp(seconds, timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    return {'size_bytes': size, 'mtime_utc': f'{time}.{fraction:07d}+00:00',
            'mtime_ns': seconds * 1_000_000_000 + fraction * 100}


def _bundle(row, entries, requirements):
    src, dst, size, mtime = row
    sp, ss, sc = requirements[src]
    dp, ds, dc = requirements[dst]
    if any(_state(entries, p) != 'directory' for p in sp):
        raise ValueError('source_parent_unavailable')
    # The destination folder need not exist yet, but its drive must be online.
    if _state(entries, dp[-1]) != 'directory' or any(_state(entries, p) == 'file' for p in dp):
        raise ValueError('destination_parent_unavailable')
    if _state(entries, src) != 'file':
        raise ValueError('source_missing')
    sig = _signature(entries[src])
    if type(size) is not int or size != sig['size_bytes'] or not isinstance(mtime, str) or not mtime:
        raise ValueError('source_changed')
    # Native enumeration emits .NET round-trip UTC: a single changed tick is
    # observable. Retain legacy tolerance only for coarser scan timestamps.
    if re.fullmatch(r'.*\.\d{7}(?:Z|[+-]\d\d:\d\d)', mtime):
        changed = sig['mtime_ns'] // 100 != _signature_ticks({'mtime_utc': mtime})
    else:
        changed = abs(_mtime_timestamp(sig['mtime_utc']) - _mtime_timestamp(mtime)) > 0.005
    if changed:
        raise ValueError('source_changed')
    if any(_state(entries, p) != 'missing' for p in [dst, *dc.values()]):
        raise ValueError('destination_exists')
    if any(_state(entries, p) != 'missing' for p in ss + ds):
        raise ValueError('ambiguous_companion_stem')
    members = [dict(kind='video', src=src, dst=dst, precondition=sig)]
    cohort = []
    for kind, p in sc.items():
        state = _state(entries, p)
        if state == 'missing':
            continue
        if state != 'file':
            raise ValueError('non_regular_bundle_member')
        members.append(dict(kind=kind, src=p, dst=dc[kind], precondition=_signature(entries[p])))
        cohort.append(kind)
    return dict(bundle_version=BUNDLE_VERSION, members=members, companion_cohort=cohort)


def inspect_windows_moves(rows, *, reader=None):
    """Yield one (bundle, refusal) per exact (src, dst, scanned size, mtime).

    Only share observations within one chunk. Apply still obtains fresh proof.
    A damaged response aborts planning; a per-entry IO error holds every row
    that needs the entry without silently excluding it from its closed cohort.
    """
    if not rows:
        return
    reader = reader or windows_entry_reader()
    for i in range(0, len(rows), BATCH_SIZE):
        chunk = rows[i:i + BATCH_SIZE]
        requirements = {}
        paths = {}
        for src, dst, _, _ in chunk:
            for video in (src, dst):
                if video not in requirements:
                    parents, siblings, companions = requirements[video] = _requirements(video)
                    paths.update(dict.fromkeys([video, *parents, *siblings, *companions.values()]))
        entries = reader(list(paths))
        if set(entries) != paths.keys():
            raise ValueError('incomplete_windows_probe_batch')
        for row in chunk:
            try:
                bundle = _bundle(row, entries, requirements)
            except (ValueError, OverflowError, OSError) as exc:
                yield None, str(exc)
            else:
                yield bundle, None
