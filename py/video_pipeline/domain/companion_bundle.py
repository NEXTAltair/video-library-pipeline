"""Capture a closed, versioned recording cohort before any member is moved."""
from datetime import datetime, timezone
import errno
from pathlib import Path, PureWindowsPath
import stat

from video_pipeline.platform.pathscan_common import windows_to_wsl_path

BUNDLE_VERSION = 1
KINDS = ('psc', 'vtt', 'program_txt', 'recording_json')
VIDEO_EXTENSIONS = {'.ts', '.m2ts', '.mts', '.mp4', '.mkv', '.m4v', '.avi', '.mov', '.mpeg', '.mpg', '.m2t', '.webm'}


def companion_paths(video):
    p = Path(video) if str(video).startswith("/") else PureWindowsPath(video)
    return dict(zip(KINDS, (str(p.with_suffix('.psc')), str(p.with_suffix('.vtt')),
                            str(p) + '.program.txt', str(p) + '.recording.json')))


def parent_move_succeeded(row):
    """Old video-only receipts remain valid; versioned bundles need completion."""
    return row.get('ok') is True and ('bundle_version' not in row or row.get('bundle_complete') is True)


def _check_ancestors(path):
    for p in (path, *path.parents):
        try:
            s = p.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(s.st_mode) or getattr(s, 'st_file_attributes', 0) & 0x400:
            raise ValueError('reparse_point')


def _signature(path):
    s = path.lstat()
    if not stat.S_ISREG(s.st_mode) or getattr(s, 'st_file_attributes', 0) & 0x400:
        raise ValueError('non_regular_bundle_member')
    seconds, ns = divmod(s.st_mtime_ns, 1_000_000_000)
    # NTFS timestamps are 100ns ticks. Preserve the full local observation too.
    utc = datetime.fromtimestamp(seconds, timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    return {'size_bytes': s.st_size, 'mtime_utc': f'{utc}.{ns // 100:07d}+00:00',
            'mtime_ns': s.st_mtime_ns}


def capture_bundle(src, dst, *, localize=windows_to_wsl_path, windows_probe=None, allow_destination_exists=False):
    """Return fields to merge into a move. Empty cohort is explicit evidence.

    Native fallback MUST return these same fields and affirm bundle_verified;
    an old video-only probe is insufficient to establish absent companions.
    """
    try:
        source, dest = Path(localize(src)), Path(localize(dst))
        _check_ancestors(source)
        _check_ancestors(dest)
        # Replace-extension companions cannot be assigned to either recording
        # when another video with the same stem exists (even if cohort is empty).
        # Probe the bounded known container extensions, not every entry in the
        # directory for every video (a large flat library would become O(n^2)).
        for video in ((source,) if allow_destination_exists else (source, dest)):
            for extension in VIDEO_EXTENSIONS:
                sibling = video.with_suffix(extension)
                if sibling.name.casefold() != video.name.casefold() and (sibling.exists() or sibling.is_symlink()):
                    raise ValueError('ambiguous_companion_stem')
        members = [{'kind': 'video', 'src': src, 'dst': dst, 'precondition': _signature(source)}]
        cohort = []
        destinations = companion_paths(dst)
        for kind, path in companion_paths(src).items():
            p = Path(localize(path))
            target = Path(localize(destinations[kind]))
            _check_ancestors(p)
            _check_ancestors(target)
            # Detect occupied sidecar destinations, including absent source members.
            if not allow_destination_exists and (target.exists() or target.is_symlink()):
                raise ValueError('destination_exists')
            if p.exists() or p.is_symlink():
                members.append({'kind': kind, 'src': path, 'dst': destinations[kind],
                                'precondition': _signature(p)})
                cohort.append(kind)
        if not allow_destination_exists and (dest.exists() or dest.is_symlink()):
            raise ValueError('destination_exists')
        return {'bundle_version': BUNDLE_VERSION, 'members': members, 'companion_cohort': cohort}
    except OSError as exc:
        if exc.errno not in (errno.ENAMETOOLONG, errno.EIO):
            raise
        if windows_probe is None:
            from video_pipeline.platform.native_companion_probe import read_native_bundle
            windows_probe = read_native_bundle
        result = windows_probe(src, dst)
        collision_allowed = allow_destination_exists and result.get('bundle_error') == 'destination_exists'
        if result.get('ok') is not True or (result.get('bundle_verified') is not True and not collision_allowed):
            raise ValueError(result.get('bundle_error') or 'native_companion_capture_unavailable') from exc
        fields = {k: result[k] for k in ('bundle_version', 'members', 'companion_cohort')}
        if fields['bundle_version'] != BUNDLE_VERSION or not isinstance(fields['members'], list) or not fields['members']:
            raise ValueError('invalid_native_companion_capture')
        expected = {'video': (src, dst), **{k: (v, companion_paths(dst)[k]) for k, v in companion_paths(src).items()}}
        kinds = []
        for member in fields['members']:
            k = member['kind']
            if k not in expected or k in kinds or (member['src'], member['dst']) != expected[k]:
                raise ValueError('invalid_native_companion_capture')
            sig = member['precondition']
            if type(sig.get('size_bytes')) is not int or not sig.get('mtime_utc'):
                raise ValueError('invalid_native_companion_signature')
            kinds.append(k)
        if kinds[0] != 'video' or kinds[1:] != fields['companion_cohort'] or any(k not in KINDS for k in kinds[1:]):
            raise ValueError('invalid_native_companion_cohort')
        return fields


def _signature_ticks(signature):
    import re
    if signature.get('mtime_ns') is not None:
        return int(signature['mtime_ns']) // 100
    value = signature['mtime_utc'].replace('Z', '+00:00')
    dt = datetime.fromisoformat(value)
    fraction = re.search(r'\.(\d+)', value)
    ticks = int(((fraction.group(1) if fraction else '') + '0000000')[:7])
    return int(dt.replace(microsecond=0).timestamp()) * 10_000_000 + ticks


def validate_restore_bundle(plan, receipt, current):
    """A restore must preserve the quarantined cohort, not bless a new one."""
    expected = plan.get('members') or [{'kind': 'video', 'src': plan['src'], 'dst': plan['dst'],
                                        'precondition': plan['precondition']}]
    kinds = [member['kind'] for member in expected]
    if len(set(kinds)) != len(kinds) or kinds[0] != 'video':
        raise ValueError('invalid recorded quarantine bundle')
    if 'bundle_version' not in receipt:
        if kinds != ['video'] or current['companion_cohort']:
            raise ValueError('unversioned quarantine cannot prove companion cohort')
        return
    if receipt['bundle_version'] != BUNDLE_VERSION or plan.get('bundle_version', BUNDLE_VERSION) != BUNDLE_VERSION:
        raise ValueError('unsupported quarantine bundle version')
    recorded = receipt.get('members') or []
    observed = current.get('members') or []
    if ([m['kind'] for m in recorded] != kinds or [m['kind'] for m in observed] != kinds
            or current['companion_cohort'] != kinds[1:]):
        raise ValueError('quarantined companion cohort changed')
    for old, proof, now in zip(expected, recorded, observed):
        if (proof['src'], proof['dst']) != (old['src'], old['dst']):
            raise ValueError('quarantine bundle receipt identity mismatch')
        for actual in (proof['precondition'], now['precondition']):
            if actual['size_bytes'] != old['precondition']['size_bytes'] or _signature_ticks(actual) != _signature_ticks(old['precondition']):
                raise ValueError('quarantined companion signature changed')
