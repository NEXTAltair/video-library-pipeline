"""Auditable program-level genre decisions, never episode identity or routing edits.

A three-fifths supermajority with a two-vote lead avoids 1:0/2:1 sample accidents
and holds 4:3 mixed-program cohorts. This is a conservative decision rule, not
an inferred probability. Explicit rules win the target; already-classified
sources still need a matching majority and checked broadcaster evidence.
"""
from collections import Counter, defaultdict
from copy import deepcopy

from .work_title_normalizer import alias_key
from .genre_resolver import resolve_genre

GENRES = {
    'アニメ': 'anime', '特撮': 'tokusatsu', '映画': 'movie', 'ドラマ': 'drama',
    'ドキュメンタリー・情報': 'documentary', 'ドキュメンタリー･情報': 'documentary',
    'ドキュメンタリー': 'documentary', '情報': 'documentary', '情報/ワイドショー': 'documentary',
    'バラエティ': 'variety', 'ニュース・報道': 'news', 'ニュース': 'news',
}
LABELS = {'anime': 'アニメ', 'tokusatsu': '特撮', 'movie': '映画', 'drama': 'ドラマ',
          'documentary': 'ドキュメンタリー・情報', 'variety': 'バラエティ', 'news': 'ニュース・報道'}


def genre_key(value):
    value = str(value or '').strip()
    return GENRES.get(value, value if value in LABELS else None)


def _evidence(entry):
    return (isinstance(entry, dict) and bool(str(entry.get('reference') or '').strip())
            and bool(str(entry.get('note') or '').strip()))


def _maps(policy):
    aliases, canonical_names = {}, {}
    for entry in policy.get('aliases', []):
        if not _evidence(entry) or not str(entry.get('canonical_title') or '').strip():
            raise ValueError('alias requires canonical title, reference and note')
        canonical = entry['canonical_title'].strip()
        for name in [canonical, *entry.get('aliases', [])]:
            key = alias_key(name)
            if not key or (key in aliases and aliases[key] != canonical):
                raise ValueError('empty or conflicting alias')
            aliases[key] = canonical
    def keyed_entries(name):
        result = {}
        for title, value in policy.get(name, {}).items():
            if not _evidence(value) or not genre_key(value.get('genre')):
                raise ValueError(f'{name} requires known genre, reference and note')
            key = alias_key(aliases.get(alias_key(title), title))
            if key in result and result[key] != value:
                raise ValueError(f'conflicting {name}')
            result[key] = value
        return result
    return aliases, canonical_names, keyed_entries('explicit_rules'), keyed_entries('official_classifications')


def prepare_program_genre_review(rows, policy=None):
    """Return review-only overrides, decisions and blocked row IDs.

    Input is the complete metadata cohort (not only minority rows). A row may
    carry source_genre/default and classification flags from the relocation
    audit. Missing source placement blocks changes, never assumes B/default.
    A report is not approval: normal run-bound review/apply must still follow.
    """
    policy = policy or {}
    aliases, names, explicit, official = _maps(policy)
    grouped = defaultdict(list)
    seen = set()
    for raw in rows:
        row = deepcopy(raw)
        path_id = str(row.get('path_id') or '')
        title = str(row.get('program_title') or '').strip()
        if not path_id or path_id in seen or not title:
            raise ValueError('cohort requires unique path_id and nonempty program_title')
        seen.add(path_id)
        canonical = aliases.get(alias_key(title), title)
        key = alias_key(canonical)
        names.setdefault(key, canonical)
        grouped[key].append(row)
    decisions, overrides, held = [], {}, []
    for key, cohort in sorted(grouped.items()):
        counts = Counter(genre_key(r.get('genre', r.get('metadata_genre'))) for r in cohort)
        unknown = counts.pop(None, 0)
        ranking = counts.most_common()
        winner, votes = ranking[0] if ranking else (None, 0)
        runner = ranking[1][1] if len(ranking) > 1 else 0
        total = len(cohort)
        # Unknown episodes count against the threshold, rather than disappear.
        consensus = bool(winner and votes * 5 >= total * 3 and votes - runner >= 2)
        rule = explicit.get(key)
        target = genre_key(rule['genre']) if rule else (winner if consensus else None)
        source = 'explicit_rule' if rule else ('supermajority' if consensus else 'hold_close_or_insufficient')
        off = official.get(key)
        # checked=true certifies an evidence review; a URL alone is not checking.
        official_ok = bool(off and off.get('checked') is True and genre_key(off['genre']) == target)
        row_results = []
        for row in cohort:
            pid = str(row['path_id'])
            old = genre_key(row.get('genre', row.get('metadata_genre')))
            placement = row.get('source_genre')
            reasons = []
            if target is None:
                reasons.append('close_or_insufficient_votes')
            flags = row.get('flags') or {}
            movement_holds = []
            if flags.get('zero_corrupt') or row.get('size_bytes') == 0:
                movement_holds.append('zero_corrupt')
                reasons.append('zero_corrupt')
            if flags.get('godzilla_title') or flags.get('bond007_title'):
                reasons.append('protected_work_policy')
            if target == 'movie' or old == 'movie' or flags.get('movie_route_or_standalone') or row.get('program_title') == '東京ゴッドファーザーズ':
                movement_holds.append('movie_policy_pending')
            if row.get('program_title') == '東京ゴッドファーザーズ':
                reasons.append('movie_genre_preserved')
            if flags.get('L_syllabary'):
                movement_holds.append('syllabary_policy_pending')
            if flags.get('existing_series_outward'):
                movement_holds.append('existing_series_outward')
            if placement is None:
                reasons.append('source_placement_unknown')
            elif target and placement not in ('default', target):
                approved = policy.get('approved_source_changes', {}).get(pid)
                if not _evidence(approved):
                    if not consensus or winner != target:
                        reasons.append('source_change_without_matching_majority')
                    if not official_ok:
                        reasons.append('official_classification_unverified_or_conflicting')
            changed = target is not None and old != target
            title_changed = row['program_title'] != names[key]
            if not reasons and (changed or title_changed):
                note = f'program genre policy: {source}; votes={dict(counts)}; total={total}'
                edit = {'review_note': note}
                if changed:
                    edit['genre'] = LABELS[target]
                if title_changed:
                    edit['program_title'] = names[key]
                overrides[pid] = edit
            if reasons:
                held.append({'path_id': pid, 'program_title': row['program_title'], 'reasons': reasons})
            row_results.append({'path_id': pid, 'metadata_change': changed or title_changed,
                                'eligible_for_review': not reasons, 'reasons': reasons, 'movement_holds': movement_holds,
                                'series_layout_preserved': True})
        decisions.append({'program_title': names[key], 'rows': total, 'votes': dict(counts),
                          'unknown_votes': unknown, 'target_genre': target, 'decision': source,
                          'consensus': consensus, 'official_checked_consistent': official_ok,
                          'rule_evidence': rule, 'official_evidence': off, 'row_decisions': row_results})
    return {'schema': 'program_genre_review/v1', 'threshold': {'share': '3/5', 'minimum_lead': 2,
            'rationale': 'hold small/close samples; unknown votes count in denominator'},
            'rows': len(seen), 'programs': decisions, 'row_overrides': overrides, 'held_rows': held,
            'normalization_candidates': [{'path_id': str(r['path_id']), 'genre': LABELS[genre_key(r.get('genre'))]} for cohort in grouped.values() for r in cohort if genre_key(r.get('genre')) and r.get('genre') != LABELS[genre_key(r.get('genre'))]],
            'routing_changes': 0, 'physical_moves': 0, 'db_writes': 0}


def validate_program_genre_consistency(rows):
    """Reject newly accepted split genres across the entire multi-batch cohort.

    Historical/replayed commits are checked by their original receipt instead;
    this guard is for new acceptance only, after explicit aliases/overrides.
    """
    groups = defaultdict(set)
    names = {}
    for row in rows:
        title = str(row.get('program_title') or '').strip()
        key = alias_key(title)
        names.setdefault(key, title)
        genre = genre_key(resolve_genre(row))
        if key and genre:
            groups[key].add(genre)
    splits = {names[key]: sorted(genres) for key, genres in groups.items() if len(genres) > 1}
    if splits:
        raise ValueError(f'program-level genre split remains; prepare full-cohort review or hold programs: {splits}')
