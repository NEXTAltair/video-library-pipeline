"""Explicit, evidence-backed duration exceptions; never scheduled heuristics.

This accepts a concrete maintenance review, not sparse fingerprint similarity as
automatic equivalence. All references become exact-plan checksum preconditions.
"""
import hashlib
import json
import math
from pathlib import Path


def load_coverage_reviews(evidence, checksums):
    evidence = Path(evidence).resolve()
    name = 'content-coverage-reviews.json'
    path = evidence / name
    if not path.exists():
        return []
    body = json.loads(path.read_text())
    if body.get('schema_version') != 1 or not isinstance(body.get('reviews'), list):
        raise ValueError('invalid content coverage review schema')
    checksums[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    drops = set()
    for review in body['reviews']:
        if (review.get('review_status') != 'verified_program_coverage'
                or review.get('full_source_audio_compared') is not True
                or review.get('source_program_content_covered') is not True
                or review.get('keeper_program_complete') is not True
                or not review.get('review_notes')
                or not review.get('reviewed_by')
                or not review.get('evidence_files')):
            raise ValueError('incomplete content coverage review')
        source, keeper = review['source'], review['keeper']
        if source['path'] == keeper['path'] or source['path'].casefold() in drops:
            raise ValueError('overlapping content coverage review')
        drops.add(source['path'].casefold())
        for item in (source, keeper):
            if not item.get('signature') or not math.isfinite(item.get('duration_seconds', 0)) or item['duration_seconds'] <= 0:
                raise ValueError('unsigned content coverage review')
        for interval in review.get('unmatched_source_intervals', []):
            start, end = interval['start'], interval['end']
            if (not math.isfinite(start) or not math.isfinite(end)
                    or not 0 <= start < end <= source['duration_seconds']
                    or interval.get('classification') not in {
                        'commercial_or_promotion', 'program_verified_in_keeper',
                        'fingerprint_boundary_same_scene'}
                    or not interval.get('review_notes')):
                raise ValueError('unreviewed source content interval')
        for reference in review['evidence_files']:
            relative = Path(reference['path'])
            resolved = (evidence / relative).resolve()
            if relative.is_absolute() or '..' in relative.parts or not resolved.is_relative_to(evidence) or not resolved.is_file():
                raise ValueError('content coverage evidence escaped directory')
            digest = hashlib.sha256(resolved.read_bytes()).hexdigest()
            if digest != reference['sha256']:
                raise ValueError('content coverage evidence changed')
            checksums[str(relative)] = digest
    return body['reviews']


def match_coverage_review(review, source, keeper, title_key):
    """Preservation, identity, freshness and quality remain independent guards."""
    source_subtitle, keeper_subtitle = (title_key(source['subtitle']),
                                       title_key(keeper['subtitle']))
    if source_subtitle != keeper_subtitle:
        discrepancy = review.get('subtitle_discrepancy_review') or {}
        # Only a missing label can be reconciled by full-content evidence.
        # Two genuinely different named episodes remain an identity conflict.
        if (bool(source_subtitle) == bool(keeper_subtitle)
                or discrepancy.get('missing_label_only') is not True
                or discrepancy.get('same_episode_verified_from_full_content') is not True
                or not discrepancy.get('review_notes')
                or discrepancy.get('source_subtitle') != source['subtitle']
                or discrepancy.get('keeper_subtitle') != keeper['subtitle']):
            raise ValueError('content coverage subtitle preservation identity mismatch')
    if (title_key(review.get('program_title')) != title_key(source['title'])
            or title_key(source['title']) != title_key(keeper['title'])
            or review.get('episodes') != source['episodes']
            or source['episodes'] != keeper['episodes']
            or len(source['episodes']) != 1
            or review.get('edition') != source['edition']
            or source['edition'] != keeper['edition']
            or review.get('broadcast_kind') != 'repeat_by_sourced_date'):
        raise ValueError('content coverage preservation identity mismatch')
    for label, item in (('source', source), ('keeper', keeper)):
        observed = review[label]
        if (observed['path'].casefold() != item['path'].casefold()
                or observed['signature'] != item['signature']
                or abs(observed['duration_seconds'] - item['quality']['duration']) > .001):
            raise ValueError('content coverage recording changed')
    # A review cannot trade away independently observed spatial quality.
    if (keeper['quality']['width'] * keeper['quality']['height']
            < source['quality']['width'] * source['quality']['height']):
        raise ValueError('content coverage keeper quality regression')
    return True
