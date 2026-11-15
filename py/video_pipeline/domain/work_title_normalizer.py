"""Shared curated-alias and work-prefix precedence for ingest and extraction.

Storage folders are not identities. Only the beginning of the recording title,
never a programme mentioned inside its description, may identify a work.
"""
import re
import sys
import unicodedata
from broadcast_frame_titles import canonical_from_known_program_prefix, strip_broadcast_frame_prefix


def alias_key(value):
    text = unicodedata.normalize('NFKC', str(value or '')).casefold()
    return re.sub(r'[\s_<>:"/\\|?*]+', '', text)


def work_title_header(base):
    """Cut explicit episode/subtitle markers, not distinct work-name words."""
    text = re.sub(r'^【[^】]*無料の日[^】]*】\s*', '', base)
    text = strip_broadcast_frame_prefix(text)
    marker = re.search(r'(?:第\s*\d+\s*話|\s+\d+\s*話|[#＃]\d+(?=[\s_「『-]|$)|[【「『▽▼◇◆])', text)
    return text[:marker.start()].strip(' _') if marker and marker.start() > 0 else text.strip(' _')


def resolve_work_title(base, aliases=None, regex_rules=(), *, parsed_title=None, dictionary_match=None):
    """Curated rules/aliases > known frame/work prefix > DB prefix lookup."""
    for pattern, canonical, field in regex_rules:
        target = parsed_title if field == 'program_title' and parsed_title is not None else base
        match = pattern.search(target)
        if match:
            try:
                return match.expand(canonical)
            except re.error:
                return canonical
    normalized = {}
    for raw, canonical in (aliases or {}).items():
        key = alias_key(raw)
        if key in normalized and normalized[key] != canonical:
            raise ValueError('conflicting canonical aliases after normalization')
        normalized[key] = canonical
    content_base = re.sub(r'^【[^】]*無料の日[^】]*】\s*', '', base)
    prefix = canonical_from_known_program_prefix(content_base)
    stripped = strip_broadcast_frame_prefix(content_base)
    headers = [alias_key(work_title_header(content_base))]
    if prefix:
        headers.append(alias_key(prefix))
    if parsed_title is not None and alias_key(parsed_title) in headers:
        headers.insert(0, alias_key(parsed_title))
    for header in headers:
        if header in normalized:
            return normalized[header]
    candidates = [alias_key(base), alias_key(stripped)]
    if prefix:
        candidates.append(alias_key(prefix))
    for key in sorted(normalized, key=len, reverse=True):
        if key and any(candidate.startswith(key) and candidate[len(key):len(key) + 1] in ('!', '?', '【', '「', '『', '▽', '▼', '◇', '◆', '#')
                       for candidate in candidates):
            return normalized[key]
    if prefix:
        return prefix
    return dictionary_match(stripped) if dictionary_match else None


def regex_rules_from_data(data):
    """The same explicit regex resource format is accepted by both readers."""
    result = []
    rules = data.get('rules') if isinstance(data, dict) else None
    if not isinstance(rules, list):
        return result
    for rule in rules:
        if not isinstance(rule, dict) or not rule.get('enabled', True):
            continue
        match, values = rule.get('match'), rule.get('set')
        if not isinstance(match, dict) or not isinstance(values, dict):
            continue
        pattern, title = match.get('regex'), values.get('program_title')
        if not isinstance(pattern, str) or not isinstance(title, str) or not pattern or not title:
            continue
        field = match.get('field', 'base')
        if field not in ('base', 'program_title'):
            field = 'base'
        try:
            result.append((re.compile(pattern), title.strip(), field))
        except re.error as error:
            print(f'W invalid regex in rules: pattern={pattern!r} error={error}', file=sys.stderr)
    return result
