"""Explicit movie storage groups, independent of canonical work identity."""
import re
import unicodedata


def compact(value):
    return re.sub(r"[\s_]+", "", unicodedata.normalize("NFKC", str(value or ""))).casefold()


def has_movie_evidence(metadata):
    genre = str(metadata.get('genre') or '').casefold()
    if genre:
        return genre in ('movie', '映画')
    epg = metadata.get('epg_genres') or []
    if isinstance(epg, list):
        for entry in epg:
            category = str(entry.get('category') or '') if isinstance(entry, dict) else str(entry)
            if category == '映画' or category.startswith('映画 - '):
                return True
    return str(metadata.get('epg_genre') or '').startswith('映画 - ')


class MovieGrouping:
    def __init__(self, data):
        self.default_dest = str(data.get("default_dest_root") or "").rstrip("\\")
        self.frames = list(data.get("frames") or [])
        self.series = list(data.get("series") or [])
        if not isinstance(data.get("frames", []), list) or any(not isinstance(f, str) or not f.strip() for f in self.frames):
            raise ValueError("movie frames must be nonempty names in a list")
        for group in self.series:
            if not group.get("folder") or not group.get("dest_root") or not group.get("works"):
                raise ValueError("movie series requires folder, dest_root and explicit works")

    def destinations(self):
        return [root for root in [self.default_dest, *(g['dest_root'] for g in self.series)] if root]

    def resolve(self, title, filename, movie_evidence=False):
        raw = unicodedata.normalize("NFKC", str(filename))
        raw = re.sub(r"^(?:\[[^\]]+\])+", "", raw).strip()
        raw = re.sub(r"^CM-アリ\s*", "", raw)
        frame = next((f for f in self.frames if re.match(re.escape(f) + r"(?:[「『\s_]|$)", raw) or re.search(r"【" + re.escape(f) + r"(?:[・\s]|】)", raw)), None)
        if not frame and not movie_evidence:
            return None
        work_raw = raw[len(frame):].strip() if frame and raw.startswith(frame) else raw
        work_raw = re.sub(r"^映画\s*", "", work_raw)
        quoted = re.match(r'^[「『](.+?)[」』]', work_raw)
        quoted_work = compact(quoted.group(1).lstrip('「『')) if quoted else None
        title_key = compact(title)
        infer_from_name = title_key in ('', 'unknown') or any(title_key.startswith(compact(f)) for f in self.frames)

        def matches_work(alias):
            key = compact(alias)
            if not key:
                return False
            if title_key == key:
                return True
            if not infer_from_name:
                return False
            if quoted_work is not None:
                return quoted_work == key
            pattern = r'[\s_]*'.join(re.escape(c) for c in key)
            match = re.match(pattern + r'(?=\s|_|[「『【\[<(]|$)', work_raw, re.I)
            if not match:
                return False
            # An English next word can be part of a different movie title.
            rest = work_raw[match.end():]
            return not (re.search(r'[a-z]', key) and re.match(r'\s+[A-Za-z]', rest))
        matches = []
        for group in self.series:
            for work in group["works"]:
                aliases = [work] if isinstance(work, str) else [work["title"], *work.get("aliases", [])]
                for alias in aliases:
                    key = compact(alias)
                    if matches_work(alias):
                        matches.append((len(key), group))
        if matches:
            longest = max(length for length, _ in matches)
            groups = {g["folder"]: g for length, g in matches if length == longest}
            if len(groups) != 1:
                raise ValueError("ambiguous movie series membership")
            group = next(iter(groups.values()))
            return str(group["dest_root"]).rstrip("\\"), str(group["folder"])
        if frame and self.default_dest:
            return self.default_dest, frame
        return None
