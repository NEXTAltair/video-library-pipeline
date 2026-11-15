"""Explicit movie storage groups, independent of canonical work identity."""
import re
import unicodedata


def compact(value):
    return re.sub(r"[\s_]+", "", unicodedata.normalize("NFKC", str(value or ""))).casefold()


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

    def resolve(self, title, filename, movie_evidence=False):
        raw = unicodedata.normalize("NFKC", str(filename))
        raw = re.sub(r"^(?:\[[^\]]+\])+", "", raw).strip()
        raw = re.sub(r"^CM-アリ\s*", "", raw)
        frame = next((f for f in self.frames if re.match(re.escape(f) + r"(?:[「『\s_]|$)", raw) or re.search(r"【" + re.escape(f) + r"(?:[・\s]|】)", raw)), None)
        if not frame and not movie_evidence:
            return None
        work_raw = raw[len(frame):].strip() if frame and raw.startswith(frame) else raw
        work_raw = re.sub(r"^映画\s*", "", work_raw).lstrip('「『')
        candidates = [compact(title), compact(work_raw)]
        matches = []
        for group in self.series:
            for work in group["works"]:
                aliases = [work] if isinstance(work, str) else [work["title"], *work.get("aliases", [])]
                for alias in aliases:
                    key = compact(alias)
                    if key and any(candidate.startswith(key) for candidate in candidates):
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
