"""Explicit movie storage groups, independent of canonical work identity."""
import re
import unicodedata
from typing import NamedTuple

WORK_LAYOUTS = ("flat", "by_work_year_month")


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


class MoviePlacement(NamedTuple):
    """Where a grouped movie belongs.

    ``work_folder`` is set only when the series opts into a per-work layout.
    """
    root: str
    folder: str
    work_folder: str | None = None
    layout: str = "flat"
    frame_fallback: bool = False


def _work_entries(work):
    """Return (declared title, aliases, explicit folder) of one works entry."""
    if isinstance(work, str):
        return work, [work], None
    title = work["title"]
    folder = work.get("folder")
    return title, [title, *work.get("aliases", [])], folder


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
            layout = group.get("work_layout", "flat")
            if layout not in WORK_LAYOUTS:
                raise ValueError(f"movie series work_layout must be one of {WORK_LAYOUTS}: {layout}")
            for work in group["works"]:
                if isinstance(work, str):
                    if not work.strip():
                        raise ValueError("movie series work titles must be nonempty")
                    continue
                if not isinstance(work, dict) or not str(work.get("title") or "").strip():
                    raise ValueError("movie series work entries require a title")
                folder = work.get("folder")
                if folder is not None and (not isinstance(folder, str) or not folder.strip()
                                           or re.search(r'[<>:"/\\|?*]', folder) or folder.strip() in (".", "..")):
                    raise ValueError("movie series work folder must be one safe path component: " + str(folder))

    def work_layout_series_containing(self, src_path_win):
        """Series folder (as a path) holding ``src`` when that series uses a per-work layout."""
        from pathlib import PureWindowsPath
        src = PureWindowsPath(str(src_path_win))
        for group in self.series:
            if group.get("work_layout", "flat") == "flat":
                continue
            from .path_placement_rules import safe_dir_name
            series_dir = PureWindowsPath(str(group["dest_root"]).rstrip("\\") + "\\") / safe_dir_name(group["folder"], maxlen=80)
            if src.parent == series_dir or series_dir in src.parents:
                return series_dir
        return None

    def destinations(self):
        return [root for root in [self.default_dest, *(g['dest_root'] for g in self.series)] if root]

    def resolve(self, title, filename, movie_evidence=False, *, allow_frame_fallback=True):
        """Return (root, folder) of the storage group, or None."""
        placement = self.resolve_placement(title, filename, movie_evidence, allow_frame_fallback=allow_frame_fallback)
        return (placement.root, placement.folder) if placement else None

    def resolve_placement(self, title, filename, movie_evidence=False, *, allow_frame_fallback=True):
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
        infer_from_name = allow_frame_fallback and (title_key in ('', 'unknown') or any(title_key.startswith(compact(f)) for f in self.frames))

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
                declared, aliases, folder = _work_entries(work)
                for alias in aliases:
                    key = compact(alias)
                    if matches_work(alias):
                        matches.append((len(key), group, declared, folder))
        if matches:
            longest = max(m[0] for m in matches)
            best = [m for m in matches if m[0] == longest]
            groups = {m[1]["folder"]: m[1] for m in best}
            if len(groups) != 1:
                raise ValueError("ambiguous movie series membership")
            group = next(iter(groups.values()))
            works = {(m[2], m[3]) for m in best}
            if len(works) != 1:
                raise ValueError("ambiguous movie work membership")
            declared, folder = next(iter(works))
            layout = str(group.get("work_layout", "flat"))
            work_folder = None
            if layout == "by_work_year_month":
                from .path_placement_rules import legacy_safe_dir_name
                # Same escaping as the folders already curated by hand (／ → ＿).
                work_folder = folder.strip() if folder else legacy_safe_dir_name(declared, maxlen=80)
            return MoviePlacement(str(group["dest_root"]).rstrip("\\"), str(group["folder"]), work_folder, layout)
        if allow_frame_fallback and frame and self.default_dest:
            return MoviePlacement(self.default_dest, frame, frame_fallback=True)
        return None
