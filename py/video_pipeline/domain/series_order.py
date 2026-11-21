"""Owner-assigned series order prefixes such as ``ゴジラ#03 [映]…``.

Inside a curated movie series folder the owner numbers films by hand with a
leading ``<abbreviation>#<number>`` (half- or full-width ``#``). The prefix is
an ordering label, not the program title, so title checks must read the name
after it. Outside series folders ``#03`` usually means an episode number and is
left alone; callers decide the scope (see ``MovieGrouping.series_containing``).
The file name itself is never rewritten.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

# NFKC maps ＃ to # and full-width digits to ASCII before matching.
_SERIES_ORDER_RE = re.compile(
    r"^(?P<abbr>[^\s#\[\]【】「」『』()<>]{1,16}?)#(?P<num>\d{1,4})(?![\d.,])[\s_]*"
)


@dataclass(frozen=True)
class SeriesOrder:
    abbr: str
    number: int
    rest: str

    def metadata_fields(self) -> dict[str, object]:
        return {"series_abbr": self.abbr, "series_order": self.number}


def parse_series_order(name: str) -> SeriesOrder | None:
    """Return the leading owner series order of a recording name, if any."""
    normalized = unicodedata.normalize("NFKC", str(name or "")).lstrip()
    match = _SERIES_ORDER_RE.match(normalized)
    if not match:
        return None
    return SeriesOrder(match.group("abbr"), int(match.group("num")), normalized[match.end():])
