"""Conservative title-text cleanup, independent of filesystem escaping."""

PAIRS = (('(', ')'), ('[', ']'), ('{', '}'), ('<', '>'),
         ('（', '）'), ('［', '］'), ('｛', '｝'), ('＜', '＞'),
         ('「', '」'), ('『', '』'), ('【', '】'), ('〈', '〉'), ('《', '》'),
         ('“', '”'), ('‘', '’'))


def clean_title_edges(value: str) -> str:
    """Remove only unmatched delimiters at either edge; preserve balanced titles.

    Interior punctuation (including the apostrophe in NNNドキュメント'26)
    and balanced quoted names are content, not extraction leftovers.
    """
    text = str(value or '').strip()
    while text:
        previous = text
        for quote in ('"', "'", '＂', '＇'):
            if text.count(quote) % 2:
                if text.startswith(quote):
                    text = text[1:].strip()
                elif text.endswith(quote):
                    text = text[:-1].strip()
        for opening, closing in PAIRS:
            for char, other in ((opening, closing), (closing, opening)):
                if text.count(char) > text.count(other):
                    if text.startswith(char):
                        text = text[1:].strip()
                    elif text.endswith(char):
                        text = text[:-1].strip()
        if text == previous:
            break
    return text
